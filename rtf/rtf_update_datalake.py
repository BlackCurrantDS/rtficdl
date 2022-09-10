import logging


import boto3
import pandas as pd
import numpy as np
import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
# import awswrangler as wr
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, when

from smart_open import open

from rtf_job_helpers import common_functions

import itertools
from datetime import datetime
from awsglue.utils import getResolvedOptions

import rtf_datalake_update_helpers as datalake_helpers

environment_args = getResolvedOptions(sys.argv, ['s3_rtf'])

bucket_folder = environment_args['s3_rtf']
my_s3_path = "s3://" + bucket_folder + "/Customer_files_to_update/"
logging.info(f"The bucket name is {bucket_folder}")

lake_metadata_file = 'rtf_lake_mapping_config.txt'

# Which email to mask/delete
given_email_file = "s3://" + bucket_folder+"/config_files/" + "rtf_given_email.txt"
columns_to_update_file = "s3://" + bucket_folder+ "/Customer_files_to_update/" + "rtf_datalake_columns_to_update.txt"
# read the emails
myObject = {}
with open(given_email_file) as f:
    for line in f.readlines():
        # print(line)
        key, value = line.rstrip("\n").split("=")
        myObject[key] = value
f.close()

given_email = common_functions.eval_dtype(myObject['given_email'])

logging.info(f"Emails to delete are: {given_email}")

#get the columns to update
myObject = {}
with open(columns_to_update_file) as f:
    for line in f.readlines():
        # print(line)
        key, value = line.rstrip("\n").split("=")
        myObject[key] = value
f.close()

columns_to_update = common_functions.eval_dtype(myObject['rest_all_pii_columns'])


def get_s3_locations_of_glue_updates(html_file_path,each_email):
    df_list = datalake_helpers.get_dataframes_from_html(html_file_path)

    s3_paths = []

    with open(my_s3_path + each_email+"_rtf_glue_update_logs.txt", 'w') as f, open(my_s3_path + each_email+"_rtf_backupfile_logs.txt", 'w') as bf:

        for n, my_df in enumerate(df_list):

            print("processing for dataframe..", n, file=f)

            df = datalake_helpers.get_dataframe_with_updated_flag(my_df, 'update_flag', 'Y', 'DataLake')

            if len(df) == 0:
                print("There are no updates for the table {}".format(n), file=f)
            else:

                # get schema and table name
                full_table_name = df['Table Name'].unique()
                if full_table_name != ['dp_campaign_pii_stdzd.cheetah_stdzd_pii_contact_history']:
                    print("full_table_name", full_table_name, file=f)
                    schema_name, table_name = full_table_name[0].split(".")[0], full_table_name[0].split(".")[1]
                    s3_partition_unique = df['partition_0'].astype(str) + '/' + '0' + df['partition_1'].astype(
                        str) + '/' + df['partition_2'].astype(str)  # 2021/08/26
                    s3_partition_unique = s3_partition_unique.unique()
                    print("s3_partition_unique", s3_partition_unique, file=f)
                    s3_main_location = datalake_helpers.get_gluetable_file_location(schema_name, table_name)

                    files_to_be_replaced = [s3_main_location + i for i in s3_partition_unique]
                    print("files ot be replaced in s3 for dataframe{} is{}".format(n, files_to_be_replaced), file=f)

                    # check the number of rows to be replaced
                    # 1. get the main dataframe
                    df_glue_main = datalake_helpers.get_table_data(schema_name, table_name)
                    df_glue_main_spark = df_glue_main.toDF()  # to sparkdataframe from gluedynamic

                    # count value for each partition type
                    for partition in s3_partition_unique:

                        splited = partition.split("/")
                        p0, p1, p2 = splited[0], splited[1], splited[2]
                        p1 = p1 if len(str(p1)) == 2 else str(p1).rjust(2, '0')  # only for month
                        p2 = p2 if len(str(p2)) == 2 else str(p2).rjust(2, '0')
                        print("partiitons are", (p0, p1, p2))

                        print("splitting the mail df per partition")
                        # get the first partition from main
                        main_df_to_partition = df_glue_main_spark.filter(
                            (df_glue_main_spark.partition_0 == p0) & (
                                    df_glue_main_spark.partition_1 == p1) & (
                                    df_glue_main_spark.partition_2 == p2))

                        # print("main df for the partition{}total count {}".format(splited,main_df_to_partition.count()), file=f)
                        print("main df is filtered...")

                        print("main df is splitted for partition...")

                        print("getting df to subrtact from main df")
                        # get main dataframe wihtout the needs to update records
                        df_to_filter_from_main = datalake_helpers.get_dataframe_to_filter(df)

                        df_to_filter_from_main_spark = datalake_helpers.dict_to_pyspark_df(df_to_filter_from_main)
                        df_to_filter_from_main_spark = df_to_filter_from_main_spark.filter(
                            (df_to_filter_from_main_spark.partition_0 == p0) & (
                                    df_to_filter_from_main_spark.partition_1 == p1) & (
                                    df_to_filter_from_main_spark.partition_2 == p2))
                        print("df_to_filter for partition{} count is {}".format(splited,
                                                                                df_to_filter_from_main_spark.count()),
                              file=f)
                        #makign sure the partitions are same
                        df_to_filter_from_main_spark_1 = df_to_filter_from_main_spark.withColumn("partition_1", when(F.length(df_to_filter_from_main_spark.partition_1) == 1,F.concat(F.lit("0"), col("partition_1"))).otherwise(df_to_filter_from_main_spark.partition_1))
                        df_to_filter_from_main_spark_2 = df_to_filter_from_main_spark_1.withColumn("partition_2", when(F.length(df_to_filter_from_main_spark_1.partition_2) == 1,F.concat(F.lit("0"), col("partition_2"))).otherwise(df_to_filter_from_main_spark_1.partition_2))
                        # print("df_to_filter_from_main_spark dtypes", df_to_filter_from_main_spark.dtypes,file=f)
                        print("filtering is done for updated df..")
                        print("now substraction it from main df..")
                        # main new df
                        main_filtered_df = main_df_to_partition.exceptAll(df_to_filter_from_main_spark_2)
                        print("main_filtered_df for partition {} is count {}".format(splited,
                                                                                     main_filtered_df.count()),
                              file=f)
                        print("substraction is done..")
                        # get hashed df to be replaced
                        # for hash values of columns
                        print("getting hashed df...")
                        hashed_df_pandas, table_name, s3_file_name = datalake_helpers.get_hashed_dataframe(df,
                                                                                                           list(set(main_filtered_df.columns).intersection(columns_to_update)))
                        hashed_df_spark = datalake_helpers.dict_to_pyspark_df(hashed_df_pandas)
                        print("hashed_df_count for partition {} is count {}".format(splited, hashed_df_spark.count()),
                              file=f)
                        print("now concating main df to hashed df...")
                        # concat main df filters and hash_df
                        df_concat=main_filtered_df
                        #df_concat = main_filtered_df.union(hashed_df_spark.select(main_filtered_df.columns))
                        df_concat = df_concat.repartition(1)  # new df wiht exisiitng values and new hashed values
                        print("concatanation done...")
                        print("count after concat is{}".format(df_concat.count()), file=f)
                        # take backup of the s3 file pointing to this partition

                        source_bucket_path = s3_main_location
                        source_bucket_without_s3 = source_bucket_path.split('s3://')[1]
                        source_bucket = source_bucket_without_s3.split('/')[0]
                        prefix = source_bucket_without_s3.split(source_bucket)[1] + partition + '/'
                        new_prefix = prefix[1:]

                        print("getting s3 files...")
                        print("getting s3 files...", file=f)
                        print("source bucket is {} and prefix is {}".format(source_bucket, new_prefix), file=f)
                        # list all files in source bucket
                        all_files = datalake_helpers.get_s3_file_list(source_bucket, new_prefix)
                        print("all_files", all_files)
                        print("all_files", all_files, file=f)
                        for i in all_files:

                            print("i", i.key, file=f)
                            print("files in bucket {} is {}".format((source_bucket + new_prefix), i.key), file=f)

                            source_file_name = i.key
                            if source_file_name != new_prefix:
                                print("source_file_name", source_file_name, file=f)
                                source_file_name = source_file_name.split(new_prefix)[1]
                                print("soure file name in {} is {}".format((source_bucket + new_prefix),
                                                                           source_file_name),
                                      file=f)
                                file_extension = source_file_name.split('.')[1]
                                target_file_name = new_prefix + 'backup_' + datetime.now().strftime(
                                    "%I%p") + '_' + source_file_name
                                target_bucket = source_bucket

                                s_file_name = new_prefix + source_file_name

                                print("taking the backup of file..")
                                # take backup
                                datalake_helpers.copy_objects(source_bucket, target_bucket, s_file_name,
                                                              target_file_name)  # TODO: write all backup file paths to be deleted later

                                print(f"Backup file locations for email{each_email}:\n",file=bf)
                                print(f"Bucket:{target_bucket}+'File:'+{target_file_name}", file=bf)
                                print(f"backup file location {target_bucket} and file {target_file_name}",file=f)
                                print("going to replace section..")
                                # check if new concat column count is same as earlier
                                print("intersect dataframe", type(
                                    df_concat.exceptAll(main_df_to_partition).intersect(hashed_df_spark).take(1)))
                                #if df_concat.exceptAll(main_df_to_partition).intersect(
                                #        hashed_df_spark).count() == hashed_df_spark.count():  # df_concat.count() == main_df_to_partition.count():

                                print("writing {} to {}".format(df_concat.count(),
                                                                    (source_bucket_path + partition + '/')), file=f)
                                df_concat.write.format(file_extension).mode("append") \
                                        .save(source_bucket_path + partition + '/')
                                # deleteold file and rename new
                                # get the name of new file written
                                print("Finding the new file written to bucket", file=f)
                                for new_file in get_s3_file_list(source_bucket, new_prefix):
                                        if 'part' in new_file.key:
                                            print(
                                                f"new file found is {new_file.key}, renaming it to {s_file_name} in bucket {source_bucket}",
                                                file=f)
                                            datalake_helpers.rename_s3_file(source_bucket, new_file.key, s_file_name)
                                            print(f"Renaming is done.")
                else:
                    print("not this table")
                    continue
                s3_paths.extend(files_to_be_replaced)

    f.close()
    bf.close()
    return s3_paths


def get_s3_file_list(bucket, prefix):
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket)
    return my_bucket.objects.filter(Prefix=prefix)


for each_email in given_email:
    for obj in get_s3_file_list(bucket_folder, 'Customer_files_to_update'):
        print("obj key", obj.key)
        if obj.key.endswith('html') and each_email in obj.key:
            print("Reading customer file", "s3://" + bucket_folder + '/' + obj.key)
            get_s3_locations_of_glue_updates("s3://" + bucket_folder + '/' + obj.key,each_email)