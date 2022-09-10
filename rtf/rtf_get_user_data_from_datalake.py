import warnings

import boto3
import pandas as pd
import numpy as np
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
import sys

from smart_open import open

import itertools
import logging

warnings.filterwarnings("ignore")

try:
    from rtf_job_helpers import common_functions, redshift_gdpr_config
    from rtf_initial_config_file import candidate_key_columns
    import rtf_get_glue_metadata
    import rtf_redshift_mapping_metadata

except Exception as exception:
    print(exception, False)
    raise

environment_args = getResolvedOptions(sys.argv, ['s3_rtf'])

bucket_folder = environment_args['s3_rtf']
my_s3_path = "s3://" + bucket_folder + "/config_files/"
logging.info(f"The bucket name is {bucket_folder}")

lake_metadata_file = 'rtf_lake_mapping_config.txt'
given_email_file = "rtf_given_email.txt"
# reading email
given_email_param = pd.read_csv(my_s3_path + given_email_file,
                                delimiter='\s*=\s*',
                                header=None,
                                names=['key', 'value'],
                                dtype=dict(key=np.object, value=np.object),
                                index_col=['key']).to_dict()['value']

given_email = common_functions.eval_dtype(given_email_param['given_email'])

# get lake metadata in
lake_metadata_parameters = pd.read_csv(my_s3_path + lake_metadata_file,
                                       delimiter='\s*=\s*',
                                       header=None,
                                       names=['key', 'value'],
                                       dtype=dict(key=np.object, value=np.object),
                                       index_col=['key']).to_dict()['value']

all_email_cols = common_functions.eval_dtype(lake_metadata_parameters['all_email_columns'])

rest_all_pii_columns = common_functions.eval_dtype(lake_metadata_parameters['rest_all_pii_columns'])

all_pii_table_with_email = common_functions.eval_dtype(lake_metadata_parameters['all_tables_with_email_columns'])

non_email_table = common_functions.eval_dtype(lake_metadata_parameters['all_pii_tables_without_email_columns'])

additional_column = common_functions.intersect(rest_all_pii_columns, candidate_key_columns)

glue = boto3.client('glue')

# Create a Glue context
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session


# function to get glue table data
def get_table_data(database_name, table_name):
    # Create a DynamicFrame using the given table and database
    try:
        memberships = glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name=table_name)

        df = memberships.toDF()

        table_columns = [x[0] for x in df.dtypes]
    except Exception as e:
        logging.info(e)
        pass

    return df, table_columns


# function to get mathcing column from

def get_matching_data_for_column(table_df, column_name, column_value):
    logging.info(f"Runnig for table {table_df}")
    try:

        df = table_df[table_df[column_name] == str(column_value)]

    except:

        print("Column not found in table")
        return 42

    else:

        try:

            if df.count() != 0:
                df_pandas = df.toPandas()  # converting to pandas

                return df_pandas
        except Exception as e:
            raise e

        else:

            print("No matching value in {} for {} and value {}".format(table_df, column_name, column_value))  #

            return 41


def get_key_value_pair_of_table(table_name, table_df_pandas, initial_potential_keys):
    table_key_value_pair = []

    potentail_data_dict = {}

    keys_in_table = list(set(initial_potential_keys).intersection(table_df_pandas.columns))

    for i in keys_in_table:
        print("Checking for", i)

        potentail_data_dict['table'] = table_name
        potentail_data_dict['column'] = i
        potentail_data_dict['value'] = table_df_pandas[i].iloc[0]

        print("potentail_data_dict", potentail_data_dict)

        table_key_value_pair.append(potentail_data_dict.copy())

    return table_key_value_pair


def create_zipped_list(in_list):
    # get the list of all potential columns and values from the given list hwere values from found so far..

    all_potential_columns = [d['column'] for d in in_list]
    all_potential_values = [d['value'] for d in in_list]

    zipped_list = list(zip(all_potential_columns, all_potential_values))

    return zipped_list


# start with again


all_tables_to_look_for = all_pii_table_with_email + non_email_table

print("all_tables_to_look_for", all_tables_to_look_for)

zipped_list = [(x, y) for x in all_email_cols for y in given_email]
for each_email in given_email:
    pass_cnt = 0
    with open(my_s3_path + "rtf_datalake_" + each_email + ".csv", 'w') as f:
        while (len(all_tables_to_look_for) > 0 ):  # untill all tables has been looked for

            list_of_tables_with_data_found = []
            list_of_tables_with_data_not_found = []
            list_of_tables_with_data_not_found_for_email = []
            list_of_tables_with_realdata = []
            new_zipped_list = []

            print('initial zipped_list', zipped_list)

            # if pass_cnt == 0:

            for table in all_tables_to_look_for:

                print("calling for table..\n\n", table)
                # get all columns of the table being called to check which column to fire the query on..

                table_name = table.split(".")[1]

                database_name = table.split(".")[0]

                try:
                    table_df, table_column_list = get_table_data(database_name, table_name)

                except:
                    logging.info(f"Exception for table {table_name}")
                    continue
                for col, value in zipped_list:

                    print("Col searching for..", col)

                    print("value searching for..", value)

                    try:
                        logging.info(f"calling for table {table_name}")
                        df_pandas = get_matching_data_for_column(table_df, col, value)
                    except Exception as e:
                        raise e

                    if not isinstance(df_pandas, pd.DataFrame) and df_pandas == 42:

                        list_of_tables_with_data_not_found.append(table)

                        print("list_of_tables_with_data_not_found because of column doesn't exists\n")

                    elif not isinstance(df_pandas, pd.DataFrame) and df_pandas == 41:

                        list_of_tables_with_data_not_found_for_email.append(table)

                        # all_tables_to_look_for.remove(table) #remove all tables which has email but not for given email

                        # print("all_tables_to_look_for", all_tables_to_look_for)

                        print("doesn't exists data for this email")

                    else:

                        print("Got the matching value")

                        my_data_to_dict = df_pandas.to_dict()

                        # write data to file

                        f.write("for table" + str(table) + " data " + str(my_data_to_dict) + '\n')

                        print("Wrote to the file..")

                        # update the zipped list

                        print("updating the zipped list...")

                        potential_key = additional_column.copy()
                        potential_key.extend(all_email_cols)
                        potential_key.remove(col)

                        table_key_value_pair = get_key_value_pair_of_table(table_name, df_pandas, potential_key)

                        print("Got complete key pair for the table", table_key_value_pair)

                        list_of_tables_with_data_found.append(table_key_value_pair)

                        list_of_tables_with_realdata.append(table)

                        print("list_of_tables_with_realdata", list_of_tables_with_realdata)

                        print("list_of_tables_with_data_found", list_of_tables_with_data_found)

                        new_zipped_list.extend(
                            create_zipped_list(list(itertools.chain(*list_of_tables_with_data_found))))

                        print('new zipped_list', list(set(new_zipped_list)))

            # else:
            #    print("other than 1st pass")
            #    break

            pass_cnt = pass_cnt + 1

            list_of_tables_with_data_not_found_for_email = [x for x in list_of_tables_with_data_not_found_for_email if
                                                            x not in list_of_tables_with_realdata]
            print("list_of_tables_with_data_not_found_for_email\n", list_of_tables_with_data_not_found_for_email)

            list_of_tables_with_data_not_found = [x for x in list_of_tables_with_data_not_found if
                                                  x not in list_of_tables_with_realdata]
            print("list_of_tables_with_data_not_found\n", list_of_tables_with_data_not_found)

            l3 = [x for x in all_tables_to_look_for if x not in list_of_tables_with_realdata]

            all_tables_to_look_for = l3

            # zipped_list.extend(list(set(new_zipped_list)))
            # zipped_list = list(set(zipped_list))
            zipped_list = list(set(new_zipped_list))
            print("all_tables_to_look_for after pass\n", all_tables_to_look_for)
            print("zipped_list after the pass\n", zipped_list)
            print("Starting new pass..", '\n\n')
            print('pass_cnt', pass_cnt)

        print("Total passes", pass_cnt)
    f.close()