import logging

import pandas as pd
import numpy as np
from smart_open import open
import ast

import json
from pandas import Timestamp
import html5lib

import requests
from bs4 import BeautifulSoup

import rtf_datalake_update_helpers as datalake_helpers
from rtf_job_helpers import common_functions
import hashlib
import sys
from awsglue.utils import getResolvedOptions
import rtf_datalake_update_helpers as datalake_helpers

environment_args = getResolvedOptions(sys.argv, ['s3_rtf'])

bucket_folder = environment_args['s3_rtf']
my_s3_path = "s3://" + bucket_folder + "/Customer_files_to_update/"
logging.info(f"The bucket name is {bucket_folder}")

# Which email to mask/delete
given_email_file = "s3://" + bucket_folder + '/config_files/' + "rtf_given_email.txt"
columns_to_update_file = "s3://" + bucket_folder + '/Customer_files_to_update/' + "rtf_redshift_columns_to_update.txt"
# read the emails
myObject = {}
with open(given_email_file) as f:
    for line in f.readlines():
        # print(line)
        key, value = line.rstrip("\n").split("=")
        myObject[key] = value
f.close()

given_email = common_functions.eval_dtype(myObject['given_email'])

if not given_email:
        raise "no email to update"

#get the columns to update
myObject = {}
with open(columns_to_update_file) as f:
    for line in f.readlines():
        # print(line)
        key, value = line.rstrip("\n").split("=")
        myObject[key] = value
f.close()

columns_to_update = common_functions.eval_dtype(myObject['columns_to_update'])

if not columns_to_update:
        raise "No columns to update"

# fucntion to convet each dataframe to update s


def get_sql_update_statments(df, columns_to_update):
    # convert dataframe to dictionary
    df_to_dict = df.to_dict(orient='records')

    column_list_with_qoutes = datalake_helpers.get_df_column_name_list(df)
    print("column_list_with_qoutes",column_list_with_qoutes)

    columns_to_update = list(set([s.strip("'") for s in column_list_with_qoutes]).intersection(columns_to_update))
    print("columns_to_update",columns_to_update)

    sqls = []
    to_pop = []

    for my_key in df_to_dict[0].keys():
        if 'ts' in my_key:
            print(my_key)
            to_pop.append(my_key)

    # get each row for where clause
    for i in df_to_dict:
        i.pop('update_flag', None)
        i.pop('Source', None)
        i.pop('Unnamed: 0', None)
        table_name = i['Table Name']
        i.pop('Table Name', None)
        for my_key in to_pop:
            i.pop(my_key, None)

        # get value of the columns names to be masked in another dictionary

        to_masked = dict((k, i[k]) for k in columns_to_update if k in i)
        # dictionary after masking

        after_masked = {x: datalake_helpers.hash_value(x) for x in to_masked}

        # create update statment

        # set value statemtn

        res = {key.replace("'", ""): val for key, val in after_masked.items()}

        update_clause = str(res).replace(':', '=')

        update_clause = update_clause.replace('{', "")
        update_clause = update_clause.replace('}', "")

        for item in column_list_with_qoutes:
            if item in update_clause:
                update_clause = update_clause.replace(item, item.strip("'"))

        # where set value

        where_clause = str(i).replace(':', '=')

        where_clause = where_clause.replace('{', "")
        where_clause = where_clause.replace('}', "")

        for item in column_list_with_qoutes:
            if item in where_clause:
                where_clause = where_clause.replace(item, item.strip("'"))
                where_clause = where_clause.replace(',', " and ")

        if len(update_clause) != 0:
            sql_update_st = f"UPDATE TABLE {table_name} SET {update_clause} WHERE {where_clause};"

            sqls.append(sql_update_st)

    return sqls


def get_sql_update_file(html_file_path, each_email):
    df_list = datalake_helpers.get_dataframes_from_html(html_file_path)

    with open(my_s3_path + each_email+"_rtf_redshift_update_logs.txt", 'w') as f:

        for n, my_df in enumerate(df_list):

            print("processing for dataframe..", n, file=f)

            df = datalake_helpers.get_dataframe_with_updated_flag(my_df, 'update_flag', 'Y', 'Redshift')

            if len(df) == 0:
                print("There are no updates for the table {}".format(n), file=f)
            else:

                with open(my_s3_path + each_email + '_rtf_sql_update_file.py', 'w') as ff:

                    sql_update = get_sql_update_statments(df, columns_to_update)
                    print("sql_update = ", file=ff)

                    ff.write(str(sql_update))

                ff.close()


for each_email in given_email:
    for obj in datalake_helpers.get_s3_file_list(bucket_folder, 'Customer_files_to_update'):
        print("obj key", obj.key)
        if obj.key.endswith('html') and each_email in obj.key:
            print("Reading customer file", "s3://" + bucket_folder + '/' + obj.key)
            get_sql_update_file("s3://" + bucket_folder + '/' + obj.key, each_email)