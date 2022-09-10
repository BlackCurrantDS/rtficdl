##RTF data lake updates helper

import pandas as pd
import numpy as np
from smart_open import open
import ast
import datetime as datetime
import time

import json
from pandas import Timestamp
import html5lib

import requests
from bs4 import BeautifulSoup

import hashlib

import pyspark
from pyspark.sql import SparkSession

# for glue object
import boto3

import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
# import awswrangler as wr
from pyspark.sql import SparkSession

from rtf_job_helpers import common_functions

import itertools


def get_s3_file_list(bucket, prefix):
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket)
    return my_bucket.objects.filter(Prefix=prefix)


# fucntion to convet each dataframe to update statments

# hash function
def hash_value(x):
    return hashlib.sha256(x.encode()).hexdigest()


# read all dataframes from html file
def get_dataframes_from_html(input_html_file):
    with open(input_html_file) as f:
        df_list = pd.read_html(f, header=[0], flavor='bs4')

        print("Table found are", len(df_list))

    return df_list


def get_dataframe_with_updated_flag(df, update_flag, flag_value, data_source_name):
    new_df = df[(df.update_flag == flag_value) & (df.Source == data_source_name)]

    return new_df


def get_df_column_name_list(df):
    column_list = list(df.columns.values)

    column_list_with_qoutes = []

    for x in column_list:
        column_list_with_qoutes.append("'" + x + "'")

    return column_list_with_qoutes


def dict_to_pyspark_df(in_dict):
    spark = SparkSession.builder.appName('sparkdf').getOrCreate()
    # creating a dataframe
    dataframe = spark.createDataFrame(in_dict)

    return dataframe


def get_gluetable_file_location(databasename, tablename):
    glue = boto3.client('glue')

    tbl_data = glue.get_table(DatabaseName=databasename, Name=tablename)

    s3_location = tbl_data['Table']['StorageDescriptor']['Location']

    return s3_location


def get_hashed_dataframe(df, columns_to_update):
    # convert dataframe to dictionary
    df_to_dict = df.to_dict(orient='records')
    column_list_with_qoutes = get_df_column_name_list(df)
    # print("column_list_with_qoutes",column_list_with_qoutes)

    for i in df_to_dict:
        # print(i)
        i.pop('update_flag', None)
        i.pop('Unnamed: 0', None)
        table_name = i['Table Name']
        s3_file_name = i['file_name']
        i.pop('Table Name', None)
        i.pop('Source', None)
        # print("after poppping",i)
        # get value of the columns names to be masked in another dictionary

        to_masked = dict((k, i[k]) for k in columns_to_update if k in i)
        # print("to_masked", to_masked)

        # dictionary after masking
        after_masked = {x: hash_value(x) for x in to_masked}
        # print("after_masked", after_masked)

        # update old
        i.update((k, v) for k, v in after_masked.items())
        # print("after updating", i)

    hashed_df_pandas = pd.DataFrame.from_dict(df_to_dict)
    # print("hashed_df_pandas",hashed_df_pandas)

    return hashed_df_pandas, table_name, s3_file_name


def get_dataframe_to_filter(df):
    # convert dataframe to dictionary
    df_to_dict = df.to_dict(orient='records')
    column_list_with_qoutes = get_df_column_name_list(df)
    # print("column_list_with_qoutes",column_list_with_qoutes)

    for i in df_to_dict:
        # print(i)
        i.pop('update_flag', None)
        i.pop('Unnamed: 0', None)
        table_name = i['Table Name']
        s3_file_name = i['file_name']
        i.pop('Table Name', None)
        i.pop('Source', None)
        # print("after poppping",i)
        # get value of the columns names to be masked in another dictionary
    hashed_df_pandas = pd.DataFrame.from_dict(df_to_dict)
    # print("hashed_df_pandas",hashed_df_pandas)

    return hashed_df_pandas


# function to get glue table data
def get_table_data(database_name, table_name):
    glue = boto3.client('glue')

    glueContext = GlueContext(SparkContext.getOrCreate())
    memberships = glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name=table_name)

    # df = memberships.toDF()

    # table_columns = [x[0] for x in df.dtypes]

    return memberships


def get_s3_file_list(bucket, prefix):
    s3 = boto3.resource('s3')

    print("bucket is,", bucket)
    print("prefix is,", prefix)
    my_bucket = s3.Bucket(bucket)

    # print(file.key)
    return (my_bucket.objects.filter(Prefix=prefix))


def copy_objects(source_bucket, target_bucket, source_file_name, target_file_name):
    s3 = boto3.resource('s3')
    try:
        copy_source = {
            'Bucket': source_bucket,
            'Key': source_file_name
        }

        s3.meta.client.copy(copy_source, target_bucket, target_file_name)

    except Exception as e:
        print("exception while copying is", e)
        raise e


def rename_s3_file(bucket_name, old_file_key, new_file_key):
    s3 = boto3.resource('s3')
    s3.Object(bucket_name, new_file_key).copy_from(CopySource=bucket_name + '/' + old_file_key)
    s3.Object(bucket_name, old_file_key).delete()


def delete_s3_file(bucket_name, file_key):
    s3 = boto3.resource('s3')
    s3.Object(bucket_name, file_key).delete()
