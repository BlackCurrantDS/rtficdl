import pandas as pd
import numpy as np
from smart_open import open
import ast

import json
from pandas import Timestamp
import html5lib

import requests
from bs4 import BeautifulSoup
import sys
from awsglue.utils import getResolvedOptions

import rtf_datalake_update_helpers as datalake_helpers
from rtf_job_helpers import common_functions,redshift_gdpr_config
import hashlib
import rtf_sql_update_file
import logging

environment_args = getResolvedOptions(sys.argv, ['s3_rtf'])

bucket_folder = environment_args['s3_rtf']
my_s3_path = "s3://" + bucket_folder + "/Customer_files_to_update/"
logging.info(f"The bucket name is {bucket_folder}")

# Which email to mask/delete
given_email_file = "s3://" + bucket_folder + '/' + "rtf_given_email.txt"
# read the emails
myObject = {}
with open(given_email_file) as f:
    for line in f.readlines():
        # print(line)
        key, value = line.rstrip("\n").split("=")
        myObject[key] = value
f.close()

given_email = common_functions.eval_dtype(myObject['given_email'])
given_email = ['bhardwaj.priyamvada@gmail.com']

logging.info(f"Emails to delete are: {given_email}")

# fucntion to convet each dataframe to update statments

conn = common_functions.getconnection(redshift_gdpr_config['database'], redshift_gdpr_config['host'], redshift_gdpr_config['port'], redshift_gdpr_config['user'],
                                      redshift_gdpr_config['password'])


def main(update_sql, each_email):
    with open(my_s3_path + each_email+"_rtf_redshift_update_logs.txt", 'w') as f:
        for each_update in update_sql:
            run_response = common_functions.runquery(conn, each_update)
            print("response for query {} is {}".format(each_update, run_response), file=f)


for each_email in given_email:
    for obj in datalake_helpers.get_s3_file_list(bucket_folder, 'Customer_files_to_update'):
        print("obj key", obj.key)
        if obj.key.endswith('py') and each_email in obj.key and 'sql' in obj.key:
            print("Getting customer sql file", "s3://" + bucket_folder + '/' + obj.key)
            rtf_sql_update_file = "s3://" + bucket_folder + '/' + obj.key
            update_sql = rtf_sql_update_file.sql_update
            main(update_sql, each_email)