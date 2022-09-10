import warnings
import pandas as pd
import numpy as np
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
import sys
import boto3

from smart_open import open

import itertools
import logging

warnings.filterwarnings("ignore")

try:
    from rtf_job_helpers import common_functions, redshift_gdpr_config
    from rtf_initial_config_file import candidate_key_columns
    import rtf_get_glue_metadata
    import rtf_redshift_mapping_metadata
    import rtf_datalake_update_helpers

except Exception as exception:
    print(exception, False)
    raise

environment_args = getResolvedOptions(sys.argv, ['s3_rtf'])

bucket_folder = environment_args['s3_rtf']
my_s3_path = "s3://" + bucket_folder + "/Customer_files_to_update/"
logging.info(f"The bucket name is {bucket_folder}")

lake_metadata_file = 'rtf_lake_mapping_config.txt'
given_email_file = "rtf_given_email.txt"
# reading email
given_email_param = pd.read_csv("s3://" + bucket_folder + "/config_files/" + given_email_file,
                                delimiter='\s*=\s*',
                                header=None,
                                names=['key', 'value'],
                                dtype=dict(key=np.object, value=np.object),
                                index_col=['key']).to_dict()['value']

given_email = common_functions.eval_dtype(given_email_param['given_email'])


def get_string_between(s, start, end):
    return s[s.find(start) + len(start):s.rfind(end)]

for each_email in given_email:
    print("each_email", each_email)
    print("file opening is", my_s3_path + each_email + "_rtf_backupfile_logs.txt")
    with open(my_s3_path + each_email + "_rtf_backupfile_logs.txt", 'r') as bf:
        next(bf)
        for line in bf:
            if len(line) > 1:
                bucket_name, file_name = get_string_between(line, 'Bucket:', "+'File:"), get_string_between(line,
                                                                                                            "+'File:'+",
                                                                                                            '')
                print(f"bucket name is {bucket_name} and file name is {file_name}")
                rtf_datalake_update_helpers.delete_s3_file(bucket_name.strip(), file_name.strip())

    bf.close()
