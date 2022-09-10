from awsglue.transforms import *
import warnings
from awsglue.utils import getResolvedOptions
import sys
import logging

warnings.filterwarnings("ignore")

try:
    import rtf_initial_config_file

except Exception as exception:
    print(exception, False)
    raise

environment_args = getResolvedOptions(sys.argv, ['s3_rtf'])
bucket_folder = environment_args['s3_rtf']
my_s3_path = "s3://"+bucket_folder+"/config_files/"
logging.info(f"The bucket name is {bucket_folder}")

#required cnfig files
initial_config_file = my_s3_path+'rtf_initial_mapping_config.txt'
# This create initial config file, if to create in s3 pass my_s3_path for local don' pass that
# create_initial_config_file(my_s3_path+initial_config_file)#s3
rtf_initial_config_file.create_initial_config_file(initial_config_file)
logging.info(f"Initial config file is created.")