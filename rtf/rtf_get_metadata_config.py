import pandas as pd
from awsglue.transforms import *
import numpy as np
import warnings
from smart_open import open
from awsglue.utils import getResolvedOptions
import sys
import logging

warnings.filterwarnings("ignore")

try:
    from rtf_job_helpers import common_functions, redshift_gdpr_config
    import rtf_initial_config_file
    import rtf_get_glue_metadata
    import rtf_redshift_mapping_metadata

except Exception as exception:
    print(exception, False)
    raise

environment_args = getResolvedOptions(sys.argv, ['s3_rtf'])


bucket_folder = environment_args['s3_rtf']
my_s3_path = "s3://"+bucket_folder+"/config_files/"
logging.info(f"The bucket name is {bucket_folder}")

#required config files
initial_config_file = my_s3_path+'rtf_initial_mapping_config.txt'
lake_config_file = my_s3_path+'rtf_lake_mapping_config.txt'
redshift_config_file = my_s3_path+'rtf_redshift_mapping_config.txt'
redshift_metadata_file = my_s3_path+'rtf_redshift_complete_metadata.csv'
all_email_data = my_s3_path+'rtf_all_email_data_from_glue.txt'
redshift_columns_to_update_config_file = my_s3_path+'rtf_redshift_columns_to_update.txt'
datalake_columns_to_update_config_file = my_s3_path+'rtf_datalake_columns_to_update.txt'


# if there is change in the initial config file read those again to get tables based on new configuration
# get initial configuration file in
my_parameters = pd.read_csv(initial_config_file,
                            delimiter='\s*=\s*',
                            header=None,
                            names=['key', 'value'],
                            dtype=dict(key=np.object, value=np.object),
                            index_col=['key']).to_dict()['value']

# reading the parameters again
default_map_column = my_parameters['default_map_column']
candidate_key_field = my_parameters['candidate_key_columns']
search_phrase = my_parameters['search_phrase']

logging.info(f"Paramters from config files are: default_map_column is {default_map_column} candidate_key_field is {candidate_key_field} and search_phrase is {search_phrase}")


# getting lake metadata

logging.info("Getting lake metadata..")

# get tables from glue catalogue matching with the key column to map

db_name_list, pii_database_list = rtf_get_glue_metadata.get_glue_databases(search_phrase)

# print("All databases are:", db_name_list)
# print("All {} databases are:{}".format(search_phrase,pii_database_list))

# getting all the tables and columns

pii_tables_with_email_columns, pii_tables_without_email_column, email_column_names, key_cols_name, rest_all_pii_columns, all_pii_cols_with_email = rtf_get_glue_metadata.get_glue_tables(
    pii_database_list, search_phrase, default_map_column)

# keeping it all in dictionary

lake_meta_data_dicts = {}

lake_meta_data_dicts['database_name_list'] = db_name_list

lake_meta_data_dicts['pii_database_name_list'] = pii_database_list

# lake_meta_data_dicts['all_tables'] = pii_tables #TODO: replace all fucntions with the module call

all_email_columns = common_functions.remove_duplicate_list_items(
    [item for sub_list in email_column_names for item in sub_list])

lake_meta_data_dicts['all_email_columns'] = all_email_columns

lake_meta_data_dicts['all_tables_with_email_columns'] = common_functions.remove_duplicate_list_items(
    pii_tables_with_email_columns)

lake_meta_data_dicts['all_pii_tables_without_email_columns'] = common_functions.remove_duplicate_list_items(
    pii_tables_without_email_column)

rest_pii_col_has_email = list(
    set(list(filter(lambda column_name: default_map_column not in column_name, all_pii_cols_with_email))))

rest_pii_col_without_email = common_functions.remove_duplicate_list_items(rest_all_pii_columns)

lake_meta_data_dicts['rest_all_pii_columns'] = common_functions.remove_duplicate_list_items(
    rest_pii_col_has_email + rest_pii_col_without_email)

# write all lake meta data to config file

with open(lake_config_file, 'w') as f:
    logging.info("Opening the file...")

    for key, value in lake_meta_data_dicts.items():
        f.write(str(key) + '=' + str(value) + '\n')

    logging.info("File writing done!")

f.close()

#writing columns to update for datalake
with open(datalake_columns_to_update_config_file, 'w') as f:
    logging.info("Opening the file...")

    f.write('rest_all_pii_columns =' + str(lake_meta_data_dicts.get("rest_all_pii_columns","")))

    logging.info("File writing done!")

f.close()

# getting redshift metadata...

logging.info("Getting Redshift metadata..")



# calling the connection to redshift

conn = common_functions.getconnection(redshift_gdpr_config['database'], redshift_gdpr_config['host'], redshift_gdpr_config['port'], redshift_gdpr_config['user'],
                                                   redshift_gdpr_config['password'])

# get complete metadata
all_meatdata_df \
, all_schema \
, pii_schema_list \
, pii_table_list \
, all_email_cols \
, all_tables_with_email \
, all_pii_table_with_email \
, all_pii_columns \
, all_tables_with_email_schema \
, all_pii_table_with_email_schema \
,all_pii_tables_with_schema = rtf_redshift_mapping_metadata.runquery(conn, '''SELECT * FROM svv_columns;''', search_phrase, default_map_column, redshift_metadata_file)

logging.info("writing to redshift metatdata file..")

with open(redshift_config_file, 'w') as f:
    f.write("all_schemas=" + str(list(set(all_schema))) + '\n')
    f.write("pii_schema_list=" + str(list(set(pii_schema_list))) + '\n')
    f.write("pii_table_list=" + str(list(set(all_pii_tables_with_schema))) + '\n')
    f.write("all_email_cols=" + str(list(set(all_email_cols))) + '\n')
    f.write("all_table_with_email=" + str(list(set(all_tables_with_email))) + '\n')
    f.write("all_pii_table_with_email=" + str(list(set(all_pii_table_with_email_schema))) + '\n')
    f.write("all_pii_columns=" + str(list(set(all_pii_columns))))

f.close()

with open(redshift_columns_to_update_config_file, 'w') as f:
    f.write("columns_to_update=" + str(list(set(all_pii_columns))))

f.close()

logging.info("Writing done for redshift metadata config file...")