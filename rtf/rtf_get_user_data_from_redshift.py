import sys
import warnings
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions

import pandas as pd
import numpy as np
import logging

from smart_open import open

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

given_email_file = "rtf_given_email.txt"
redshift_metadata_file = 'rtf_redshift_mapping_config.txt'

# get redshift metadata in
redshift_metadata_parameters = pd.read_csv(my_s3_path + redshift_metadata_file,
                                           delimiter='\s*=\s*',
                                           header=None,
                                           names=['key', 'value'],
                                           dtype=dict(key=np.object, value=np.object),
                                           index_col=['key']).to_dict()['value']

all_email_cols = common_functions.eval_dtype(redshift_metadata_parameters['all_email_cols'])

rest_all_pii_columns = common_functions.eval_dtype(redshift_metadata_parameters['all_pii_columns'])

all_pii_table_with_email = common_functions.eval_dtype(redshift_metadata_parameters['all_pii_table_with_email'])

pii_table_list = common_functions.eval_dtype(redshift_metadata_parameters['pii_table_list'])


non_email_table = list(set(pii_table_list) - set(all_pii_table_with_email))
# get redshift metadata in
given_email_param = pd.read_csv(my_s3_path + given_email_file,
                                delimiter='\s*=\s*',
                                header=None,
                                names=['key', 'value'],
                                dtype=dict(key=np.object, value=np.object),
                                index_col=['key']).to_dict()['value']

given_email = common_functions.eval_dtype(given_email_param['given_email'])

additional_column = common_functions.intersect(rest_all_pii_columns, candidate_key_columns)


conn = common_functions.getconnection(redshift_gdpr_config['database'], redshift_gdpr_config['host'], redshift_gdpr_config['port'], redshift_gdpr_config['user'],
                                      redshift_gdpr_config['password'])

for email_i, email in enumerate(given_email):

    logging.info(f"Running for email {email}")

    list_of_tables_with_data_found = []
    list_of_tables_with_data_not_found = []

    with open(my_s3_path + "rtf_redshift_data1_" + str(email) + ".csv", 'w') as f:

        for table in all_pii_table_with_email:

            logging.info("calling for table {table}" )

            # get all columns of the table being called to check which column to fire the query on..

            table_name = table.split(".")[1]

            get_columns_query = "SELECT column_name FROM svv_columns where table_name=" + "'" + table_name + "';"

            logging.info("getting columns of the table..")

            all_columns = common_functions.runquery(conn, get_columns_query)

            # get common columns with email columns

            common_columns = list(set(all_columns['column_name']).intersection(all_email_cols))

            logging.info("email columns in the table are..", common_columns)

            # checking the data for each column..

            for col in common_columns:

                query = "SELECT * FROM " + table + " where " + str(col) + "=" + "'" + str(email) + "';"

                logging.info(f"query is {query}")

                try:

                    my_data = common_functions.runquery(conn, query)

                    if len(my_data) != 0:

                        logging.info(f"got data for columns {col}")

                        logging.info(f"Found {len(my_data)} records")

                        my_data_to_dict = my_data.to_dict()

                        logging.info("writing data to file..")

                        f.write("for table" + str(table) + " data " + str(my_data_to_dict) + '\n')

                        # get potential key column values

                        potential_key = common_columns.copy()
                        potential_key.remove(col)
                        potential_key.extend(additional_column)

                        logging.info(f"potential_key are {potential_key}")

                        # potential_key are the columns for which we want to get data

                        potentail_data_dict = {}

                        for i in potential_key:

                            if i in my_data.columns:
                                potentail_data_dict['table'] = table
                                potentail_data_dict['column'] = i
                                potentail_data_dict['value'] = my_data[i].iloc[0]

                        list_of_tables_with_data_found.append(potentail_data_dict)
                    else:
                        list_of_tables_with_data_not_found.append(table)
                except Exception as e:
                    logging.info("Exception in first raverse")
                    logging.exception(e)
                    pass

    f.close()

    logging.info(f"list_of_tables_with_data_found {list_of_tables_with_data_found}")
    logging.info(f"list_of_tables_with_data_not found {list_of_tables_with_data_not_found}")

    # get the list of all potential columns

    all_potential_columns = [d['column'] for d in list_of_tables_with_data_found]
    all_potential_values = [d['value'] for d in list_of_tables_with_data_found]

    zipped_list = list(zip(all_potential_columns, all_potential_values))

    # all tables which someway has data

    all_table_which_has_data = [d['table'] for d in list_of_tables_with_data_found]

    reamining_tables = list(set(list_of_tables_with_data_not_found) - set(all_table_which_has_data))
    reamining_tables.extend(non_email_table)

    logging.info(f"reamining_tables to still look for {reamining_tables}")

    with open(my_s3_path + "rtf_redshift_data2_" + str(email) + ".csv", 'w') as ff:

        for table in reamining_tables:

            logging.info(f"table name {table}")

            table_name = table.split(".")[1]

            get_columns_query = "SELECT column_name FROM svv_columns where table_name=" + "'" + table_name + "';"

            logging.info("getting columns of the table..")

            all_columns = common_functions.runquery(conn, get_columns_query)

            logging.info(f"all_columns {all_columns['column_name']}")

            for col, value in zipped_list:

                logging.info(f"Col searching for {col}")

                if col in list(all_columns['column_name']):

                    query = "SELECT * FROM " + table + " where " + str(col) + "=" + "'" + str(value) + "';"

                    logging.info(f"query is {query}")

                    try:
                        my_data = common_functions.runquery(conn, query)

                        if len(my_data) != 0:

                            logging.info(f"got data for columns {col}")

                            logging.info(f"Found {len(my_data)} records")

                            my_data_to_dict = my_data.to_dict()

                            logging.info("writing data to file..")

                            ff.write("for table" + str(table) + " data " + str(my_data_to_dict) + '\n')

                            break

                        else:

                            logging.info(f"Data still not found for this {table}")

                    except Exception as e:
                        logging.info("Exception in second traverse")
                        logging.exception(e)
                        pass

                    else:
                        logging.info("Columns not found in tables")

    logging.info('Completed.')
    ff.close()