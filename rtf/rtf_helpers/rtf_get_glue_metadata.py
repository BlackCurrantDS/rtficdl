"""this script gets glue catalogue metadata"""

import boto3

def get_glue_databases(search_phrase):
    # gets all database list and also database which has 'pii' in name

    glue = boto3.client('glue')

    db_name_list = [db['Name'] for db in glue.get_databases()['DatabaseList']]

    # print("List of all databases in Glue Catalogue", db_name_list)

    pii_database_list = [s for s in db_name_list if search_phrase in s]

    return db_name_list, pii_database_list


def get_glue_tables(databse_to_look_into_list, search_phrase, default_map_column):
    pii_tables_with_email_columns = []
    pii_tables_without_email_column = []
    email_column_names = []
    key_cols_name = []
    rest_all_pii_columns = []
    all_pii_cols_with_email = []

    pii_tables = {}

    glue = boto3.client('glue')

    paginator = glue.get_paginator('get_tables')

    for i, database in enumerate(databse_to_look_into_list):

        # print("Fecthing database...",database)

        database_name = database

        page_iterator = paginator.paginate(DatabaseName=database)  # given database

        for page in page_iterator:

            # print("Table in database",page['TableList'])

            for cnt, tables in enumerate(page['TableList']):

                if search_phrase in tables['Name']:  # if the table has pii data

                    table_name = tables['Name']

                    name = "database=" + database_name + "_table=" + table_name + '_' + str(cnt)

                    pii_tables[name] = tables['StorageDescriptor']['Columns']

                    email_cols = list(filter(lambda column_name: default_map_column in column_name['Name'],
                                             tables['StorageDescriptor']['Columns']))

                    if email_cols:

                        email_column_names.append([x['Name'] for x in email_cols])

                        for columns in tables['StorageDescriptor']['Columns']:
                            # getting tables with email column
                            pii_tables_with_email_columns.append(database_name + "." + tables['Name'])

                            # get all other column names
                            all_pii_cols_with_email.append(columns['Name'])

                    else:
                        # tables with pii data but not the email column

                        for columns in tables['StorageDescriptor']['Columns']:
                            pii_tables_without_email_column.append(database_name + "." + tables['Name'])

                            rest_all_pii_columns.append(columns['Name'])

    return pii_tables_with_email_columns, pii_tables_without_email_column, email_column_names, key_cols_name, rest_all_pii_columns, all_pii_cols_with_email