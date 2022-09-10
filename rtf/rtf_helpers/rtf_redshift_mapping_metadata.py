import pandas as pd


# get metatdata

def runquery(conn, query, search_phrase, default_map_column, redshift_metadata_outfile):
    """
    Just run a query given a connection
    """

    print("query is", query)
    print("conn is", conn)

    all_meatdata_df = pd.read_sql(query, conn)

    all_meatdata_df.to_csv(redshift_metadata_outfile, index=False)

    # all schemas

    all_schema = all_meatdata_df['table_schema'].unique()

    # all pii schemas

    pii_schema_list = [s for s in all_schema if search_phrase in s]

    # all pii tables

    pii_table_list = [s for s in all_meatdata_df['table_name'] if search_phrase in s]

    # all pii tables with schema

    combined_list = all_meatdata_df['table_schema'] + '.' + all_meatdata_df['table_name']

    all_pii_tables_with_schema = [s for s in combined_list if search_phrase in s]

    # all email like columns

    all_email_cols = [s for s in all_meatdata_df['column_name'] if default_map_column in s]

    # all pii table which has email column

    all_tables_with_email = all_meatdata_df.loc[all_meatdata_df['column_name'].isin(all_email_cols)]['table_name']

    # to get it with schema information

    new_df = all_meatdata_df.loc[all_meatdata_df['column_name'].isin(all_email_cols)]

    all_tables_with_email_schema = new_df['table_schema'] + '.' + new_df['table_name']

    # all pii tables wiht email

    all_pii_table_with_email = [s for s in all_tables_with_email if search_phrase in s]

    all_pii_table_with_email_schema = [s for s in all_tables_with_email_schema if search_phrase in s]

    # all pii column names except email

    all_pii_columns = all_meatdata_df.loc[all_meatdata_df['table_name'].isin(pii_table_list)]['column_name']

    all_pii_columns = [item for item in all_pii_columns if item not in all_email_cols]

    return all_meatdata_df, all_schema, pii_schema_list, pii_table_list, all_email_cols, all_tables_with_email, all_pii_table_with_email, all_pii_columns, all_tables_with_email_schema, all_pii_table_with_email_schema, all_pii_tables_with_schema