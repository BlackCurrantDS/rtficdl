"""This is helper file to  get common functions"""

import ast
import json

import pg8000 as dbapi
import pandas as pd
import boto3

client = boto3.client("secretsmanager", region_name="your_region")


get_secret_value_response = client.get_secret_value(
        SecretId="your_secret"
)

secret = get_secret_value_response['SecretString']
rds_secret = json.loads(secret)

rds_host = rds_secret.get("host")
rds_dbname = rds_secret.get("dbname")
rds_username = rds_secret.get("username")
rds_password = rds_secret.get("password")
rds_port = rds_secret.get("port")
redshift_gdpr_config = {
        "database": rds_dbname,
        "host": rds_host,
        "port": rds_port,
        "user": rds_username,
        "password": rds_password
    }


class common_functions:

    @staticmethod
    # Compare two lists and get common items
    def get_common_items(list1, list2):

        common_item_list = list(set(list1).intersection(list2))

        return common_item_list

    @staticmethod
    # remove duplicates from the list

    def remove_duplicate_list_items(in_list):

        out_list = list(set(in_list))

        return out_list

    @staticmethod
    # evaluating the values read from the file

    def eval_dtype(input):

        output = ast.literal_eval(input)

        return output

    @staticmethod
    # getting intersect in list

    def intersect(List1, List2):

        # empty list for values that match
        ret = []
        for i in List2:
            for j in List1:
                if i in j:
                    ret.append(j)
        return ret

    # fucntion to get the database connection
    def getconnection(database, host, port, user, password):

        try:
            connection_db = dbapi.connect(
                database=database,
                user=user,
                password=password,
                host=host,
                port=port
            )
        except:
            print("Error getting DB connection")
            return None
        return connection_db

    # query..
    def runquery(conn, query):

        get_data = pd.read_sql_query(query, conn)

        return get_data

