import pandas as pd
from smart_open import open
import warnings
import os
import glob
from awsglue.utils import getResolvedOptions
import sys
import boto3
from smart_open import open
from pandas import Timestamp
import logging
import shutil
import numpy as np

try:
    from rtf_job_helpers import common_functions, redshift_gdpr_config
    from rtf_initial_config_file import candidate_key_columns
    import rtf_get_glue_metadata
    import rtf_redshift_mapping_metadata

except Exception as exception:
    print(exception, False)
    raise

warnings.filterwarnings("ignore")

environment_args = getResolvedOptions(sys.argv, ['s3_rtf'])

bucket_folder = environment_args['s3_rtf']
my_s3_path = "s3://" + bucket_folder + "/config_files/"

given_email_file = "rtf_given_email.txt"
# reading email
given_email_param = pd.read_csv(my_s3_path + given_email_file,
                                delimiter='\s*=\s*',
                                header=None,
                                names=['key', 'value'],
                                dtype=dict(key=np.object, value=np.object),
                                index_col=['key']).to_dict()['value']

given_email = common_functions.eval_dtype(given_email_param['given_email'])
print("given_email", given_email)


# HTML sytle helpers
def html_input(c):
    return '<input name="{}" value="{{}}" />'.format(c)


def html_edit(c):
    return '<div contenteditable=true><span style="color: #f00;">Y</div>'.format(c)


def row_style(c):
    return c.style.set_table_styles({0: [{'selector': 'td:hover', 'props': [('font-size', '25px')]}]}, axis=1,
                                    overwrite=False)


start = 'for table'
end = 'data '


def get_pretty_file(source_type, all_filenames, user_name):
    for f_number, file in enumerate(all_filenames):

        print("Processing for file..", file)

        with open(file) as f:

            lines = f.readlines()

            for i, line in enumerate(lines):

                with open(my_s3_path + "/lake_data_files/" + "df_" + source_type + str(i) + ".txt", 'w') as fw:

                    # print(line)

                    # get table name

                    table_name = line[line.find(start) + len(start):line.rfind(end)]

                    print("table_name", table_name)

                    # remove the table name part

                    string_to_replace = start + table_name + end

                    removed_line = line.replace(string_to_replace, "")
                    # replace nan

                    ff_removed_line = removed_line.replace('nan', "'no_data'")
                    ff_removed_line = ff_removed_line.replace('NaT', "'no_time'")
                    # json_to_dataframe(line).to_csv(data_file_out)
                    my_line = eval(ff_removed_line)

                    # my_line = json.dumps(removed_line)

                    # eval_line = eval(my_line)

                    df = pd.DataFrame(my_line)

                    df['update_flag'] = 'Y'
                    df['Source'] = source_type
                    df['Table Name'] = table_name

                    cols_to_move = ['Source', 'Table Name']
                    df = df[cols_to_move + [col for col in df.columns if col not in cols_to_move]]

                    # table_name = df['table_name'][0]

                    # html=df.style.format({c: html_input(c) for c in df.columns if c=='update_flag'}).render()
                    # html=df.style.set_table_styles([{'selector': '','props': [('border', '5px solid green')]}])
                    html = df.style.format({c: html_edit(c) for c in df.columns if c == 'update_flag'})
                    html.set_table_styles([{'selector': 'tr:hover', 'props': [('background-color', 'yellow'),
                                                                              ('border', '6px solid #000066')]}])
                    # html= df.to_html()

                    html = html.render()

                    if i == 0 and f_number == 0:

                        fw.write("<br></br>")
                        fw.write(
                            "<strong>Complete customer Data from for " + user_name + "</strong>")
                        fw.write("<br></br>")
                        # fw.write("<strong>Coumns to be Updated in Table "+table_name+" are "+str(columns_to_update)+"</strong>")
                        fw.write("<br></br>")
                        # fw.write("<div contenteditable="+"true"+">This text can be edited by the user.</div>")
                        fw.write(html)

                    else:
                        fw.write("<br></br>")
                        # fw.write("<strong>Coumns to be Updated in Table "+table_name+" are "+str(columns_to_update)+"</strong>")
                        fw.write("<br></br>")
                        fw.write(html)
                fw.close()
                print("File is closed")
            print(f"Wrote {i} dataframes")
        f.close()


def get_s3_file_list(bucket, prefix):
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket)
    return my_bucket.objects.filter(Prefix=prefix)


def combine_all_files(list_of_files, out_file):
    with open(out_file, 'wb') as wfd:
        for f in list_of_files:
            with open(f, 'rb') as fd:
                shutil.copyfileobj(fd, wfd)
            fd.close()
    wfd.close()


def delete_s3_file(bucket, key):
    s3_client = boto3.client('s3')
    s3_client.delete_object(Bucket=bucket, Key=key)


for each_email in given_email:

    redshift_file = []
    lake_file = []
    all_files = []
    for obj in get_s3_file_list(bucket_folder, 'config_files'):
        if obj.key.endswith('csv') and each_email in obj.key and 'redshift' in obj.key:
            redshift_file.append("s3://" + bucket_folder + '/' + obj.key)

    for obj in get_s3_file_list(bucket_folder, 'config_files'):
        if obj.key.endswith('csv') and each_email in obj.key and 'lake' in obj.key:
            lake_file.append("s3://" + bucket_folder + '/' + obj.key)

    # files to get get for each email
    print("redshift_file", redshift_file)

    print("lake_file", lake_file)

    # get the individual data frames
    if redshift_file:
        get_pretty_file('Redshift', redshift_file, each_email)
    if lake_file:
        get_pretty_file('DataLake', lake_file, each_email)

    # combine files into single html file
    for obj in get_s3_file_list(bucket_folder, 'config_files//lake_data_files'):
        if 'df' in obj.key and obj.key.endswith('txt'):
            all_files.append("s3://" + bucket_folder + '/' + obj.key)

    print("all files", all_files)

    if all_files:
        combine_all_files(all_files, my_s3_path + "/lake_data_files/" + each_email + "_output_file.html")

    for obj in get_s3_file_list(bucket_folder, 'config_files//lake_data_files'):
        if 'df' in obj.key and obj.key.endswith('txt'):
            delete_s3_file(bucket_folder, obj.key)