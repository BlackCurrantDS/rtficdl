"""This script writes the initial config file with default columns and read if it has changes"""

from smart_open import open

default_map_column = 'email'
candidate_key_columns = ['key', 'id', 'hash']
search_phrase = 'pii'


def create_initial_config_file(filename):
    # filename can be s3 location or local

    # create an intial file wih default initial values
    with open(filename, 'w') as f:
        print("Opening the file...")

        f.write('default_map_column=' + str(default_map_column) + '\n')
        f.write('candidate_key_columns=' + str(candidate_key_columns) + '\n')
        f.write('search_phrase=' + str(search_phrase))

    print("File writing done!")
    f.close()