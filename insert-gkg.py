# insert-gkg.py
import logging
import os.path
import sys
from gkglib.managedb import *
from gkglib.gkglogging import *


UNZIPS_DIR = "data/unzips"
TEST_DIR = "data/test-csvs"
#SOURCE_DIR = UNZIPS_DIR  # point to test or prod dir
SOURCE_DIR = TEST_DIR  # point to test or prod dir

# Diagnostic metrics
diags = {
    'gkg_entries_lowest' : 999999,
    'gkg_entries_highest' : 0,
    'gkg_entries_total' : 0,
    'files_processed' : 0,
    'sql_rows_created' : 0,
    'fields_errors' : 0   
}


def make_fields(fname, line_num, gkg_line):
    fields_list = gkg_line.split("\t")
    if(len(fields_list) != 27):
        logging.error("FIELDS LENGTH IS " + str(len(fields_list)) + " File: " + fname + " Line: " + str(line_num))
        return 1, fields_list
    else:
        return 0, fields_list

def insert_gkg_item(conn, fields_list):
    
    with conn:
        num_created = create_gkgrow(conn, tuple(fields_list))

    return num_created

# Main
def main():

    # create a database connection
    conn = create_connection(database)
    if conn is not None:
        # create project table
        create_table(conn, sql_create_projects_table)
        # create gkg table
        create_table(conn, sql_create_gkg_table)
        
    else:
        logging.error("Error! cannot create the database connection.")


    file_list = next(os.walk(SOURCE_DIR))[2]
    for csv_file in file_list:

        # Open CSV
        try:
            fh = open(SOURCE_DIR + '/' + csv_file, 'rt', encoding="ISO-8859-1")  # r: read, t: text
        except:
            logging.error("Unable to open " + csv_file)    

        # Read Lines
        gkg_entries_this = 0
        try:
            for line in fh.readlines():
                gkg_entries_this += 1
                diags['gkg_entries_total'] += 1
                fields_err, fields_list = make_fields(csv_file, gkg_entries_this, line)
                if (fields_err == 0):
                    # row s/b 0 (error or duplicate) or 1 (new row added)
                    row = insert_gkg_item(conn, fields_list)
                    diags['sql_rows_created'] += row
                else:
                    diags['fields_errors'] += 1

        except Exception as e:
            logging.error("Failed reading " + csv_file)
            logging.error(e)
            sys.exit()

        logging.info("Found " + str(gkg_entries_this) + " in " + csv_file)

        if gkg_entries_this < diags['gkg_entries_lowest']:
            diags['gkg_entries_lowest'] = gkg_entries_this  
        
        if gkg_entries_this > diags['gkg_entries_highest']:
            diags['gkg_entries_highest'] = gkg_entries_this

        diags['files_processed'] += 1

    # Diagnostics
    logging.info("RESULTS: ")
    for key, value in diags.items():
        logging.info(key + ": " + str(value))

if __name__ == '__main__':
    main()