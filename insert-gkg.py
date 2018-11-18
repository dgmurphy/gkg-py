# insert-gkg.py
import logging
import os.path
import sys
from gkglib.managedb import *
from gkglib.gkglogging import *


UNZIPS_DIR = "data/unzips"
TEST_DIR = "data/test-csvs"
SOURCE_DIR = UNZIPS_DIR  # point to test or prod dir
#SOURCE_DIR = TEST_DIR  # point to test or prod dir

# Diagnostic metrics
diags = {
    'gkg_entries_lowest' : 999999,
    'gkg_entries_highest' : 0,
    'gkg_entries_total' : 0,
    'files_processed' : 0,
    'sql_rows_created' : 0,
    'fields_errors' : 0,
    'inserts_ignored' : 0
}


def make_fields(fname, line_num, gkg_line):
    fields_list = gkg_line.split("\t")
    if(len(fields_list) != 27):
        logging.error("FIELDS LENGTH IS " + str(len(fields_list)) + " File: " + fname + " Line: " + str(line_num))
        return 1, fields_list
    else:
        return 0, fields_list

def insert_gkg_item(conn, fname, line_num, gkg_line):

    num_inserted = 0

    fields_err, fields_list = make_fields(fname, line_num, gkg_line)

    if (fields_err == 0):
        with conn:
            num_inserted = create_gkgrow(conn, tuple(fields_list)) # INSERY GKG 
            
        diags['sql_rows_created'] += num_inserted
        print('.', end='', flush=True)

        if num_inserted == 0:
            diags['inserts_ignored'] += 1
        
    else:
        diags['fields_errors'] += 1


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
                insert_gkg_item(conn, csv_file, gkg_entries_this, line) # INSERT INTO SQLITE
                

        except Exception as e:
            logging.error("Failed reading " + csv_file)
            logging.error(e)
            sys.exit()

        logging.info("\nFound " + str(gkg_entries_this) + " in " + csv_file)

        if gkg_entries_this < diags['gkg_entries_lowest']:
            diags['gkg_entries_lowest'] = gkg_entries_this  
        
        if gkg_entries_this > diags['gkg_entries_highest']:
            diags['gkg_entries_highest'] = gkg_entries_this

        diags['files_processed'] += 1

        # Diagnostics
        logging.info("-------- STATUS -------")
        for key, value in diags.items():
            logging.info(key + ": " + str(value))

if __name__ == '__main__':
    main()