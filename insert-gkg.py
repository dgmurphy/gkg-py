# insert-gkg.py
import logging
import os.path
import sys
#import re

UNZIPS_DIR = "data/unzips"
TEST_DIR = "data/test-csvs"
SOURCE_DIR = UNZIPS_DIR  # point to test or prod dir

# Logging
logging.basicConfig(
    level=logging.DEBUG,  # minimum level capture in the file
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p',
    handlers=[
        logging.FileHandler("{0}/{1}.log".format(".", "log-gkg"), mode='w'),
        logging.StreamHandler()]
    )

# Functions
def insert_gkg_item(fname, line_num, gkg_line):

    fields_list = gkg_line.split("\t")
    #logging.info("Num Fields: " + str(len(fields_list)))

    if(len(fields_list) != 27):
        logging.error("FIELDS LENGTH IS " + str(len(fields_list)) + " File: " + fname + " Line: " + str(line_num))
        return 1

    i = 1
    for field in fields_list:
        #logging.info("Field " + str(i) + ": " + field)
        i += 1

    #logging.info("SQL Inserting line " + str(line_num) + " from " + str(fname))

    return 0

# Main
file_list =  next(os.walk(SOURCE_DIR))[2]
gkg_entries_lowest =  999999
gkg_entries_highest = 0
gkg_entries_total = 0
files_processed = 0
insert_errors = 0

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
            gkg_entries_total += 1
            #logging.info("Processing line: " + str(gkg_entries_this))
            errval = insert_gkg_item(csv_file, gkg_entries_this, line)
            insert_errors += errval

    except Exception as e:
        logging.error("Failed reading " + csv_file)
        print(e)
        sys.exit()

    logging.info("Found " + str(gkg_entries_this) + " in " + csv_file)

    if (gkg_entries_this < gkg_entries_lowest):
        gkg_entries_lowest = gkg_entries_this  
    elif (gkg_entries_this > gkg_entries_highest):
        gkg_entries_highest = gkg_entries_this

    files_processed += 1

logging.info("Files processed: " + str(files_processed))
logging.info("Total entries found: " + str(gkg_entries_total))
logging.info("Largest in one file: " + str(gkg_entries_highest))
logging.info("Smallest in one file: " + str(gkg_entries_lowest))
logging.info("Insert Errors: " + str(insert_errors))
logging.info("Done.")