#getlatlongs.py
import logging
import sys
from gkglib.managedb import *
from gkglib.gkglogging import *

# Main
def main():

    # create a database connection
    conn = create_connection(database)
    if conn is None:
        logging.error("Error! cannot create the database connection.")
        return 1
   
    # Get the GKG IDs and Locations
    rows = select_locations(conn)
    for row in rows:
        print (row)

if __name__ == '__main__':
    main()