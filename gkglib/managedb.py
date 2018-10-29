# managedb.py
import sqlite3
from sqlite3 import Error
from gkglib.gkglogging import *

database = "db/gkgsqlite.db"


# GKG Table
sql_create_gkg_table = """ CREATE TABLE IF NOT EXISTS gkg (
                                        GKGRECORDID text PRIMARY KEY,
                                        DATE integer,
                                        SourceCollectionIdentifier integer,
                                        SourceCommonName text,
                                        DocumentIdentifier text,
                                        Counts text,
                                        V2Counts text,
                                        Themes text,
                                        V2Themes text,
                                        Locations text,
                                        V2Locations text,
                                        Persons text,
                                        V2Persons text,
                                        Organizations text,
                                        V2Organizations text,
                                        V2Tone text,
                                        Dates text,
                                        GCAM text,
                                        SharingImage text,
                                        RelatedImages text,
                                        SocialImageEmbeds text,
                                        SocialVideoEmbeds text,
                                        Quotations text,
                                        AllNames text,
                                        Amounts text,
                                        TranslationInfo text,
                                        Extras
                                    ); """

# SAMPLES 
sql_create_projects_table = """ CREATE TABLE IF NOT EXISTS projects (
                                        id integer PRIMARY KEY,
                                        name text NOT NULL,
                                        begin_date text,
                                        end_date text
                                    ); """
 
sql_create_tasks_table = """CREATE TABLE IF NOT EXISTS tasks (
                                    id integer PRIMARY KEY,
                                    name text NOT NULL,
                                    priority integer,
                                    status_id integer NOT NULL,
                                    project_id integer NOT NULL,
                                    begin_date text NOT NULL,
                                    end_date text NOT NULL,
                                    FOREIGN KEY (project_id) REFERENCES projects (id)
                                );"""

# Functions
def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(db_file)
        logging.info("Connecting to: " + db_file + " sqlite version: " + sqlite3.version)
        return conn
    except Error as e:
        logging.error(e)
 
    return None

def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        logging.error(e)

def create_gkgrow(conn, gkg):
    """
    Create a new gkg item
    :param conn:
    :param gkg:
    :return:
    """
 
    sql = ''' INSERT INTO gkg
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, gkg)
    return cur.lastrowid


# Sample only
def create_project(conn, project):
    """
    Create a new project into the projects table
    :param conn:
    :param project:
    :return: project id
    """
    sql = ''' INSERT INTO projects(name,begin_date,end_date)
              VALUES(?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, project)
    return cur.lastrowid