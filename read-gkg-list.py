# read-gkg-list.py
import logging
import urllib.request
import zipfile
import os.path


# Constants
GKG_FILENAME_MATCH = ".gkg.csv.zip"
ZIPS_DIR = "data/zips"
UNZIPS_DIR = "data/unzips"
FILELIST = "data/file-lists/gkg-filelist-oct20-2018.txt"

# Logging
logging.basicConfig(
    level=logging.DEBUG,  # minimum level capture in the file
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p',
    handlers=[
        logging.FileHandler("{0}/{1}.log".format(".", "log-gkg"), mode='w'),
        logging.StreamHandler()]
    )

# Function Defs
def get_gkg_file_list():

    gkg_file_list = []
    all_file_list_len = 0
    fh = open(FILELIST, 'rt')  # r: read, t: text

    for line in fh.readlines():
        all_file_list_len += 1
        if( GKG_FILENAME_MATCH in line ):
            gkg_file_list.append(line.strip())
           
    logging.info('Found ' + str(all_file_list_len) + ' entries.')
    logging.info('Found ' + str(len(gkg_file_list)) + ' gkg files')

    fh.close()
    return gkg_file_list

def download_file(gkg_file):

    url = gkg_file[gkg_file.find("http"):]
    file_name = ZIPS_DIR + "/" + url[url.rfind("/") + 1:]
    # Download the file from `url` and save it locally under `file_name`:
    logging.info("Downloading from " + url + " to " + file_name)
    urllib.request.urlretrieve(url, file_name)
    return file_name

def check_files():
    zips =  next(os.walk(ZIPS_DIR))[2]
    csvs =  next(os.walk(UNZIPS_DIR))[2]
    logging.info("Zip files: " + str(len(zips)))
    logging.info("CSV files: " + str(len(csvs)))



# Main
if not os.path.exists(ZIPS_DIR):
    os.makedirs(ZIPS_DIR)

if not os.path.exists(UNZIPS_DIR):
    os.makedirs(UNZIPS_DIR)

gkg_file_list = get_gkg_file_list()
downloaded_zips = []
for gkg_file in gkg_file_list:
    zip_download = download_file(gkg_file)
    downloaded_zips.append(zip_download)

num_unzipped = 0
for zip_file in downloaded_zips:
    logging.info("Unzipping: " + zip_file)
    with zipfile.ZipFile(zip_file,"r") as zip_ref:
        zip_ref.extractall(UNZIPS_DIR)
    num_unzipped += 1
logging.info("Unzipped " + str(num_unzipped))
check_files()
logging.info("Done.")