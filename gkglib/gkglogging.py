#gkglogging.py
import logging

logging.basicConfig(
    level=logging.DEBUG,  # minimum level capture in the FILE
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p',
    handlers=[
        logging.FileHandler("{0}/{1}.log".format(".", "log-gkg"), mode='w'),
        logging.StreamHandler()]
    )
