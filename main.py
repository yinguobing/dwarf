"""Dwarf aggregate files of interest and stores detailed information in the 
dataset.
"""
import sys
import yaml
import logging
import logging.config

from clerk import Clerk
from porter import Porter
from stocker import Stocker
from steward import Steward

# Load the configuration file.
CFG_FILE = sys.argv[1] if len(sys.argv) > 1 else 'config.yml'
with open(CFG_FILE, 'r') as f:
    CFG = yaml.load(f, Loader=yaml.FullLoader)

# Setup the logger.
logging.config.dictConfig(yaml.load(open("logging.yml", 'r'), yaml.FullLoader))
logger = logging.getLogger('root')

if __name__ == "__main__":

    # Employ a porter to watch the barn for new files.
    Jack = Porter(CFG['dirs']['barn'])
    logger.info("Porter is ready.")

    # Employ a stocker to fill the warehouse.
    Tom = Stocker(CFG['dirs']['barn'],
                  CFG['dirs']['warehouse'])
    logger.info("Stocker is ready.")

    # Employ a clerk to manage the books.
    Julie = Clerk(CFG['mongodb']["host"],
                  CFG['mongodb']['port'],
                  CFG['mongodb']['username'],
                  CFG['mongodb']['password'],
                  CFG["mongodb"]['name'])
    logger.info("Clark is ready.")

    # Employ a steward to manage the entire process.
    Andrew = Steward(Tom, Julie)
    logger.info("Steward is ready.")

    # Let the process begin.
    try:
        Jack.start_watching()
        Andrew.start_processing()
    except KeyboardInterrupt:
        Jack.stop()
        print("Interupted by keyboard.")

