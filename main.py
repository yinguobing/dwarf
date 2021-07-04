"""Dwarf aggregate files of interest and stores detailed information in the 
dataset.
"""
import sys
import yaml

from clerk import Clerk
from porter import Porter
from stocker import Stocker
from steward import Steward

# Load the configuration file.
CFG_FILE = sys.argv[1] if len(sys.argv) > 1 else 'config.yml'
with open(CFG_FILE, 'r') as f:
    CFG = yaml.load(f, Loader=yaml.FullLoader)

if __name__ == "__main__":

    # Employ a porter to watch the barn for new files.
    Jack = Porter(CFG['dirs']['barn'])

    # Employ a stocker to fill the warehouse.
    Tom = Stocker(CFG['dirs']['barn'],
                  CFG['dirs']['warehouse'])

    # Employ a clerk to manage the books.
    Julie = Clerk(CFG['mongodb']["host"],
                  CFG['mongodb']['port'],
                  CFG['mongodb']['username'],
                  CFG['mongodb']['password'],
                  CFG["mongodb"]['name'])

    # Employ a steward to manage the entire process.
    Andrew = Steward(Tom, Julie)

    # Let the process begin.
    try:
        Jack.start_watching()
        Andrew.start_processing()
    except KeyboardInterrupt:
        print("Interupted by keyboard.")
