"""A stocker moves the data from the barn to the warehouse."""

import logging
import logging.config
import os
import shutil
import sys
from hashlib import md5 as hash_func

import yaml

from rabbit import Rabbit

RACK = "originals"

# Setup the logger.
logging.config.dictConfig(yaml.load(open("logging.yml", 'r'), yaml.FullLoader))
logger = logging.getLogger('stocker')

# Load the configuration file.
CFG_FILE = sys.argv[1] if len(sys.argv) > 1 else 'config.yml'
with open(CFG_FILE, 'r') as f:
    CFG = yaml.load(f, Loader=yaml.FullLoader)


class Stocker:

    def __init__(self, barn, warehouse):
        """A stocker moves the data from the barn to the warehouse.

        Args:
            barn: the direcotry where the raw data is stored.
            warehouse: the directory where the indexed data is stored.
        """
        self.barn = barn
        self.warehouse = warehouse
        self.rabbit = Rabbit(address=CFG['rabbitmq']['address'],
                             queue=CFG['rabbitmq']['queue'],
                             talking=True)

    def list_files(self, dir):
        """List all the files in the dir."""
        all_files = []
        for dirpath, _, files in os.walk(dir):
            all_files.extend([os.path.join(dirpath, f) for f in files])

        return all_files

    def check_inventory(self):
        """List all the files in the barn."""
        files = self.list_files(self.barn)

        # If any job left, tell people.
        if files:
            logger.debug("New files discovered: {}".format(len(files)))
            for new_file in files:
                self.rabbit.speak(new_file)

    def get_checksum(self, file_path):
        """Get the hash value of the input file."""
        with open(file_path, 'rb') as f:
            return hash_func(f.read()).hexdigest()

    def stock(self, src_file):
        """Stock the warehouse with the target file.

        Args:
            src_file: the source file to be stocked.

        Returns:
            succeed: a flag indicating the process succeeds.
            target: the full path of the target file saved.
        """
        # Get the new path of the file.
        hash_value = self.get_checksum(src_file)
        new_name = hash_value + os.path.splitext(src_file)[-1]
        dst_dir = os.path.join(CFG["dirs"]["warehouse"], RACK, hash_value[0])

        # If the directories do not exist, make them.
        if not os.path.exists(dst_dir):
            os.makedirs(dst_dir)
        dst_file = os.path.join(dst_dir, new_name)

        # Move the file to the warehouse.
        try:
            new_path = shutil.copy2(src_file, dst_file)
            succeed = True
            logger.debug("{}: File saved.".format(new_path))
        except:
            new_path = None
            succeed = False
            logger.debug("{}: Failed to move.".format(src_file))

        return succeed, new_path

    def destry(self, file_path):
        """Destry a file."""
        os.remove(file_path)
