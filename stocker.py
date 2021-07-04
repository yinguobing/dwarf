"""A stocker moves the data from the barn to the warehouse."""
import os
import shutil
import sys
from hashlib import md5 as hash_func

import yaml

RACK = "originals"

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

    def list_files(self, dir):
        """List all the files in the dir."""
        all_files = []
        for dirpath, _, files in os.walk(dir):
            all_files.extend([os.path.join(dirpath, f) for f in files])

        return all_files

    def check_inventory(self):
        """List all the files in the barn."""
        return self.list_files(self.barn)

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
            new_path = shutil.move(src_file, dst_file)
            succeed = True
        except:
            new_path = None
            succeed = False
            print("Can not move file.")

        return succeed, new_path
