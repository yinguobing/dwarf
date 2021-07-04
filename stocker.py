"""A stocker moves the data from the barn to the warehouse."""
import os
import shutil


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