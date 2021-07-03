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

