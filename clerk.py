import logging
import logging.config
import yaml

from pymongo import MongoClient

# Setup the logger.
logging.config.dictConfig(yaml.load(open("logging.yml", 'r'), yaml.FullLoader))
logger = logging.getLogger('clerk')


class Clerk:

    def __init__(self, address, port, username, password, name):
        """Initialize a MongoDB client.

        Args:
            address: the host address, like `localhost`
            port: the port.
            name: the database name
        """
        self.client = MongoClient(address, port,
                                  username=username,
                                  password=password,
                                  authSource=name)
        self.db = self.client.get_database(name)
        try:
            self.db.get_collection("images").find_one()
        except:
            logger.error("Failed to read database, please check.")

    def set_collection(self, name):
        """Get the collection by name"""
        self.collection = self.db.get_collection(name)

    def check_existence(self, hash_value):
        """Check if the hash value existed in the current collection."""
        exists = self.collection.find_one({'hash': hash_value})
        return True if exists else False

    def check_existence(self, hash_value, collection):
        """Check if the hash value existed in the collection."""
        exists = self.db.get_collection(collection).find_one(
            {'hash': hash_value})
        return True if exists else False

    def keep_a_record(self, record):
        """Insert a record into the current collection."""
        return self.collection.insert_one(record).inserted_id
