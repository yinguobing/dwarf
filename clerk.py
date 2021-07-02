from pymongo import MongoClient


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
                                  password=password)
        self.db = self.client.get_database(name)

    def set_collection(self, name):
        """Get the collection by name"""
        self.col = self.db.get_collection(name)

    def check_existence(self, hash_value):
        """Check if the hash value existed in the current collection."""
        existence = self.col.find_one({'hash': hash_value})
        return True if existence else False
