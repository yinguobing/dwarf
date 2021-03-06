"""The watchdog watches the barn for any file changes."""

import logging
import logging.config
import os
import sys

import yaml
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from rabbit import Rabbit

# Setup the logger.
logging.config.dictConfig(yaml.load(open("logging.yml", 'r'), yaml.FullLoader))
logger = logging.getLogger('porter')

# Load the configuration file.
CFG_FILE = sys.argv[1] if len(sys.argv) > 1 else 'config.yml'
with open(CFG_FILE, 'r') as f:
    CFG = yaml.load(f, Loader=yaml.FullLoader)


class FolderEventHandler(FileSystemEventHandler):

    def __init__(self, messenger):
        super().__init__()

        # Summon a rabbit.
        self.messenger = messenger

    def on_created(self, event):
        logger.debug("{}:{}".format(event.event_type, event.src_path))

    def on_modified(self, event):
        logger.debug("{}:{}".format(event.event_type, event.src_path))

    def on_deleted(self, event):
        logger.debug("{}:{}".format(event.event_type, event.src_path))

    def on_closed(self, event):
        logger.debug("{}:{}".format(event.event_type, event.src_path))
        self.send_message(event.src_path)

    def send_message(self, src_path):
        if not os.path.isdir(src_path):
            self.messenger.speak(src_path)


class Porter:

    def __init__(self, target):
        """A porter will watch any file changes in the target directory.

        Args:
            target: the directory to be watched.
        """
        # Where is the barn to watch? Make sure the folder already existed.
        assert os.path.exists(target), "Target folder not found, please check."

        self._rabbit = Rabbit(address=CFG['rabbitmq']['host'],
                              port=CFG['rabbitmq']['port'],
                              queue=CFG['rabbitmq']['queue'],
                              talking=True)

        # Setup the file observer.
        self.observer = Observer()
        self.event_handler = FolderEventHandler(self._rabbit)
        self.observer.schedule(self.event_handler, target, recursive=True)

    def start_watching(self):
        """Staring to watch the changes."""
        self.observer.start()
        logger.info('[*] Monitoring...')

    def stop(self):
        """Let it go."""
        self.observer.stop()
        self.observer.join()
