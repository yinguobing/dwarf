"""The watchdog watches the barn for any file changes."""

import logging
import os
import sys

import yaml
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from rabbit import Rabbit

# Load the configuration file.
CFG_FILE = sys.argv[1] if len(sys.argv) > 1 else 'config.yml'
with open(CFG_FILE, 'r') as f:
    CFG = yaml.load(f, Loader=yaml.FullLoader)


class FolderEventHandler(FileSystemEventHandler):

    def __init__(self, mq_address, queue):
        super().__init__()

        # Summon a rabbit.
        self.PETER = Rabbit(address=mq_address, queue=queue, talking=True)

    def on_created(self, event):
        print(event.event_type, event.src_path)
        self.send_message(event.src_path)

    def on_modified(self, event):
        print(event.event_type, event.src_path)

    def on_deleted(self, event):
        print(event.event_type, event.src_path)

    def send_message(self, src_path):
        if not os.path.isdir(src_path):
            self.PETER.speak(src_path)


if __name__ == "__main__":
    # Where is the barn to watch? Make sure the folder already existed.
    barn = CFG['dirs']['barn']
    assert os.path.exists(barn), "Target folder not found, please check."

    # Setup the file observer.
    observer = Observer()
    event_handler = FolderEventHandler(mq_address=CFG['rabbitmq']['address'],
                                       queue=CFG['rabbitmq']['queue'])
    observer.schedule(event_handler, barn, recursive=True)
    observer.start()

    print(' [*] Monitoring... To exit press CTRL+C')

    # Let it go.
    try:
        while observer.is_alive():
            observer.join(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()
