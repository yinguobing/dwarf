"""The watchdog watches the barn for any file changes."""

import logging
import os
import sys

import yaml
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer


class FolderEventHandler(FileSystemEventHandler):

    def on_created(self, event):
        print(event.event_type, event.src_path)
        self.send_message(event.src_path)

    def on_modified(self, event):
        print(event.event_type, event.src_path)

    def on_deleted(self, event):
        print(event.event_type, event.src_path)

    def send_message(self, src_path):
        file_type = 'Directory' if os.path.isdir(src_path) else 'File'
        print("{} created: {}".format(file_type, src_path))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    # Load the configuration file.
    cfg_file = sys.argv[1] if len(sys.argv) > 1 else 'config.yml'
    with open("config.yml", 'r') as f:
        cfg = yaml.load(f, Loader=yaml.FullLoader)

    # Where is the barn to watch?
    barn = cfg['porter']['barn']

    observer = Observer()
    event_handler = FolderEventHandler()
    observer.schedule(event_handler, barn, recursive=True)
    observer.start()

    try:
        while observer.is_alive():
            observer.join(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()
