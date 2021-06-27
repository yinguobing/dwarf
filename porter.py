"""The watchdog watches the barn for any file changes."""

import logging
import os
import sys

import pika
import yaml
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer


class FolderEventHandler(FileSystemEventHandler):

    def __init__(self, mq_address, queue):
        super().__init__()

        # Setup the post office.
        self.messenger = pika.BlockingConnection(
            pika.ConnectionParameters(mq_address))
        self.channel = self.messenger.channel()
        self.channel.queue_declare(queue=queue)
        self.queue = queue

    def on_created(self, event):
        print(event.event_type, event.src_path)
        self.send_message(event.src_path)

    def on_modified(self, event):
        print(event.event_type, event.src_path)

    def on_deleted(self, event):
        print(event.event_type, event.src_path)

    def send_message(self, src_path):
        if not os.path.isdir(src_path):
            self.channel.basic_publish(exchange='', routing_key=self.queue,
                                       body=src_path)
            
if __name__ == "__main__":
    # Load the configuration file.
    cfg_file = sys.argv[1] if len(sys.argv) > 1 else 'config.yml'
    with open("config.yml", 'r') as f:
        cfg = yaml.load(f, Loader=yaml.FullLoader)

    # Where is the barn to watch? Make sure the folder already existed.
    barn = cfg['porter']['barn']
    assert os.path.exists(barn), "Target folder not found, please check."

    # Setup the file observer.
    observer = Observer()
    event_handler = FolderEventHandler(mq_address=cfg['rabbitmq']['address'],
                                       queue=cfg['rabbitmq']['queue'])
    observer.schedule(event_handler, barn, recursive=True)
    observer.start()

    # Let it go.
    try:
        while observer.is_alive():
            observer.join(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()
