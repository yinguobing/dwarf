"""The steward watches the file list and dispatch the tasks."""
import os
import sys

import pika
import yaml
import ffmpeg
from PIL import Image


def get_video_tags(video_path):
    """Check the video codec and return it."""
    return ffmpeg.probe(video_path)


def get_image_tags(image_file):
    """Return the basic tags for image file."""
    with Image.open(image_file) as f:
        return {"format": f.format,
                "width:": f.width,
                "height": f.height}


def get_file_type(file_path):
    """Get the file type by it's suffix."""
    return os.path.splitext(file_path)[-1].split('.')[-1]
    

if __name__ == '__main__':
    # Load the configuration file.
    cfg_file = sys.argv[1] if len(sys.argv) > 1 else 'config.yml'
    with open("config.yml", 'r') as f:
        cfg = yaml.load(f, Loader=yaml.FullLoader)

    try:
        # Setup the post office.
        post_office = pika.BlockingConnection(
            pika.ConnectionParameters(cfg['rabbitmq']['address']))
        channel = post_office.channel()

        queue_name = cfg['rabbitmq']['queue']
        channel.queue_declare(queue=queue_name, durable=True)

        # Define the actions for message.
        def callback(ch, method, properties, body):
            src_file = body.decode()

            # Probe the file by type.
            suffix = get_file_type(src_file)

            if suffix in cfg['video_types']:
                raw_tag = get_video_tags(src_file)
            else:
                raw_tag = get_image_tags(src_file)
            
            print(raw_tag)

            ch.basic_ack(delivery_tag = method.delivery_tag)

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=queue_name,
                              on_message_callback=callback,
                              auto_ack=False)

        print(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()

    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
