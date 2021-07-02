"""The steward watches the file list and dispatch the tasks."""
import os
import sys
import time

import ffmpeg
import pika
import yaml
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

def log_unknown_file(somefile):
    """Log the not supported file."""
    print("Unknown file type: {}".format(somefile))
    return {}


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
            # Get the full file path.
            src_file = body.decode()
            print("File created: {}".format(src_file))

            # Get the parse function by file type.
            suffix = get_file_type(src_file)

            if suffix in cfg['video_types']:
                parse_func = get_video_tags
            elif suffix in cfg['image_types']:
                parse_func = get_image_tags
            else:
                parse_func = log_unknown_file
            
            # Parse the file.
            num_try = 0
            seconds_wait = 0
            process_succeed = False

            while True:

                if num_try >= 3:
                    print("Can not open file. Tried 3 times.")
                    break

                if seconds_wait >= 30:
                    print("Can not open file. Timeout for 30 seconds.")
                    break

                if not os.path.exists(src_file):
                    seconds_wait += 1
                    time.sleep(1)
                    continue

                try:
                    num_try += 1
                    raw_tags = parse_func(src_file)
                    process_succeed = True
                except:
                    print("Can not open file. Try for {} time(s)".format(num_try))
                    continue
            
            if process_succeed:
                print(raw_tags)

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
