"""The steward watches the file list and dispatch the tasks."""
import os
import sys
import time
from hashlib import md5 as hash_func

import ffmpeg
import pika
import yaml
from PIL import Image

# Load the configuration file.
CFG_FILE = sys.argv[1] if len(sys.argv) > 1 else 'config.yml'
with open(CFG_FILE, 'r') as f:
    CFG = yaml.load(f, Loader=yaml.FullLoader)

def get_checksum(file_path):
    """Get the hash value of the input file."""
    with open(file_path, 'rb') as f:
        return hash_func(f.read()).hexdigest()


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


def get_tags(src_file, parse_func, max_num_try, timeout):
    """Get the tags from the source file."""
    num_try = 0
    seconds_wait = 0
    process_succeed = False
    raw_tags = {}

    while True:

        if num_try >= max_num_try:
            print("Can not open file. Tried 3 times.")
            print(src_file)
            break

        if seconds_wait >= timeout:
            print("Can not open file. Timeout for 30 seconds.")
            print(src_file)
            break

        if not os.path.exists(src_file):
            seconds_wait += 1
            time.sleep(1)
            continue

        try:
            num_try += 1
            raw_tags = parse_func(src_file)
            process_succeed = True
            break
        except:
            print("Failed to open file.")
            print(src_file)
            continue

    return process_succeed, raw_tags


def callback(ch, method, properties, body):
    # Get the full file path.
    src_file = body.decode()
    print('_' * 65)
    print("File created: {}".format(src_file))

    # The files may comes from any source. Check them before going.
    supported_types = CFG['video_types'] + CFG['image_types']

    # Get the parse function by file type.
    suffix = get_file_type(src_file)

    if suffix not in supported_types:
        log_unknown_file(src_file)
        succeed = False
    else:
        if suffix in CFG['video_types']:
            parse_func = get_video_tags
        elif suffix in CFG['image_types']:
            parse_func = get_image_tags

        succeed, tags = get_tags(src_file,
                                 parse_func,
                                 CFG['monitor']['max_num_try'],
                                 CFG['monitor']['timeout'])
    if succeed:
        print(tags)

    ch.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == '__main__':

    try:
        # Setup the post office.
        post_office = pika.BlockingConnection(
            pika.ConnectionParameters(CFG['rabbitmq']['address']))
        channel = post_office.channel()

        queue_name = CFG['rabbitmq']['queue']
        channel.queue_declare(queue=queue_name, durable=True)
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
