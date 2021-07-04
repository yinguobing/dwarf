"""The steward watches the file list and dispatch the tasks."""
import datetime
import os
import sys
import time
from hashlib import md5 as hash_func

import ffmpeg
import pika
import yaml
from PIL import Image

from clerk import Clerk
from stocker import Stocker

# Load the configuration file.
CFG_FILE = sys.argv[1] if len(sys.argv) > 1 else 'config.yml'
with open(CFG_FILE, 'r') as f:
    CFG = yaml.load(f, Loader=yaml.FullLoader)

# Employ a clerk to manage the books.
JULIE = Clerk(CFG['mongodb']["host"],
              CFG['mongodb']['port'],
              CFG['mongodb']['username'],
              CFG['mongodb']['password'],
              CFG["mongodb"]['name'])

# Employ a stocker to fill the warehouse.
TOM = Stocker(CFG['dirs']['barn'],
              CFG['dirs']['warehouse'])


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


def precheck(file_path):
    """Check the src file and return the parse function and the collection name.

    The files may comes from any source. But only the video and image files
    are of concerns. This can be configured in the config.yml file.

    Args:
        file_path: the full path of the file to be checked.

    Returns:
        parse_func: the parse function to be used.
        collection_name: the collection name this file's record should be saved.
    """
    supported_types = CFG['video_types'] + CFG['image_types']
    suffix = get_file_type(file_path)

    if suffix not in supported_types:
        print("Unknown file type: {}".format(file_path))
    else:
        if suffix in CFG['video_types']:
            parse_func = get_video_tags
            collection_name = CFG["mongodb"]["collections"]["videos"]
        elif suffix in CFG['image_types']:
            parse_func = get_image_tags
            collection_name = CFG["mongodb"]["collections"]["images"]

    return parse_func, collection_name


def get_tags(src_file, parse_func, max_num_try, timeout):
    """Get the tags from the source file.

    Args:
        src_file: the file's full path.
        parse_func: which function to use when parsing the file.
        max_num_try: max times trying if the parsing fails.
        timeout: how long to wait for a valid parsing.

    returns:
        process_succeed: a boolean value indicating the process status
        raw_tags: the parsed results.
    """
    num_try = 0
    seconds_wait = 0
    process_succeed = False
    raw_tags = {}

    while True:

        if num_try >= max_num_try:
            print("Can not open file. Tried 3 times.")
            break

        if seconds_wait >= timeout:
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
            break
        except:
            print("Failed to open file. Try again...")
            # Wait for a moment so that the file could be fully created.
            time.sleep(3)
            continue

    return process_succeed, raw_tags


def process(src_file):
    """Process the sample file.

    Tasks: 
        - Check if the file is valid and of interest.
        - Get the raw tags of the file and stock it the warehouse.
        - Create a valid record to be stored in the database.

    Args:
        src_file: the file to be processed.

    Returns:
        succeed: a flag indicating the process accomplished successfully.
        record_id: the database record id.
    """
    # Mark the initial state to False to save a lot lines of code.
    failure = False, None

    # The file may be of any format. Precheck it to get the correct parse
    # function and the DB collection name.
    parse_func, collection_name = precheck(src_file)

    # Make sure this file was not processed before.
    hash_value = TOM.get_checksum(src_file)
    already_existed = JULIE.check_existence(hash_value, collection_name)
    if already_existed:
        print("Duplicated file detected.")
        return failure

    # Get the tags of the file.
    succeed_tag, tags = get_tags(src_file,
                                 parse_func,
                                 CFG['monitor']['max_num_try'],
                                 CFG['monitor']['timeout'])
    if not succeed_tag:
        print("Failed to get file tags.")
        return failure

    # Stock the file in the warehouse if any tag got.
    succeed_file, dst_file = TOM.stock(src_file)
    if not succeed_file:
        print("Failed to move the file.")
        return failure

    # Create a database record and save it.
    record = {"base_name": os.path.basename(src_file),
              "path": dst_file,
              "hash": hash_value,
              "file_size": os.stat(dst_file).st_size,
              "index_time": datetime.datetime.utcnow(),
              "raw_tag": tags}
    JULIE.set_collection(collection_name)
    try:
        record_id = JULIE.keep_a_record(record)
    except:
        print("Failed to save in database.")
        TOM.destry(dst_file)
        return failure
    
    # Finally, clean the original file.
    TOM.destry(src_file)

    return True, record_id


def callback(ch, method, properties, body):
    """This is the function that was called when a message is received."""
    # Get the full file path.
    src_file = body.decode()

    print('_' * 65)
    print("File created: {}".format(src_file))

    # Try to process the source file.
    succeed, record_id = process(src_file)

    if succeed:
        print("File saved and logged in with ID: {}".format(record_id))
    else:
        print("No data saved.")

    # Tell the rabbit the result.
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
