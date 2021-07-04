"""The steward watches the file list and dispatch the tasks."""

import datetime
import logging
import logging.config
import os
import sys
import time

import ffmpeg
import yaml
from PIL import Image

from rabbit import Rabbit

# Setup the logger.
logging.config.dictConfig(yaml.load(open("logging.yml", 'r'), yaml.FullLoader))
logger = logging.getLogger('steward')

# Load the configuration file.
CFG_FILE = sys.argv[1] if len(sys.argv) > 1 else 'config.yml'
with open(CFG_FILE, 'r') as f:
    CFG = yaml.load(f, Loader=yaml.FullLoader)


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


class Steward:

    def __init__(self, stocker, clark):
        """Steward is in charge of the data processing project.

        Args:
            clark: a clark to manage the books.
            stocker: a stocker to fill the warehouse.
        """
        self.stocker = stocker
        self.clark = clark

    def precheck(self, file_path):
        """Check the src file and return the parse function and the collection name.

        The files may comes from any source. But only the video and image files
        are of concerns. This can be configured in the config.yml file.

        Args:
            file_path: the full path of the file to be checked.

        Returns:
            succeed: true if the file is valid.
            parse_func: the parse function to be used.
            collection_name: the collection name this file's record should be saved.
        """
        supported_types = CFG['video_types'] + CFG['image_types']
        suffix = get_file_type(file_path)

        if suffix not in supported_types:
            logger.warning("Unknown file type: {}".format(file_path))
            return False, None, None
        else:
            if suffix in CFG['video_types']:
                parse_func = get_video_tags
                collection_name = CFG["mongodb"]["collections"]["videos"]
            elif suffix in CFG['image_types']:
                parse_func = get_image_tags
                collection_name = CFG["mongodb"]["collections"]["images"]

        return True, parse_func, collection_name

    def get_tags(self, src_file, parse_func, max_num_try, timeout):
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
                logger.warning("Can not open file. Tried 3 times.")
                break

            if seconds_wait >= timeout:
                logger.warning("Can not open file. Timeout for 30 seconds.")
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
            except FileNotFoundError:
                logger.error("FFMPEG not installed correctly.")
            except:
                logger.warning("Failed to open file. Try again...")
                # Wait for a moment so that the file could be fully created.
                time.sleep(3)
                continue

        return process_succeed, raw_tags

    def get_manual_tags(self, src_file):
        """Get the manual tags from a tag file."""
        common_prefix = CFG['dirs']['barn'].rstrip(os.path.sep)
        job_dir_name = src_file[len(common_prefix):].split(os.path.sep)[1]
        tag_file = os.path.join(common_prefix, job_dir_name, 'tags.txt')
        if os.path.exists(tag_file):
            with open(tag_file, 'r') as f:
                tags = f.readline().split(' ')
                succeed = True
        else:
            tags = None
            succeed = False

        return succeed, tags

    def process(self, src_file):
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
        succeed, parse_func, collection_name = self.precheck(src_file)
        if not succeed:
            return failure

        # Make sure this file was not processed before.
        hash_value = self.stocker.get_checksum(src_file)
        already_existed = self.clark.check_existence(
            hash_value, collection_name)
        if already_existed:
            logger.warning("Duplicated file detected.")
            return failure

        # Get the tags of the file.
        succeed, raw_tags = self.get_tags(src_file,
                                          parse_func,
                                          CFG['monitor']['max_num_try'],
                                          CFG['monitor']['timeout'])
        if not succeed:
            logger.warning("Failed to get file tags.")
            return failure

        # Stock the file in the warehouse if any tag got.
        succeed, dst_file = self.stocker.stock(src_file)
        if not succeed:
            logger.warning("Failed to move the file.")
            return failure

        # Try to get the manual tags.
        succeed, manual_tags = self.get_manual_tags(src_file)
        if not succeed:
            logger.warning("Manual tag file not found. This file will not be processed.")
            return failure

        # Create a database record and save it.
        record = {"base_name": os.path.basename(src_file),
                  "path": dst_file,
                  "hash": hash_value,
                  "file_size": os.stat(dst_file).st_size,
                  "index_time": datetime.datetime.utcnow(),
                  "raw_tag": raw_tags,
                  "manual_tags": manual_tags}
        self.clark.set_collection(collection_name)
        try:
            record_id = self.clark.keep_a_record(record)
        except:
            logger.warning("Failed to save in database.")
            self.stocker.destry(dst_file)
            return failure

        # Finally, clean the original file.
        self.stocker.destry(src_file)

        return True, record_id

    def callback(self, ch, method, properties, body):
        """This is the function that was called when a message is received."""
        # Get the full file path.
        src_file = body.decode()
        logger.info("File created: {}".format(src_file))

        # Try to process the source file.
        succeed, record_id = self.process(src_file)

        if succeed:
            logger.info("File saved and logged in with ID: {}".format(record_id))
        else:
            logger.warning("No data saved.")

        # Tell the rabbit the result.
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start_processing(self):
        """Start to process new files in the barn"""
        # Summon a rabbit to deliver the mesages.
        self.runner = Rabbit(address=CFG['rabbitmq']['address'],
                             queue=CFG['rabbitmq']['queue'],
                             talking=False,
                             callback=self.callback)

        # Start listening..
        logger.info('[*] Waiting for messages...')
        self.runner.start_listening()
