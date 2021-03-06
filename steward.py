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
        suffix = get_file_type(file_path).lower()

        if suffix not in supported_types:
            logger.debug("{}: Unknown file type.".format(file_path))
            return False, None, None
        else:
            if suffix in CFG['video_types']:
                parse_func = get_video_tags
                collection_name = CFG["mongodb"]["collections"]["videos"]
            elif suffix in CFG['image_types']:
                parse_func = get_image_tags
                collection_name = CFG["mongodb"]["collections"]["images"]

        return True, parse_func, collection_name

    def get_raw_tags(self, src_file, parse_func, max_num_try, timeout):
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
                logger.debug("Can not open file. Tried 3 times.")
                break

            if seconds_wait >= timeout:
                logger.debug("Open file timeout: {} seconds.".format(timeout))
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
                logger.debug("Failed to open file. Try again...")
                # Wait for a moment so that the file could be fully created.
                time.sleep(3)
                continue

        return process_succeed, raw_tags

    def get_tag_files(self, src_file):
        """Return the tag files of the current file.

        There are two kind of tag files. The first kind are those stored in the 
        root directory of the new files and are mandatory. The other kind are 
        tag files stored in the same directory of the src file, and are optional.
        If these two kind of files both exist, the first kind are ignored.
        """
        failure = False, None, None

        # First get the optional tag file and author file.
        current_dir, _ = os.path.split(src_file)
        opt_tag_file = os.path.join(current_dir, 'tags.txt')
        opt_author_file = os.path.join(current_dir, 'authors.txt')

        # Then get the root tag file and author file.
        barn = CFG['dirs']['barn'].rstrip(os.path.sep)
        root_dir = src_file[len(barn):].split(os.path.sep)[1]
        root_tag_file = os.path.join(barn, root_dir, 'tags.txt')
        root_author_file = os.path.join(barn, root_dir, 'authors.txt')

        # Only return the desired files.
        tag_file = opt_tag_file if os.path.exists(
            opt_tag_file) else root_tag_file
        author_file = opt_author_file if os.path.exists(
            opt_author_file) else root_author_file

        if not os.path.exists(tag_file):
            logger.debug("Tag file not found.")
            return failure

        if not os.path.exists(author_file):
            logger.debug("Author file not found.")
            return failure

        return True, tag_file, author_file

    def get_manual_tags(self, src_file):
        """Get the manual tags from the tag file and author file."""
        file_got, tag_file, author_file = self.get_tag_files(src_file)

        if file_got:
            with open(tag_file, 'r') as f:
                tags = f.readline().split(' ')
            with open(author_file, 'r') as f:
                authors = f.readline().split(' ')
            succeed = True
        else:
            tags = None
            authors = None
            succeed = False

        return succeed, tags, authors

    def is_secret_mission(self, src_file):
        """A secret mission."""
        _, tail = os.path.split(src_file)
        return True if tail == 'dwarf.run' else False

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

        # In case there is a secret mission.
        if self.is_secret_mission(src_file):
            self.stocker.destry(src_file)
            self.stocker.check_inventory()
            logger.info("=???= Secret Mission =???=")
            return failure

        # The file may be of any format. Precheck it to get the correct parse
        # function and the DB collection name.
        succeed, parse_func, collection_name = self.precheck(src_file)
        if not succeed:
            logger.warning("    File format not supported.")
            return failure

        # Make sure this file was not processed before.
        succeed, hash_value = self.stocker.get_checksum(src_file)
        if not succeed:
            logger.warning("    Falied to get the hash checksum.")
            return failure
        already_existed = self.clark.check_existence(
            hash_value, collection_name)
        if already_existed:
            logger.warning("    Duplicated file detected.")
            return failure

        # Get the tags of the file.
        succeed, raw_tags = self.get_raw_tags(src_file,
                                              parse_func,
                                              CFG['monitor']['max_num_try'],
                                              CFG['monitor']['timeout'])
        if not succeed:
            logger.warning("    Failed to get file format tags.")
            return failure

        # Stock the file in the warehouse if any tag got.
        succeed, dst_file = self.stocker.stock(src_file)
        if not succeed:
            logger.warning("    Failed to move the file.")
            return failure

        # Try to get the manual tags and authors. This is mandatory.
        succeed, manual_tags, authors = self.get_manual_tags(src_file)
        if not succeed:
            logger.warning("    Failed to get manual tags and authors.")
            return failure

        # Create a database record and save it.
        record = {"base_name": os.path.basename(src_file),
                  "path": dst_file,
                  "hash": hash_value,
                  "file_size": os.stat(dst_file).st_size,
                  "index_time": datetime.datetime.utcnow(),
                  "raw_tag": raw_tags,
                  "manual_tags": manual_tags,
                  "authors": authors}

        self.clark.set_collection(collection_name)
        try:
            record_id = self.clark.keep_a_record(record)
        except:
            logger.warning("    Failed to save in database.")
            self.stocker.destry(dst_file)
            return failure

        # Finally, clean the original file.
        if not self.stocker.destry(src_file):
            logger.warning(
                "    Failed to remove the source file. You can remove it manually.")

        return True, record_id

    def callback(self, ch, method, properties, body):
        """This is the function that was called when a message is received."""
        # Get the full file path.
        src_file = body.decode()
        logger.info(" *  File created: {}".format(src_file))

        # Try to process the source file.
        succeed, record_id = self.process(src_file)

        if succeed:
            logger.info(" ???  File logged with ID: {}".format(record_id))
        else:
            logger.warning(
                " ???  File not processed.")

        # Tell the rabbit the result.
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start_processing(self):
        """Start to process new files in the barn"""
        # Summon a rabbit to deliver the mesages.
        self._rabbit = Rabbit(address=CFG['rabbitmq']['host'],
                              port=CFG['rabbitmq']['port'],
                              queue=CFG['rabbitmq']['queue'],
                              talking=False,
                              callback=self.callback)

        # Start listening..
        logger.info('[*] Waiting for messages...')
        self._rabbit.start_listening()
