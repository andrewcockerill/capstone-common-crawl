# Packages
import os
import argparse
from utils import DownloadManager
import re
import logging

# Constants
OUTPUT_DIR = "../data"
JSON_LOC = os.path.join(OUTPUT_DIR, "warc_urls.json")
JSON_META_LOC = os.path.join(OUTPUT_DIR, "warc_urls_meta.json")

# Logging
logger = logging.getLogger('download_warcs')
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh = logging.FileHandler('../logs/download_warcs.log')
fh.setFormatter(formatter)
logger.addHandler(fh)

# Args
parser = argparse.ArgumentParser()
parser.add_argument("-n", "--numfiles")
parser.add_argument("-s", "--seed")
args = parser.parse_args()

# Downloads
if not args.numfiles:
    numfiles = 1
else:
    numfiles = int(args.numfiles)

if not args.seed:
    seed = 1
else:
    seed = int(args.seed)

dm = DownloadManager(logger=logger)
dm.download_sample(output_dir=OUTPUT_DIR, url_json_loc=JSON_LOC,
                   url_json_metadata_loc=JSON_META_LOC, num_files=numfiles, seed=seed)
