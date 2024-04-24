# Packages
import os
import argparse
from utils import DownloadManager
import re
import logging

# Constants
OUTPUT_DIR = "../data"

# Logging
logger = logging.getLogger('build_catalog')
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh = logging.FileHandler('../logs/build_catalog.log')
fh.setFormatter(formatter)
logger.addHandler(fh)

# Args
parser = argparse.ArgumentParser()
parser.add_argument("-s", "--start")
parser.add_argument("-e", "--end")
args = parser.parse_args()

# Build catalog
dm = DownloadManager(logger=logger)
year_start = int(args.start)
year_end = int(args.end)
dm.get_warc_urls(output_dir=OUTPUT_DIR,
                 year_start=year_start, year_end=year_end)
