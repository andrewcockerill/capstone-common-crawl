# Packages
import os
from utils import ArchiveProcessor
import re
import logging
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# Constants
PARQUET_PATH = '../data/parquet'
WARC_PATH = "../data/warc"

# Logging
logger = logging.getLogger('transform_load')
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh = logging.FileHandler('../logs/transform_load.log')
fh.setFormatter(formatter)
logger.addHandler(fh)

# Spark Setup
sc = SparkContext()
spark = SparkSession(sc)
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Prep and write tables
logger.info('app started')

ap = ArchiveProcessor(warc_path=WARC_PATH, parquet_path=PARQUET_PATH, logger=logger)

logger.info('started extraction of warcs')
warcs = sc.parallelize(ap.get_warc_filenames())
warc_rdd = warcs.flatMap(lambda x: ap.read_warc(x))
logger.info('finished extraction of warcs')

ap.create_or_update_tables(warc_rdd)

logger.info('app ended')

sc.stop()
spark.stop()