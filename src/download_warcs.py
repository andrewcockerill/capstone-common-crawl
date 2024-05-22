# Packages
from utils import DownloadManager
import logging
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# Constants
JSON_PATH = "../data/json"
WARC_PATH = "../data/warc"
FILES_PER_CRAWL = 1
MAX_FILES_TOTAL = 10
N_WORKERS = 2

# Logging
logger = logging.getLogger('download_warcs')
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh = logging.FileHandler('../logs/download_warcs.log')
fh.setFormatter(formatter)
logger.addHandler(fh)

# Spark Setup
sc = SparkContext()
spark = SparkSession(sc)

logger.info('app started')

dm = DownloadManager(json_path=JSON_PATH, warc_path=WARC_PATH, 
                     max_files_per_crawl=FILES_PER_CRAWL, max_files_total=MAX_FILES_TOTAL, logger=logger)
dm.get_warc_urls()

file_list = dm.get_download_queue()

if len(file_list) > 0:
    file_list_rdd = sc.parallelize(file_list, N_WORKERS)
    dm.download_warc_list(file_list_rdd)
    dm.cleanup_queue()

logger.info('app ended')

sc.stop()
spark.stop()