import os
import requests
from bs4 import BeautifulSoup
import logging
import re
import datetime
import gzip
import json
import numpy as np
from collections import Counter
from fastwarc.warc import *
from fastwarc.stream_io import GZipStream
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import types as T
import pyspark.sql.functions as F
from pyspark.sql.window import Window


class DownloadManager:
    """Class to house multiple methods for obtaining WARC archive URLs and downloading of actual files.

    Attributes
    ----------

    json_path : str
        Location of the directory to store .json outputs

    warc_path : str
        Location of the directory to store zipped WARC files (raw data)

    max_files_per_crawl : int
        Maximum number of archives that can be downloaded from a single crawl

    max_files_total : int
        Maximum number of archives in total that can be downloaded from all crawls

    logger : Logger
        Logger object for tracking

    Methods
    -------

    get_warc_urls(output_dir, year_start, year_end)
        Creates a URL catalog of Common Crawl files in JSON format in `output_dir` along with a metadata
        JSON file that includes the same keys and counts of URLs per key. Each key is a Common Crawl index
        as described at https://commoncrawl.org/overview. Data will be fetched for years from `year_start`
        to `year_end`.

    get_download_queue()
        Opens `warcs_to_download.json` at specified location and returns a list of new WARC URLs that were
        identified by get_warc_urls that didn't already exist in the catalog.

    download_warc_list(url_rdd)
        Given a parallelized version of the results in get_download_queue, instructs executors to
        download the file list to the specified `warc_path`

    cleanup_queue()
        Empties the download queue in `warcs_to_download.json`

    """

    def __init__(self, json_path, warc_path, max_files_per_crawl=1, max_files_total=10, logger=None):
        self.logger = logger
        self.json_link_file = os.path.join(json_path, 'warc_urls.json')
        self.json_metadata_file = os.path.join(json_path, 'warc_urls_meta.json')
        self.json_to_download = os.path.join(json_path, 'warcs_to_download.json')
        self.warc_path = warc_path
        self._overview_url = r"https://commoncrawl.org/overview"
        self._data_url = r"https://data.commoncrawl.org/"
        self._data_path = r"crawl-data/"
        self._warc_ext = r"warc.paths.gz"
        self._max_files_per_crawl = max_files_per_crawl
        self._max_files_total = max_files_total

    @property
    def overview_url(self):
        return self._overview_url

    @property
    def data_url(self):
        return self._data_url

    @property
    def data_path(self):
        return self._data_path

    @property
    def warc_ext(self):
        return self._warc_ext
    
    @property
    def max_files_per_crawl(self):
        return self._max_files_per_crawl
    
    @property
    def max_files_total(self):
        return self._max_files_total
    
    def _check_json(self):
        # Check to see if JSON URL and Metadata files exist; if not, create them
        for filename in [self.json_link_file, self.json_metadata_file]:
            if not os.path.exists(filename):
                with open(filename, 'w') as f:
                    json.dump({}, f)

    def _fetch_path_zips(self, year_start=None, year_end=None):
        # Establish years to collect data
        if year_start is None:
            year_start = int(datetime.datetime.now().year)
        else:
            year_start = int(year_start)
        if year_end is None:
            year_end = int(datetime.datetime.now().year)
        else:
            year_end = int(year_end)
        assert year_start <= year_end, 'end year must be > start year'
        year_regex = '|'.join([str(y) for y in range(year_start, year_end+1)])

        # Request the names of all Common Crawl index names
        html_content = requests.get(self.overview_url).content
        self.soup = BeautifulSoup(html_content, features='html.parser')
        cc_indices = [str(s.contents[0]) for s in self.soup.find_all(
            'h6') if bool(re.search(year_regex, str(s.contents[0])))]
        
        # Check if crawls have been found and logged previously, then update
        self._check_json()
        with open(self.json_metadata_file) as f:
            known_crawls = [i for i in json.load(f).keys()]
        cc_indices = [i for i in cc_indices if i not in known_crawls]

        gz_urls = [
            f"{self.data_url}{self.data_path}{ind}/{self.warc_ext}" for ind in cc_indices]
        return list(zip(cc_indices, gz_urls))

    def _decompress_path_zip(self, url):
        try:
            # Request a Common Crawl index zip file URL and unpack to a list
            zip_file = requests.get(url).content
            return [f"{self.data_url}{i}" for i in gzip.decompress(zip_file).strip().decode('utf-8').split()]
        except Exception as e:
            if self.logger:
                self.logger.error(f'unexpected error in zip decompression for {url}')
                self.logger.error(e)

    def get_warc_urls(self, year_start=None, year_end=None):
        # Construct URL catalogs and catalog metadata
        try:
            if self.logger:
                self.logger.info('started fetch of indices and path zips')
            path_zips = self._fetch_path_zips(
                year_start=year_start, year_end=year_end)
            new_links = {i[0]: self._decompress_path_zip(i[1])
                           for i in path_zips}
            new_metadata = {k: len(v) for k, v in new_links.items()}

            if len(new_metadata) > 0:
                # Update link file
                with open(self.json_link_file, 'r') as f:
                    current_links = json.load(f)
                current_links.update(new_links)
                with open(self.json_link_file, 'w') as f:
                    json.dump(current_links, f)

                # Update metadata
                with open(self.json_metadata_file, 'r') as f:
                    current_metadata = json.load(f)
                current_metadata.update(new_metadata)
                with open(self.json_metadata_file, 'w') as f:
                    json.dump(current_metadata, f)

                # Update files planned for download
                with open(self.json_to_download, 'w') as f:
                    to_download = {i:j[0:self.max_files_per_crawl] for i,j in new_links.items()}
                    json.dump(to_download, f)
                
            if self.logger:
                self.logger.info('finished fetch of indices and path zips')
        except Exception as e:
            if self.logger:
                self.logger('unexpected error when downloading warc urls')
                self.logger.error(e)

    def get_download_queue(self):
        with open(self.json_to_download, 'r') as f:
            to_download = json.load(f)
            file_list = [i for j in to_download.values() for i in j][0:self.max_files_total]
        return file_list

    def _download_warc(self, url):
        # Download WARC from a URL
        outfile = os.path.join(self.warc_path, re.sub(".*/", "", url))
        r = requests.get(url, stream=True)
        block_size = 1024
        with open(outfile, "wb") as file:
            for data in r.iter_content(block_size):
                #progress_bar.update(len(data))
                file.write(data)

    def download_warc_list(self, url_rdd):
        # Download WARCs from a list of URLs
        if self.logger:
            self.logger.info(f"started download of URL list")
        try:
            results = url_rdd.foreach(lambda x: self._download_warc(x)).collect()
        except Exception as e:
            if self.logger:
                self.logger.error('unexpected error when downloading warc files')
                self.logger.error(e)
        if self.logger:
            self.logger.info(f"finished download of URL list")

    def cleanup_queue(self):
        # Empty the queue of files to download
        if self.logger:
            self.logger.info(f"started cleanup of queue")
        with open(self.json_to_download, 'w') as f:
            json.dump({}, f)
        if self.logger:
            self.logger.info(f"finished cleanup of queue")

class ArchiveProcessor:
    """Class handle the conversion of zipped WARC files to RDDs/Spark Dataframes, transformations,
    and saving to parquet format.

    Attributes
    ----------

    warc_path : str
        Location of the directory to store zipped WARC files (raw data)

    parquet_path : str
        Location of the directory to store tables in parquet format

    logger : Logger
        Logger object for tracking

    Methods
    -------

    read_warc(warc)
        Iterator that reads a line from the zipped archive `warc` and yields an extracted WARC record as a tuple.
        Meant to be mapped to an RDD of WARC filenames.

    create_or_update_tables(rdd):
        Following conversion of WARCs using the iterator above to an RDD, converts the output to a Spark
        DataFrame, performs data cleansing and preparation, and writes output tables in parquet format.

    """

    def __init__(self, warc_path, parquet_path, logger=None):
        self.warc_path = warc_path
        self.parquet_path = parquet_path
        self._warc_header_ref = {
            'WARC-Date': ['warc_date', 'timestamp'],
            'WARC-Record-ID': ['warc_record_id', 'string'],
            'Content-Length': ['warc_content_length', 'int'],
            'WARC-Warcinfo-ID': ['warc_warcinfo_id' , 'string'],
            'WARC-IP-Address': ['warc_ip_address', 'string'],
            'WARC-Target-URI': ['warc_target_uri' ,'string'],
            'WARC-Payload-Digest': ['warc_payload_digest', 'string'],
            'WARC-Block-Digest':  ['warc_block_digest', 'string'],
        }

        self._http_header_ref = {
            'Server': ['http_server', 'string'],
            'Content-Language':['http_content_language', 'string'],
            'Last-Modified':['http_last_modified', 'timestamp'],
            'Content-Type':['http_content_type', 'string']

        }

        self._table_ref = {
            'warc_hist':'warc_record_id', 
            'language':'http_content_language', 
            'server':'http_server', 
            'domain':'domain', 
            'charset':'charset'}

        self._warc_header_keys = sorted(self.warc_header_ref.keys())
        self._http_header_keys = sorted(self.http_header_ref.keys())
        self.logger = logger

    @property
    def warc_header_ref(self):
        return self._warc_header_ref
    
    @property
    def http_header_ref(self):
        return self._http_header_ref
    
    @property
    def warc_header_keys(self):
        return self._warc_header_keys
    
    @property
    def http_header_keys(self):
        return self._http_header_keys
    
    @property
    def table_ref(self):
        return self._table_ref
    
    def _get_spark_session(self):
        return SparkSession.builder.getOrCreate()

    def get_warc_filenames(self):
        return [os.path.join(self.warc_path, i) for i in os.listdir(self.warc_path) if bool(re.search("\.warc.gz$",i))]
    
    def read_warc(self, warc):
        # Iterator for reading WARC file contents as tuples
        with open(warc, 'rb') as stream:
            for record in ArchiveIterator(stream, func_filter=lambda x: x.headers.get('WARC-Type')=='response'):
                warc_header_data = [record.headers.get(i) for i in self.warc_header_keys]
                http_header_data = [record.http_headers.get(i) for i in self.http_header_keys]
                title_data = [None]
                html_data = record.reader.read().decode('utf-8', errors='replace')[0:1000]
                title_search = re.search(r"<title>([A-Za-z0-9]*?)</title>", html_data)
                if title_search:
                     title_data = [title_search.group(1)]

                yield tuple(warc_header_data + http_header_data + title_data)

    def _warc_rdd_to_dataframe(self, warc_rdd):
        # Convert initial RDD into a spark dataframe
        init_schema = T.StructType(
            [T.StructField(self.warc_header_ref[i][0], T.StringType(), True) for i in self.warc_header_keys] +
            [T.StructField(self.http_header_ref[i][0], T.StringType(), True) for i in self.http_header_keys] +
            [T.StructField('page_title', T.StringType(), True)]
        )

        output_df = warc_rdd.toDF(init_schema)

        return output_df
    
    def _format_dataframe(self, warc_df):
        # Format/transformation of initial spark dataframe columns
        if self.logger:
            self.logger.info('started formatting ')

        # Query
        output_df = warc_df \
        .select(['warc_record_id', 'warc_block_digest', 'warc_payload_digest', 'warc_target_uri','warc_date',
                 'warc_ip_address', 'warc_content_length', 'page_title', 'http_content_type',
                 'http_last_modified', 'http_content_language', 'http_server']) \
        .withColumn('domain', F.regexp_extract('warc_target_uri', r"^(?:https?:\/\/)?(?:www\.)?([^\/]+)", 1)) \
        .withColumn('charset', F.regexp_extract('http_content_type', r"charset=([^;]+)", 1)) \
        .withColumn('charset', F.regexp_replace('charset', r"\"|\'", "")) \
        .withColumn('warc_date', F.to_timestamp('warc_date')) \
        .withColumn('http_last_modified', F.trim(F.regexp_replace('http_last_modified', "^\w{3}, |GMT", ""))) \
        .withColumn('http_last_modified', F.to_timestamp('http_last_modified', 'dd MMM yyyy HH:mm:ss')) \
        .withColumn('http_last_modified', F.when(F.to_date('http_last_modified')<F.lit('1900-01-01'), F.lit(None)).otherwise(F.col('http_last_modified'))) \
        .withColumn('warc_content_length', F.col('warc_content_length').cast('int')) \
        .drop('http_content_type')
        
        for col in ['page_title', 'domain', 'charset', 'http_server', 'http_content_language']:
            output_df = output_df \
                .withColumn(col, F.lower(F.trim(col))) \
                .withColumn(col, F.when(F.col(col)=='', F.lit(None)).otherwise(F.col(col)))

        return output_df
    
    def _segment_warc_df(self, warc_df):
        # Split up initial dataframe to form additional tables for normalization
        language_df = warc_df.select('http_content_language').filter(F.col('http_content_language').isNotNull()).distinct()
        server_df = warc_df.select('http_server').filter(F.col('http_server').isNotNull()).distinct()
        domain_df = warc_df.select('domain').filter(F.col('domain').isNotNull()).distinct()
        charset_df = warc_df.select('charset').filter(F.col('charset').isNotNull()).distinct()
        return (language_df, server_df, domain_df, charset_df)
    
    def _read_table(self, table_name):
        spark_session = self._get_spark_session()
        dir = os.path.join(self.parquet_path, table_name)
        return spark_session.read.parquet(dir)
    
    def _create_or_update_norm_table(self, table_name, df):
        # Create/update parquet tables for normalization
        norm_tables = [i for i in self.table_ref.keys() if i != 'warc_hist']
        assert table_name in norm_tables, "table name must be in (warc_hist, language, server, domain, charset)"
        output_dir = os.path.join(self.parquet_path, table_name)
        id_name = table_name+'_id'
        window = Window.orderBy(F.lit(1))

        if os.path.exists(output_dir):
            existing_df = self._read_table(table_name)
            max_id = existing_df.agg(F.max(id_name)).collect()[0][0]
            output_df = df.join(existing_df, on=self.table_ref[table_name], how='left_anti')

            if output_df.isEmpty() is False:
                output_df = df.withColumn(id_name, F.row_number().over(window) + max_id).select([id_name, self.table_ref[table_name]])
                output_df.write.mode('append').parquet(output_dir)
        else:
            output_df = df.withColumn(id_name, F.row_number().over(window)).select([id_name, self.table_ref[table_name]])
            output_df.write.parquet(output_dir)

    def _create_or_update_warc_hist(self, warc_df):
        # Create/update parquet tables for main warc historical records
        language_df = self._read_table('language')
        server_df = self._read_table('server')
        domain_df = self._read_table('domain')
        charset_df = self._read_table('charset')
        output_dir = os.path.join(self.parquet_path, 'warc_hist')

        output_df = warc_df.join(language_df, on=self.table_ref['language'], how='left').drop(self.table_ref['language']) \
        .join(server_df, on=self.table_ref['server'], how='left').drop(self.table_ref['server']) \
        .join(domain_df, on=self.table_ref['domain'], how='left').drop(self.table_ref['domain']) \
        .join(charset_df, on=self.table_ref['charset'], how='left').drop(self.table_ref['charset'])

        if os.path.exists(output_dir):
            output_df.write.mode('append').parquet(output_dir)
        else:
            output_df.write.parquet(output_dir)

    def create_or_update_tables(self, rdd):
        # After read_warc is mapped to each filename in context, convert the rdd to a dataframe
        # then format and loaded all tables

        if self.logger:
            self.logger.info('started transform and loading of warc rdd')
        try:
            warc_df = self._warc_rdd_to_dataframe(rdd)
            warc_df = self._format_dataframe(warc_df)
            language_df, server_df, domain_df, charset_df = self._segment_warc_df(warc_df)

            # Write normalization tables
            for i,j in zip(['language','server','domain','charset'],[language_df, server_df, domain_df, charset_df]):
                self._create_or_update_norm_table(i, j)

            # Write warc hist table
            self._create_or_update_warc_hist(warc_df)

            if self.logger:
                self.logger.info('finished transform and loading of warc rdd')
        except Exception as e:
            if self.logger:
                self.logger.error('unexpected error when writing tables from warc rdd')
                self.logger.error(e)