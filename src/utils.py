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
import tqdm


class DownloadManager:
    """Class to house multiple methods for obtaining WARC archive URLs and downloading of actual files.

    Attributes
    ----------

    logger : Logger
        Logger object for tracking

    Methods
    -------

    get_warc_urls(output_dir, year_start, year_end)
        Creates a URL catalog of Common Crawl files in JSON format in `output_dir` along with a metadata
        JSON file that includes the same keys and counts of URLs per key. Each key is a Common Crawl index
        as described at https://commoncrawl.org/overview. Data will be fetched for years from `year_start`
        to `year_end`.

    download_sample(output_dir, url_json_loc, url_json_metadata_loc, num_files, seed)
        Downloads actual Common Crawl WARC files in .gz archive format. A total of `num_files` are
        randomly selected based on `seed`. Download URLs are determined from JSON URL lists and metadata files
        created by get_warc_urls. Downloads are saved under `output_dir`.

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
        self._max_files_total = 10

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