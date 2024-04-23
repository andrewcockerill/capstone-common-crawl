import os
import requests
from bs4 import BeautifulSoup
import logging
import re
import datetime
import gzip
import json


class DownloadManager:
    def __init__(self, logger=None):
        self.logger = logger
        self._overview_url = r"https://commoncrawl.org/overview"
        self._data_url = r"https://data.commoncrawl.org/"
        self._data_path = r"crawl-data/"
        self._warc_ext = r"warc.paths.gz"

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

    def _fetch_path_zips(self, year_start=2020, year_end=None):
        year_start = int(year_start)
        if year_end is None:
            year_end = int(datetime.datetime.now().year)
        assert year_start <= year_end, 'end year must be > start year'
        year_regex = '|'.join([str(y) for y in range(year_start, year_end+1)])

        html_content = requests.get(self.overview_url).content
        self.soup = BeautifulSoup(html_content)
        cc_indices = [str(s.contents[0]) for s in self.soup.find_all(
            'h6') if bool(re.search(year_regex, str(s.contents[0])))]

        gz_urls = [
            f"{self.data_url}{self.data_path}{ind}/{self.warc_ext}" for ind in cc_indices]
        return list(zip(cc_indices, gz_urls))

    def _decompress_path_zip(self, url):
        zip_file = requests.get(url).content
        return [f"{self.data_url}{i}" for i in gzip.decompress(zip_file).strip().decode('utf-8').split()]

    def get_warc_urls(self, output_filename, year_start=2020, year_end=None):
        try:
            if self.logger:
                self.logger.info('started fetch of indices and path zips')
            path_zips = self._fetch_path_zips(
                year_start=year_start, year_end=year_end)
            with open(output_filename, 'w') as output:
                json.dump({i[0]: self._decompress_path_zip(i[1])
                           for i in path_zips}, output)
        except Exception as e:
            print(e)
            if self.logger:
                self.logger.error(e)
