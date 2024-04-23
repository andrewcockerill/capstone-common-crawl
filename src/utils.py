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
    def __init__(self, logger=None):
        self.logger = logger
        self._overview_url = r"https://commoncrawl.org/overview"
        self._data_url = r"https://data.commoncrawl.org/"
        self._data_path = r"crawl-data/"
        self._warc_ext = r"warc.paths.gz"
        self._max_files_download = 10

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
    def max_files_download(self):
        return self._max_files_download

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

    def get_warc_urls(self, output_dir, year_start=2020, year_end=None):
        try:
            if self.logger:
                self.logger.info('started fetch of indices and path zips')
            path_zips = self._fetch_path_zips(
                year_start=year_start, year_end=year_end)
            output_json = {i[0]: self._decompress_path_zip(i[1])
                           for i in path_zips}
            output_json_meta = {k: len(v) for k, v in output_json.items()}

            with open(os.path.join(output_dir, 'warc_urls.json'), 'w') as output:
                json.dump(output_json, output)

            with open(os.path.join(output_dir, 'warc_urls_meta.json'), 'w') as output_meta:
                json.dump(output_json_meta, output_meta)
        except Exception as e:
            print(e)
            if self.logger:
                self.logger.error(e)

    def download_sample(output_dir, url_json_loc, url_json_metadata_loc, num_files=1, seed=1):
        with open(url_json_loc) as f:
            url_dict = json.loads(f.read())

        with open(url_json_metadata_loc) as f:
            url_metadata_dict = json.loads(f.read())

        np.random.seed(seed)
        cc_sample_sizes = dict(Counter(np.random.choice(
            list(url_metadata_dict.keys()), size=num_files, replace=True)))
        cc_sample_indices = {k: np.random.choice(
            url_metadata_dict[k], size=v, replace=False).tolist() for k, v in cc_sample_sizes.items()}

        for k, v in cc_sample_indices.items():
            urls = url_dict[k]
            for elem in v:
                url = urls[elem]
                print(f"Downloading {url}:")
                outfile = os.path.join(output_dir, re.sub(".*/", "", url))
                r = requests.get(url, stream=True)
                total_size = int(r.headers.get("content-length", 0))
                block_size = 1024
                with tqdm.tqdm(total=total_size, unit="B", unit_scale=True) as progress_bar:
                    with open(outfile, "wb") as file:
                        for data in r.iter_content(block_size):
                            progress_bar.update(len(data))
                            file.write(data)
            print('Finished!')
