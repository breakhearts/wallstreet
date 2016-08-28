from __future__ import absolute_import
from wallstreet import base
from wallstreet.crawler.fetcher import CurlFetcher
from wallstreet.crawler.stockapi import SECAPI
import os


class SECCrawler(object):
    def __init__(self, data_dir):
        self.data_dir = data_dir

    def __quarter_idx_file(self, year, quarter):
        return os.path.join(self.data_dir, "{0}/QTR{1}/crawler.idx".format(year, quarter))

    def load_idx_file(self, filename, filter_form_type=None):
        api = SECAPI()
        with open(filename, "rb") as f:
            content = f.read()
            idx = api.parse_idx(content, filter_form_type)
            return idx

    def download_quarter_idx_file(self, year, quarter, filename):
        api = SECAPI()
        url, method, headers, data = api.quarter_idx_url_params(year, quarter)
        fetcher = CurlFetcher(60*30)
        return fetcher.fetch_to_file(url, method, headers, data, filename=filename)

    def load_quarter_idx(self, year, quarter, filter_form_type=None):
        filename = self.__quarter_idx_file(year, quarter)
        status_code = 200
        if not os.path.exists(filename):
            base.wise_mk_dir_for_file(filename)
            status_code = self.download_quarter_idx_file(year, quarter, filename)
            if status_code != 200:
                return status_code, None
        return status_code, self.load_idx_file(filename, filter_form_type)