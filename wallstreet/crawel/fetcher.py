"""
fetch web resource
"""
import requests


class Fetcher(object):
    """
    fetcher interface
    :returns status_code, content
    """
    def fetch(self, url, method, headers, data):
        raise NotImplementedError


class RequestsFetcher(object):
    def __init__(self, timeout=10):
        self.timeout = timeout

    """
    fetcher using requests lib
    """
    def fetch(self, url, method, headers, data):
        if method == "GET":
            r = requests.get(url, headers=headers, timeout=self.timeout)
        elif method == "POST":
            r = requests.post(url, headers=headers, data=data, timeout=self.timeout)
        else:
            raise ValueError
        return r.status_code, r.content
