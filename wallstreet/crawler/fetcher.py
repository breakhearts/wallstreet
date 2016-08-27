"""
fetch web resource
"""
import requests
from io import BytesIO
import pycurl
import certifi
try:
    # python 3
    from urllib.parse import urlencode
except ImportError:
    # python 2
    from urllib import urlencode


class Fetcher(object):
    """
    fetcher interface
    :returns status_code, content
    """
    def fetch(self, url, method, headers, data):
        raise NotImplementedError

    def fetch_to_file(self, url, method, headers, data, filename):
        raise NotImplementedError


class RequestsFetcher(Fetcher):
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

    def fetch_to_file(self, url, method, headers, data, filename):
        raise NotImplementedError


class CurlFetcher(Fetcher):
    def __init__(self, timeout=10):
        self.timeout = timeout

    def fetch(self, url, method, headers, data):
        buffer = BytesIO()
        c = pycurl.Curl()
        c.setopt(c.URL, url)
        c.setopt(c.WRITEDATA, buffer)
        c.setopt(c.CAINFO, certifi.where())
        c.setopt(c.TIMEOUT, self.timeout)
        if method == "POST":
            post_fields = urlencode(data)
            c.setopt(c.POSTFIELDS, post_fields)
        if len(headers) > 0:
            list_headers = []
            for k, v in headers.items():
                list_headers.append('{0}:{1}'.format(k, v))
            c.setopt(c.HTTPHEADER, list_headers)
        try:
            status_code = 200
            c.perform()
        except pycurl.error as exc:
            if exc.args[0] == 78:
                status_code = 404
            else:
                raise
        if url.startswith("http"):
            status_code = c.getinfo(pycurl.HTTP_CODE)
        c.close()
        return status_code, buffer.getvalue()

    def fetch_to_file(self, url, method, headers, data, filename):
        status_code, content = self.fetch(url, method, headers, data)
        if status_code != 200:
            return status_code
        with open(filename, "wb") as f:
            f.write(content)
        return status_code

