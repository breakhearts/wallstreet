"""
fetch web resource
"""
import requests
from io import BytesIO
import pycurl
import certifi
import os
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

    def fetch_to_file(self, url, method, headers, data, filename, resume_broken_downloads=True):
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

    def fetch_to_file(self, url, method, headers, data, filename, resume_broken_downloads=True):
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
        if url.startswith("ftp"):
            c.setopt(c.TCP_KEEPALIVE, 1)
            c.setopt(c.TCP_KEEPIDLE, 30)
            c.setopt(c.TCP_KEEPINTV, 30)
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

    def fetch_to_file(self, url, method, headers, data, filename, resume_broken_downloads=True):
        download_file = filename + ".download"
        start_pos = 0
        if resume_broken_downloads and os.path.exists(download_file):
            start_pos = os.path.getsize(download_file)
        status_code = 200
        finished = False
        with open(download_file, "ab") as f:
            c = pycurl.Curl()
            c.setopt(c.URL, url)
            c.setopt(c.WRITEDATA, f)
            c.setopt(c.CAINFO, certifi.where())
            c.setopt(c.TIMEOUT, self.timeout)
            if start_pos > 0:
                c.setopt(c.RANGE, "{0}-".format(start_pos))
            #if url.startswith("ftp"):
            #    c.setopt(c.FTP_RESPONSE_TIMEOUT, self.timeout)
            if method == "POST":
                post_fields = urlencode(data)
                c.setopt(c.POSTFIELDS, post_fields)
            if url.startswith("ftp"):
                c.setopt(c.TCP_KEEPALIVE, 1)
                c.setopt(c.TCP_KEEPIDLE, 30)
                c.setopt(c.TCP_KEEPINTV, 30)
            if len(headers) > 0:
                list_headers = []
                for k, v in headers.items():
                    list_headers.append('{0}:{1}'.format(k, v))
                c.setopt(c.HTTPHEADER, list_headers)
            try:
                c.perform()
                if url.startswith("http"):
                    status_code = c.getinfo(pycurl.HTTP_CODE)
                finished = True
            except pycurl.error as exc:
                if exc.args[0] == 78:
                    status_code = 404
                else:
                    raise
            finally:
                c.close()
        if finished:
            os.rename(download_file, filename)
        return status_code