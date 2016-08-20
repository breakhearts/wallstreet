from __future__ import absolute_import
from wallstreet.crawel import stockapi, fetcher


fetch = fetcher.RequestsFetcher()
api = stockapi.EdgarYearReportAPI("z7pz5epbkg65zwm6bgn375za")
url, method, headers, data = api.get_url_params("BIDU", "2010", "2012")
status_code, content = fetch.fetch(url, method, headers, data)
api.parse_ret("BIDU", content)