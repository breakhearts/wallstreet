from __future__ import absolute_import
from wallstreet.crawler import stockapi
from wallstreet.crawler.fetcher import RequestsFetcher
from datetime import datetime
from wallstreet import config


class TestYahooStockHistoryAPI:
    def test_get_url_params(self):
        api = stockapi.YahooHistoryDataAPI()
        url, method, headers, data = api.get_url_params("BIDU", start_date="20150217", end_date="20150914")
        assert url == "http://real-chart.finance.yahoo.com/table.csv" \
                      "?s=BIDU&g=d&ignore=.csv&a=1&b=17&c=2015&d=8&e=14&f=2015"
        assert method == "GET"
        assert data == {}

    def test_parse_ret(self):
        api = stockapi.YahooHistoryDataAPI()
        url, method, headers, data = api.get_url_params("BIDU", start_date="20150218", end_date="20150220")
        fetcher = RequestsFetcher()
        status_code, content = fetcher.fetch(url, method, headers, data)
        assert status_code == 200
        days = api.parse_ret("BIDU", content)
        assert len(days) == 3
        day_last = days[0]
        assert day_last.symbol == "BIDU"
        assert day_last.date == datetime(2015, 2, 20)


class TestNasdaqStockInfoAPI:
    def test_all(self):
        api = stockapi.NasdaqStockInfoAPI()
        url, method, headers, data = api.get_url_params("NASDAQ")
        fetcher = RequestsFetcher()
        status_code, content = fetcher.fetch(url, method, headers, data)
        stock_infos = api.parse_ret("NASDAQ", content)
        assert len(stock_infos) > 100


class TestEdgarAPI:
    def test_year_fiscal_report(self):
        api = stockapi.EdgarYearReportAPI(config.get_test("edgar", "core_key"))
        url, method, headers, data = api.get_url_params(["BIDU", "AAPL"], start_year=2011, end_year=2012)
        fetcher = RequestsFetcher(timeout=30)
        status_code, content = fetcher.fetch(url, method, headers, data)
        raw_report = api.parse_ret(content)
        assert len(raw_report) == 4

    def test_quarter_fiscal_report(self):
        api = stockapi.EdgarQuarterReportAPI(config.get_test("edgar", "core_key"))
        url, method, headers, data = api.get_url_params("FB", start_year=2014, end_year=2015, start_quarter=3, end_quarter=1)
        fetcher = RequestsFetcher(timeout=30)
        status_code, content = fetcher.fetch(url, method, headers, data)
        raw_report = api.parse_ret(content)
        assert len(raw_report) == 3

    def test_company_report(self):
        api = stockapi.EdgarCompanyAPI(config.get_test("edgar", "core_key"))
        url, method, headers, data = api.get_url_params(["BIDU", "BABA"])
        fetcher = RequestsFetcher(timeout=30)
        status_code, content = fetcher.fetch(url, method, headers, data)
        t = api.parse_ret(content)
        assert len(t) == 2