from __future__ import absolute_import
from wallstreet.crawel import stockapi
from wallstreet.crawel.fetcher import RequestsFetcher


class TestYahooStockAPI:
    def test_get_url_params(self):
        api = stockapi.YahooHistoryDataAPI()
        url, method, headers, data = api.get_url_params("BIDU", start_date="20150217", end_date="20150914")
        assert url == "http://real-chart.finance.yahoo.com/table.csv" \
                      "?s=BIDU&g=d&ignore=.csv&a=1&b=17&c=2015&d=8&e=14&f=2015"
        assert method == "GET"
        assert headers == {}
        assert data == {}

    def test_parse_ret(self):
        api = stockapi.YahooHistoryDataAPI()
        url, method, headers, data = api.get_url_params("BIDU", start_date="20150218", end_date="20150220")
        fetcher = RequestsFetcher()
        status_code, content = fetcher.fetch(url, method, headers, data)
        assert status_code == 200
        days = api.parse_ret("BIDU", content.decode('utf-8'))
        assert len(days) == 3
        day_last = days[0]
        assert day_last.symbol == "BIDU"
        assert day_last.date == "2015-02-20"
