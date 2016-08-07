from __future__ import absolute_import
from datetime import datetime
from wallstreet.base import StockDay, StockInfo
from dateutil.parser import parse


class StockInfoAPI(object):
    def get_url_params(self, exchange):
        """
        :param exchange: trade exchange
        :return: url, method, headers, data
        """
        raise NotImplementedError

    def parse_ret(self, exchange, content):
        """
        :params content: http response content
        :return: StockInfo list
        """
        raise NotImplementedError


class HistoryDataAPI(object):
    def get_url_params(self, stock, start_date=None, end_date=None):
        """
        get api url parameters
        :returns: url, method, headers, data
        """
        raise NotImplementedError

    def parse_ret(self, stock, content):
        """
        parse return stock data
        :return StockDay list
        """
        raise NotImplementedError


class NasdaqStockInfoAPI(StockInfoAPI):
    def get_url_params(self, exchange):
        query = "http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange={0}&render=download"\
            .format(exchange)
        return query, "GET", {}, {}

    def parse_ret(self, exchange, content):
        data = []
        symbols = {}
        for line in content.splitlines()[1:]:
            t = [x.strip('"') for x in line.split('",')]
            symbol, name, last_sale, market_cap, adr_tso, ipo_year, sector, industry, summary_quote = t[:9]
            if symbol not in symbols and symbol.find("^") == -1:
                data.append(StockInfo(symbol, exchange))
                symbols[symbol] = True
        return data


class YahooHistoryDataAPI(HistoryDataAPI):
    def get_url_params(self, stock, start_date=None, end_date=None):
        query = "?s={0}&g=d&ignore=.csv".format(stock)
        if start_date:
            start_date = datetime.strptime(start_date, "%Y%m%d")
            query += "&a={0}&b={1}&c={2}".format(start_date.month - 1, start_date.day, start_date.year)
        if end_date:
            end_date = datetime.strptime(end_date, "%Y%m%d")
            query += "&d={0}&e={1}&f={2}".format(end_date.month - 1, end_date.day, end_date.year)
        api = "http://real-chart.finance.yahoo.com/table.csv" + query
        return api, "GET", {}, {}

    def parse_ret(self, stock, content):
        data = []
        for line in content.splitlines()[1:]:
            t = line.split(",")
            if len(t) == 7:
                date, open, high, low, close, volume, adj_close = t
                close, adj_close = float(close), float(adj_close)
                adj_factor = close > 0 and adj_close / close or 1.0
                data.append(StockDay(stock.upper(), parse(date), open, close, high, low, volume, adj_factor))
        return data
