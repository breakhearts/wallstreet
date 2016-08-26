from __future__ import absolute_import
from datetime import datetime
from wallstreet.base import StockDay, StockInfo
from dateutil.parser import parse
from wallstreet import base
from collections import defaultdict
import json


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


class NasdaqStockInfoAPI(StockInfoAPI):
    def get_url_params(self, exchange):
        query = "http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange={0}&render=download"\
            .format(exchange)
        return query, "GET", {"user-agent": base.random_ua()}, {}

    def parse_ret(self, exchange, content):
        content = content.decode('utf-8')
        data = []
        symbols = {}
        for line in content.splitlines()[1:]:
            t = [x.strip('"') for x in line.split('",')]
            symbol, name, last_sale, market_cap, adr_tso, ipo_year, sector, industry, summary_quote = t[:9]
            symbol = symbol.strip()
            if symbol not in symbols and symbol.isalpha():
                data.append(StockInfo(symbol, exchange))
                symbols[symbol] = True
        return data


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
        return api, "GET", {"user-agent": base.random_ua()}, {}

    def parse_ret(self, stock, content):
        content = content.decode('utf-8')
        data = []
        for line in content.splitlines()[1:]:
            t = line.split(",")
            if len(t) == 7:
                date, open, high, low, close, volume, adj_close = t
                close, adj_close = float(close), float(adj_close)
                adj_factor = close > 0 and adj_close / close or 1.0
                data.append(StockDay(stock.upper(), parse(date), open, close, high, low, volume, adj_factor))
        return data


class QuarterReportAPI(object):
    """
    get quarter financial report, note not all company submit this report
    """
    def get_url_params(self, symbols, start_year, start_quarter, end_year, end_quarter):
        raise NotImplementedError

    def parse_ret(self, content):
        raise NotImplementedError


class YearReportAPI(object):
    """
    get year financial report, all company submit this report
    """
    def get_url_params(self, symbols, start_year, end_year):
        raise NotImplementedError

    def parse_ret(self, content):
        raise NotImplementedError


class InsiderIssuesAPI(object):
    """
    get recent insider issues
    """
    def get_url_params(self, symbol):
        raise NotImplementedError

    def parse_ret(self, symbol, content):
        raise NotImplementedError


class IssueHolderAPI(object):
    def get_url_params(self, symbol):
        raise NotImplementedError

    def parse_ret(self, symbol, content):
        raise NotImplementedError

class CompanyAPI(object):
    def get_url_params(self, symbols):
        raise NotImplementedError

    def parse_ret(self, content):
        raise NotImplementedError


class EdgarAPI(object):
    def __init__(self, key):
        self.key = key


class EdgarQuarterReportAPI(QuarterReportAPI, EdgarAPI):
    def get_url_params(self, symbols, start_year, start_quarter, end_year, end_quarter):
        if isinstance(symbols, str):
            symbols = [symbols]
        symbol = ",".join(symbols)
        api = 'http://edgaronline.api.mashery.com/v2/corefinancials/qtr?' \
              'primarysymbols={0}&fiscalperiod={1}q{2}~{3}q{4}&appkey={5}&' \
              'fields=primarysymbol,fiscalperiod,BalanceSheetConsolidated,IncomeStatementConsolidated,CashFlowStatementConsolidated'\
            .format(symbol, start_year, start_quarter, end_year, end_quarter, self.key)
        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01"
        }
        return api, "GET", headers, {}

    def parse_ret(self, content):
        t = json.loads(content.decode("utf-8"))
        ret = []
        values = {}
        for row in t["result"]["rows"]:
            for kv in row["values"]:
                values[kv["field"]] = kv["value"]
            report = base.RawFiscalReport(values["primarysymbol"], values["fiscalperiod"], json.dumps(values))
            ret.append(report)
        return ret


class EdgarYearReportAPI(YearReportAPI, EdgarAPI):
    def get_url_params(self, symbols, start_year, end_year):
        if isinstance(symbols, str):
            symbols = [symbols]
        symbol = ",".join(symbols)
        api = 'http://edgaronline.api.mashery.com/v2/corefinancials/ann?' \
              'primarysymbols={0}&fiscalperiod={1}q{2}~{3}q{4}&appkey={5}&' \
              'fields=primarysymbol,fiscalperiod,BalanceSheetConsolidated,IncomeStatementConsolidated,CashFlowStatementConsolidated'\
            .format(symbol, start_year, 1, end_year, 4, self.key)
        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01"
        }
        return api, "GET", headers, {}

    def parse_ret(self, content):
        t = json.loads(content.decode("utf-8"))
        ret = []
        values = {}
        symbol_period = {}
        for row in t["result"]["rows"]:
            for kv in row["values"]:
                values[kv["field"]] = kv["value"]
            symbol, period = values["primarysymbol"], values["fiscalperiod"]
            k = "{0}_{1}".format(symbol, period)
            if k not in symbol_period:
                symbol_period[k] = True
                report = base.RawFiscalReport(symbol, period, json.dumps(values))
                ret.append(report)
        return ret


class EdgarInsiderIssuesAPI(InsiderIssuesAPI, EdgarAPI):
    def get_url_params(self, symbol):
        api = 'http://edgaronline.api.mashery.com/v2/insiders/issues?' \
              'fields=issueticker,issueid,transactiondate,filername,transactiontype,ownershiptype,relationship,transactionpricefrom,transactionpriceto&' \
              'filter=issueticker in ({0}) &appkey={1}&limit=9999'.format(symbol, self.key)
        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01"
        }
        return api, "GET", headers, {}

    def parse_ret(self, symbol, content):
        raise NotImplementedError


class EdgarIssueHolderAPI(IssueHolderAPI, EdgarAPI):
    def get_url_params(self, symbol):
        api = 'http://edgaronline.api.mashery.com/v2/ownerships/currentissueholders?' \
              'filter=ticker in ({0}) &appkey={1}&limit=9999&' \
              'fields=modifiedsince,ticker,ownername,issueid,currentreportdate,priorreportdate,sharesheld,sharesheldchange,sharesheldpercentchange,portfoliopercent,sharesoutpercent'.format(
            symbol, self.key)
        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01"
        }
        return api, "GET", headers, {}

    def parse_ret(self, symbol, content):
        raise NotImplementedError


class EdgarCompanyAPI(CompanyAPI, EdgarAPI):
    def get_url_params(self, symbols):
        if isinstance(symbols, str):
            symbols = [symbols]
        symbol = ",".join(symbols)
        api = 'http://edgaronline.api.mashery.com/v2/companies?appkey={0}&primarysymbols={1}&' \
              'fields=primarysymbol,cik,primaryexchange,siccode,industry,sector,city'.format(self.key, symbol)
        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01"
        }
        return api, "GET", headers, {}

    def parse_ret(self, content):
        t = json.loads(content.decode("utf-8"))
        ret = []
        values = defaultdict(str)
        for row in t["result"]["rows"]:
            for kv in row["values"]:
                values[kv["field"]] = kv["value"]
            if "primarysymbol" in values:
                ret.append(base.StockInfoDetail(symbol=values["primarysymbol"], exchange=values["primaryexchange"],
                                                siccode=values["siccode"], industry=values["industry"],
                                                sector=values["sector"], city=values["city"], cik=values["cik"]))
        return ret







