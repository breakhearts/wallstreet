"""
data structure and utility functions
"""
from datetime import timedelta
import os
from dateutil.parser import parse


class StockInfo(object):
    def __init__(self, symbol, exchange):
        self.symbol = symbol
        self.exchange = exchange

    def serializable_obj(self):
        return {
            "symbol": self.symbol,
            "exchange": self.exchange
        }

    @staticmethod
    def from_serializable_obj(obj):
        return StockInfo(symbol=obj["symbol"], exchange=obj["exchange"])


class StockDay(object):
    def __init__(self, symbol, date, open, close, high, low, volume, adj_factor):
        self.symbol = symbol
        self.date = date
        self.open = open
        self.close = close
        self.high = high
        self.low = low
        self.volume = volume
        self.adj_factor = adj_factor

    @property
    def adj_open(self):
        return self.open * self.adj_factor

    @property
    def adj_close(self):
        return self.close * self.adj_factor

    @property
    def adj_high(self):
        return self.high * self.adj_factor

    @property
    def adj_low(self):
        return self.low * self.adj_factor

    def price_change(self, last_stock_day):
        return last_stock_day and (self.adj_close - last_stock_day.adj_close) / last_stock_day.adj_close or 0

    def serializable_obj(self):
        return {
            "symbol": self.symbol,
            "date": self.date.strftime("%Y-%m-%d"),
            "open": self.open,
            "close": self.close,
            "high": self.high,
            "low": self.low,
            "volume": self.volume,
            "adj_factor": self.adj_factor
        }

    @staticmethod
    def from_serializable_obj(obj):
        return StockDay(
                symbol=obj["symbol"],
                date=parse(obj["date"]),
                open=obj["open"],
                close=obj["close"],
                high=obj["high"],
                low=obj["low"],
                volume=obj["volume"],
                adj_factor=obj["adj_factor"]
            )


class BaseIndex(object):
    """
    basic index
    """
    def __init__(self, symbol=None, date=None, change=0, ma5=0, ma20=0, ma60=0, vol5=0, vol20=0, vol60=0):
        self.symbol = symbol
        self.date = date
        self.change = change
        self.ma5 = ma5
        self.ma20 = ma20
        self.ma60 = ma60
        self.vol5 = vol5
        self.vol20 = vol20
        self.vol60 = vol60

    def update(self, last_60_day):
        adj_last_60_close_price = [x.close * x.adj_factor for x in last_60_day]
        last_60_vol = [x.volume for x in last_60_day]
        self.date = last_60_day[0].date
        self.symbol = last_60_day[0].symbol
        self.change = len(adj_last_60_close_price) == 1 and 0 or (adj_last_60_close_price[0] / adj_last_60_close_price[1]) - 1
        self.ma5 = len(last_60_day) >= 5 and sum(adj_last_60_close_price[:5]) / 5 or 0
        self.ma20 = len(last_60_day) >= 20 and sum(adj_last_60_close_price[:20]) / 20 or 0
        self.ma60 = len(last_60_day) >= 60 and sum(adj_last_60_close_price[:60]) / 60 or 0
        self.vol5 = len(last_60_vol) >= 5 and sum(last_60_vol[:5]) / 5 or 0
        self.vol20 = len(last_60_vol) >= 20 and sum(last_60_vol[:20]) / 20 or 0
        self.vol60 = len(last_60_vol) >= 60 and sum(last_60_vol[:60]) / 60 or 0


def get_day_str(date):
    return date.strftime("%Y%m%d")


def get_next_day_str(today):
    """
    :param today: datetime of today
    :return: str of next day
    """
    start_date = today + timedelta(days=1)
    return start_date.strftime("%Y%m%d")


def wise_mk_dir(path):
    if path == "":
        return
    if os.path.exists(path):
        return
    p, c = os.path.split(path)
    if not os.path.exists(p):
        wise_mk_dir(p)
    os.mkdir(path)


def wise_mk_dir_for_file(filepath):
    p = os.path.dirname(filepath)
    wise_mk_dir(p)
