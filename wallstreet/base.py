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
    def __init__(self, change, ma5, ma20, ma60, vol5, vol20, vol60):
        self.change = change
        self.ma5 = ma5
        self.ma20 = ma20
        self.ma60 = ma60
        self.vol5 = vol5
        self.vol20 = vol20
        self.vol60 = vol60


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
