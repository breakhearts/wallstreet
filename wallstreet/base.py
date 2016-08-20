"""
data structure and utility functions
"""
from datetime import timedelta
from datetime import datetime
import os
from dateutil.parser import parse
from dateutil import tz


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
        if len(last_60_day) == 0:
            return
        adj_last_60_close_price = [x.close * x.adj_factor for x in last_60_day]
        last_60_vol = [x.volume for x in last_60_day]
        self.date = last_60_day[0].date
        self.symbol = last_60_day[0].symbol
        if len(adj_last_60_close_price) == 1 or adj_last_60_close_price[1] == 0:
            self.change = 0
        else:
            self.change = (adj_last_60_close_price[0] / adj_last_60_close_price[1]) - 1
        self.ma5 = len(adj_last_60_close_price) >= 5 and sum(adj_last_60_close_price[:5]) / 5 or 0
        self.ma20 = len(adj_last_60_close_price) >= 20 and sum(adj_last_60_close_price[:20]) / 20 or 0
        self.ma60 = len(adj_last_60_close_price) >= 60 and sum(adj_last_60_close_price[:60]) / 60 or 0
        self.vol5 = len(last_60_vol) >= 5 and sum(last_60_vol[:5]) / 5 or 0
        self.vol20 = len(last_60_vol) >= 20 and sum(last_60_vol[:20]) / 20 or 0
        self.vol60 = len(last_60_vol) >= 60 and sum(last_60_vol[:60]) / 60 or 0

    def serializable_obj(self):
        return {
            "symbol": self.symbol,
            "date": self.date.strftime("%Y-%m-%d"),
            "change": self.change,
            "ma5": self.ma5,
            "ma20": self.ma20,
            "ma60": self.ma60,
            "vol5": self.vol5,
            "vol20": self.vol20,
            "vol60": self.vol60
        }

    @staticmethod
    def from_serializable_obj(obj):
        return BaseIndex(
            symbol=obj["symbol"],
            date=parse(obj["date"]),
            change=obj["change"],
            ma5=obj["ma5"],
            ma20=obj["ma20"],
            ma60=obj["ma60"],
            vol5=obj["vol5"],
            vol20=obj["vol20"],
            vol60=obj["vol60"]
        )


class RawFiscalReport(object):
    YEAR = 1
    QUARTER = 2

    def __init__(self, symbol, fiscal_period, content):
        self.symbol = symbol
        self.fiscal_period = fiscal_period
        self.content = content


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


def get_trading_date_skip_weekend(t):
    if t.weekday() == 5:
        t = t - timedelta(days=1)
    elif t.weekday() == 6:
        t = t - timedelta(days=2)
    else:
        t = t
    return datetime(year=t.year, month=t.month, day=t.day)


def get_last_real_time_date():
    ny_now = datetime.now(tz.tzstr("EST5EDT"))
    if (ny_now.hour == 9 and ny_now.minute < 30) or ny_now.hour < 9:
        t = ny_now - timedelta(days=1)
    else:
        t = ny_now
    return get_trading_date_skip_weekend(t)


def get_last_pre_market_date():
    ny_now = datetime.now(tz.tzstr("EST5EDT"))
    if ny_now.hour < 4 or ny_now.hour == 4 and ny_now.minute <= 30:
        t = ny_now - timedelta(days=1)
    else:
        t = ny_now
    return get_trading_date_skip_weekend(t)


def get_last_after_hour_date():
    ny_now = datetime.now(tz.tzstr("EST5EDT"))
    if ny_now.hour < 16 or ny_now.hour == 16 and ny_now.minute <= 30:
        t = ny_now - timedelta(days=1)
    else:
        t = ny_now
    return get_trading_date_skip_weekend(t)

UA_LIST = [
    "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
    "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0;",
    "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
    "Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
    "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11",
    "Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Maxthon 2.0)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; TencentTraveler 4.0)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SE 2.X MetaSr 1.0; SE 2.X MetaSr 1.0; .NET CLR 2.0.50727; SE 2.X MetaSr 1.0)"
]


def random_ua():
    import random
    return random.choice(UA_LIST)