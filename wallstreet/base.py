"""
data structure and utility functions
"""
from datetime import datetime, timedelta


class StockInfo(object):
    def __init__(self, symbol, exchange, last_update_date=datetime.min):
        self.symbol = symbol
        self.exchange = exchange
        self.last_update_date = last_update_date


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


def get_next_day_str(today):
    """
    :param today: datetime of today
    :return: str of next day
    """
    start_date = today + timedelta(days=1)
    return start_date.strftime("%Y%m%d")
