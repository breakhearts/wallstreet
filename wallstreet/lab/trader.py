from __future__ import absolute_import
from wallstreet.storage import StockDaySqlStorage, BaseIndexSqlStorage


class Trader(object):
    HOLD = 0
    BUY = 1
    SELL = 2

    def fit(self, X_and_price_batch):
        raise NotImplementedError

    def predict(self, x_and_price, shareholders, capital):
        raise NotImplementedError

    def eval(self, X_and_prices, init_capital=1000000):
        X, prices = X_and_prices
        capital = init_capital
        shareholders = 0
        for i, x in enumerate(X):
            price = prices[i]
            _open, low, high, close = price
            op, price, count = self.predict(X, price, shareholders, capital)
            if op == Trader.BUY:
                assert count * price <= capital
                assert low <= price <= high
                capital -= price * price
                shareholders += count
            elif op == Trader.SELL:
                assert count * price <= capital
                assert low <= price <= high
                capital += price * price
                shareholders -= count
        profit = (capital + shareholders * prices[-1] / float(init_capital)) - 1
        avg_profit = profit / len(X)
        return profit, avg_profit

    def eval_batch(self, X_and_price_batch, init_capital=1000000):
        profits = []
        avg_profits = []
        for i, X_and_prices in enumerate(X_and_price_batch):
            profit, avg_profit = self.eval(X_and_prices, init_capital)
            profits.append(profit)
            avg_profits.append(avg_profit)
        return sum(profits) / len(profits), sum(avg_profits) / len(avg_profits), [t for t in zip(profits, avg_profits)]


def MA_iterator(symbol, engine, Session):
    storage = StockDaySqlStorage(engine, Session)
    days = storage.load(symbol)
    days.sort(key=lambda x:x.date)
    storage = BaseIndexSqlStorage(engine, Session)
    base_indexs = storage.load(symbol)
    base_indexs.sort(key=lambda x: x.date)
    day_index = 0
    for i, base_index in enumerate(base_indexs):
        if base_index.date < days[day_index].date:
            continue
        while base_index.date > days[day_index].date:
            day_index += 1
        day = days[day_index]
        yield (base_index.ma5, base_index.ma20, base_index.ma60), (day.open, day.low, day.high, day.close)


def batch_MA_iterator(symbols, engine, Session):
    for symbols in symbols:
        yield MA_iterator(symbols, engine, Session)


class SimpleMATrader(Trader):
    MA5 = 0
    MA20 = 0
    MA60 = 0

    def __init__(self):
        self.index = SimpleMATrader.MA5

    def fit(self, X_and_price_batch):
        t = []
        self.index = SimpleMATrader.MA5
        profit, avg_profit, _ = self.eval_batch(X_and_price_batch)
        t.append((avg_profit, SimpleMATrader.MA5))
        self.index = SimpleMATrader.MA20
        profit, avg_profit, _ = self.eval_batch(X_and_price_batch)
        t.append((avg_profit, SimpleMATrader.MA20))
        self.index = SimpleMATrader.MA60
        profit, avg_profit, _ = self.eval_batch(X_and_price_batch)
        t.append((avg_profit, SimpleMATrader.MA60))
        t.sort()
        self.index = t[-1][1]

    def predict(self, x_and_price, shareholders, capital):
        x, price = x_and_price
        ma5, ma20, ma60 = x
        _open, low, high, close = price
        if self.index == SimpleMATrader.MA5:
            t = ma5
        elif self.index == SimpleMATrader.MA20:
            t = ma20
        else:
            t = ma60
        if shareholders > 0 and low <= t:
            return Trader.SELL, shareholders, t
        elif high >= t:
            return Trader.BUY, int(capital / t), t
        else:
            return Trader.HOLD, 0, 0