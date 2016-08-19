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
        capital = init_capital
        shareholders = 0
        last_price = 0
        count = 0
        for i, t in enumerate(X_and_prices):
            X, price = t
            _open, low, high, close = price
            op, price, count = self.predict(t, shareholders, capital)
            if op == Trader.BUY:
                assert count * price <= capital
                assert low <= price <= high
                capital -= price * count
                shareholders += count
            elif op == Trader.SELL:
                assert count <= shareholders
                assert low <= price <= high
                capital += price * count
                shareholders -= count
            last_price = price
            count += 1
        profit = ((capital + shareholders * last_price) / float(init_capital)) - 1
        avg_profit = count > 0 and profit / count or 0
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
        if t == 0:
            return Trader.HOLD, 0, 0
        if shareholders > 0 and low <= t:
            price = min(max(low, t), high)
            return Trader.SELL, price, shareholders
        elif high >= t:
            price = min(max(low, t), high)
            return Trader.BUY, price, int(capital / price)
        else:
            return Trader.HOLD, 0, 0

if __name__ == "__main__":
    from wallstreet.storage import create_sql_engine_and_session_cls
    from wallstreet import config
    engine, Session = create_sql_engine_and_session_cls(config.get("storage", "url"))
    iterator = batch_MA_iterator(["BANC"], engine, Session)
    trader = SimpleMATrader()
    trader.fit(iterator)