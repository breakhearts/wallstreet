"""
storage
"""
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.orm import sessionmaker
from wallstreet import base


class StockInfoStorage(object):
    def save(self, stock_info):
        """
        save stock_info
        """
        raise NotImplementedError

    def load_all(self):
        """
        load all stock info
        """
        raise NotImplementedError


class StockDayStorage(object):
    def save(self, stock_day):
        """
        save stock_day
        """
        raise NotImplementedError

    def load(self, symbol, start_date=None, end_date=None):
        """
        load StockDay by symbol and date range
        """
        raise NotImplementedError

Base = declarative_base()


class SqlStorage(object):
    def __init__(self, url, shared_engine=None):
        if shared_engine:
            self.engine = shared_engine
        else:
            self.engine = create_engine(url)
        self.Session = sessionmaker(bind=self.engine)


class StockInfo(Base):
    __tablename__ = "stock_info"
    symbol = Column(String, primary_key=True)
    exchange = Column(String)


class StockInfoSqlStorage(StockInfoStorage, SqlStorage):
    def save(self, stock_info):
        session = self.Session()
        old_stock_info = session.query(StockInfo).filter_by(symbol=stock_info.symbol).first()
        if old_stock_info:
            old_stock_info.symbol = stock_info.symbol
            old_stock_info.exchange = stock_info.exchange
        else:
            session.add(StockInfo(symbol=stock_info.symbol, exchange=stock_info.exchange))
        session.commit()

    def load_all(self):
        session = self.Session()
        ret = []
        for t in session.query(StockInfo):
            ret.append(base.StockInfo(t.symbol, t.exchange))
        return ret


class StockDay(Base):
    __tablename__ = "stock_day"
    id = Column(Integer, primary_key=True)
    symbol = Column(String)
    date = Column(String)
    open = Column(Float)
    close = Column(Float)
    high = Column(Float)
    low = Column(Float)
    volume = Column(Integer)
    adj_factor = Column(Float)


class StockDaySqlStorage(StockDayStorage, SqlStorage):
    def save(self, stock_day):
        session = self.Session()
        old_stock_info = session.query(StockInfo).filter_by(symbol=stock_day.symbol, date=stock_day.date).first()
        if old_stock_info:
            for k, v in stock_day:
                setattr(old_stock_info, k, v)
        else:
            session.add(StockInfo(**stock_day.__dict__))
        session.commit()

    def load(self, symbol, start_date=None, end_date=None):
        session = self.Session()
        records = session.query(StockDay)\
            .filter(StockDay.symbol == symbol).filter(StockDay.date >= start_date).filter(StockDay <= end_date)
        ret = []
        for t in records:
            ret.append(base.StockDay(t.symbol, t.date, t.open, t.close, t.high, t.low, t.volume, t.adj_factor))
        return ret




