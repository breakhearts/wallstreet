"""
storage
"""
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.orm import sessionmaker
from wallstreet import base


class StockInfoStorage(object):
    def save(self, stock_infos):
        """
        save stock_info
        :parm stock_infos:StockInfo object or list
        """
        raise NotImplementedError

    def load_all(self):
        """
        load all stock info
        """
        raise NotImplementedError


class StockDayStorage(object):
    def save(self, stock_days):
        """
        save stock_day
        :parm stock_days:StockDay object or list
        """
        raise NotImplementedError

    def load(self, symbol, start_date=None, end_date=None):
        """
        load StockDay by symbol and date range
        """
        raise NotImplementedError

Base = declarative_base()


def create_sql_engine_and_session_cls(url):
    engine = create_engine(url)
    session_cls = sessionmaker(engine)
    return engine, session_cls


class SqlStorage(object):
    def __init__(self, shared_engine, session_cls):
        self.engine = shared_engine
        self.Session = session_cls


class StockInfo(Base):
    __tablename__ = "stock_info"
    symbol = Column(String(32), primary_key=True)
    exchange = Column(String(32))


class StockInfoSqlStorage(StockInfoStorage, SqlStorage):
    def save(self, stock_infos):
        if not isinstance(stock_infos, list):
            stock_infos = [stock_infos]
        session = self.Session()
        for stock_info in stock_infos:
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
    symbol = Column(String(32))
    date = Column(String(10))
    open = Column(Float)
    close = Column(Float)
    high = Column(Float)
    low = Column(Float)
    volume = Column(Integer)
    adj_factor = Column(Float)


class StockDaySqlStorage(StockDayStorage, SqlStorage):
    def save(self, stock_days):
        if not isinstance(stock_days, list):
            stock_days = [stock_days]
        session = self.Session()
        for stock_day in stock_days:
            old_stock_day = session.query(StockDay).\
                filter(StockDay.symbol == stock_day.symbol).filter(StockDay.date == stock_day.date).first()
            if old_stock_day:
                for k, v in stock_day:
                    setattr(old_stock_day, k, v)
            else:
                session.add(StockDay(**stock_day.__dict__))
        session.commit()

    def load(self, symbol, start_date=None, end_date=None):
        session = self.Session()
        records = session.query(StockDay).filter(StockDay.symbol == symbol)
        if start_date:
            records = records.filter(StockDay.date >= start_date)
        if end_date:
            records = records.filter(StockDay.date <= end_date)
        ret = []
        for t in records:
            ret.append(base.StockDay(t.symbol, t.date, t.open, t.close, t.high, t.low, t.volume, t.adj_factor))
        return ret


def create_sql_table(engine):
    Base.metadata.create_all(engine)


def drop_sql_table(engine):
    Base.metadata.drop_all(engine)

