"""
storage
"""
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime, Index
from sqlalchemy.orm import sessionmaker
from wallstreet import base


class StockInfoStorage(object):
    def save(self, stock_infos):
        """
        save stock_info
        :parameter stock_infos:StockInfo object or list
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

    def load_last(self, symbol, limit):
        """
        load last limit record
        :param symbol: stock symbol
        :param limit: last limit, sort from recently to far away
        :return:
        """
        raise NotImplementedError


class LastUpdateStorage(object):
    STOCK_DAY = 1
    """
    storage about last update date
    """
    def load_stock_day(self, symbol):
        """
        get last update date of StockDay
        :parameter symbol:stock symbok
        :return: last update date of StockDay
        """
        raise NotImplementedError

    def save_stock_day(self, symbol, last_update_date):
        """
        set last update date of StockDay
        :parameter last_update_date: last update date of StockDay
        :parameter symbol: stock symbol
        """
        raise NotImplementedError


class BaseIndexStorage(object):
    def save(self, symbol, date, base_index):
        raise NotImplementedError

    def load(self, symbol, date):
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
                for k, v in stock_info.__dict__.items():
                    setattr(old_stock_info, k, v)
            else:
                session.add(StockInfo(symbol=stock_info.symbol, exchange=stock_info.exchange))
        session.commit()

    def load(self, symbol):
        session = self.Session()
        t = session.query(StockInfo).filter(StockInfo.symbol == symbol).first()
        stock_info = t and base.StockInfo(t.symbol, t.exchange) or None
        session.commit()
        return stock_info

    def load_all(self):
        session = self.Session()
        ret = []
        for t in session.query(StockInfo):
            ret.append(base.StockInfo(t.symbol, t.exchange))
        session.commit()
        return ret


class StockDay(Base):
    __tablename__ = "stock_day"
    id = Column(Integer, primary_key=True)
    symbol = Column(String(32))
    date = Column(DateTime)
    open = Column(Float)
    close = Column(Float)
    high = Column(Float)
    low = Column(Float)
    volume = Column(Integer)
    adj_factor = Column(Float)
    __table_args__ = (Index("symbol_date_index", "symbol", "date"), )


class StockDaySqlStorage(StockDayStorage, SqlStorage):
    def save(self, stock_days):
        if not isinstance(stock_days, list):
            stock_days = [stock_days]
        session = self.Session()
        for stock_day in stock_days:
            #old_stock_day = session.query(StockDay).\
            #    filter(StockDay.symbol == stock_day.symbol).filter(StockDay.date == stock_day.date).first()
            #if old_stock_day:
            #    for k, v in stock_day.__dict__.items():
            #        setattr(old_stock_day, k, v)
            #else:
            #    session.add(StockDay(**stock_day.__dict__))
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
        session.commit()
        return ret

    def load_last(self, symbol, limit):
        session = self.Session()
        records = session.query(StockDay).filter(StockDay.symbol == symbol).order_by(StockDay.date.desc()).limit(limit)
        ret = []
        for t in records:
            ret.append(base.StockDay(t.symbol, t.date, t.open, t.close, t.high, t.low, t.volume, t.adj_factor))
        session.commit()
        return ret


class LastUpdate(Base):
    __tablename__ = "last_update"
    id = Column(Integer, primary_key=True)
    symbol = Column(String(32))
    data_type = Column(Integer)
    last_update_date = Column(DateTime)
    __table_args__ = (Index("symbol_date_type_index", "symbol", "data_type"), )


class LastUpdateSqlStorage(LastUpdateStorage, SqlStorage):

    def load_stock_day(self, symbol):
        session = self.Session()
        t = session.query(LastUpdate).filter(LastUpdate.symbol == symbol)\
            .filter(LastUpdate.data_type == LastUpdateStorage.STOCK_DAY).first()
        if t is None:
            last_update_date = None
        else:
            last_update_date = t.last_update_date
        session.commit()
        return last_update_date

    def save_stock_day(self, symbol, last_update_date):
        session = self.Session()
        t = session.query(LastUpdate).filter(LastUpdate.symbol == symbol)\
            .filter(LastUpdate.data_type == LastUpdateStorage.STOCK_DAY).first()
        if t:
            t.last_update_date = last_update_date
        else:
            session.add(LastUpdate(symbol=symbol, data_type=LastUpdateStorage.STOCK_DAY,
                                   last_update_date=last_update_date))
        session.commit()


def create_sql_table(engine):
    Base.metadata.create_all(engine)


def drop_sql_table(engine):
    Base.metadata.drop_all(engine)
