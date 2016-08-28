"""
storage
"""
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, Text, DateTime, Index
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

    def load_last(self, symbol, limit, end_date=None):
        """
        load last limit record
        :param symbol: stock symbol
        :param limit: last limit, sort from recently to far away
        :param end_date
        :return:
        """
        raise NotImplementedError

    def delete(self, symbol, start_date=None, end_date=None):
        """
        :param symbol:
        :param start_date:
        :param end_date:
        :return:
        """
        raise NotImplementedError


class LastUpdateStorage(object):
    STOCK_DAY = 1
    STOCK_BASE_INDEX = 2
    """
    storage about last update date
    """
    def load(self, symbol, data_type):
        """
        get last update date of StockDay
        :parameter symbol:stock symbok
        :parameter data_type
        :return: last update date of StockDay
        """
        raise NotImplementedError

    def save(self, symbol, last_update_date, data_type):
        """
        set last update date of StockDay
        :parameter last_update_date: last update date of StockDay
        :parameter symbol: stock symbol
        :parameter data_type
        """
        raise NotImplementedError


class BaseIndexStorage(object):
    def save(self, base_index):
        raise NotImplementedError

    def load(self, symbol, start_date=None, end_date=None):
        raise NotImplementedError

    def load_last(self, symbol, limit):
        raise NotImplementedError

    def delete(self, symbol, start_date=None, end_date=None):
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
        try:
            for stock_info in stock_infos:
                old_stock_info = session.query(StockInfo).filter_by(symbol=stock_info.symbol).first()
                if old_stock_info:
                    for k, v in stock_info.__dict__.items():
                        setattr(old_stock_info, k, v)
                else:
                    session.add(StockInfo(symbol=stock_info.symbol, exchange=stock_info.exchange))
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def load(self, symbol):
        session = self.Session()
        t = session.query(StockInfo).filter(StockInfo.symbol == symbol).first()
        try:
            stock_info = t and base.StockInfo(t.symbol, t.exchange) or None
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
        return stock_info

    def load_all(self):
        ret = []
        session = self.Session()
        try:
            for t in session.query(StockInfo):
                ret.append(base.StockInfo(t.symbol, t.exchange))
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
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
    __table_args__ = (Index("symbol_date_index", "symbol", "date", unique=True), )


class StockDaySqlStorage(StockDayStorage, SqlStorage):
    def save(self, stock_days):
        if not isinstance(stock_days, list):
            stock_days = [stock_days]
        session = self.Session()
        try:
            for stock_day in stock_days:
                session.add(StockDay(**stock_day.__dict__))
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def load(self, symbol, start_date=None, end_date=None):
        session = self.Session()
        try:
            records = session.query(StockDay).filter(StockDay.symbol == symbol)
            if start_date:
                records = records.filter(StockDay.date >= start_date)
            if end_date:
                records = records.filter(StockDay.date <= end_date)
            ret = []
            for t in records:
                ret.append(base.StockDay(t.symbol, t.date, t.open, t.close, t.high, t.low, t.volume, t.adj_factor))
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
        return ret

    def load_last(self, symbol, limit, end_date=None):
        session = self.Session()
        ret = []
        try:
            if end_date:
                records = session.query(StockDay).filter(StockDay.symbol == symbol).filter(StockDay.date <= end_date)\
                    .order_by(StockDay.date.desc()).limit(limit)
            else:
                records = session.query(StockDay).filter(StockDay.symbol == symbol).order_by(StockDay.date.desc()).limit(limit)
            for t in records:
                ret.append(base.StockDay(t.symbol, t.date, t.open, t.close, t.high, t.low, t.volume, t.adj_factor))
            session.commit()
        except:
            session.rollback()
        finally:
            session.close()
        return ret

    def delete(self, symbol, start_date=None, end_date=None):
        session = self.Session()
        try:
            records = session.query(StockDay).filter(StockDay.symbol == symbol)
            if start_date:
                records = records.filter(StockDay.date >= start_date)
            if end_date:
                records = records.filter(StockDay.date <= end_date)
            for t in records:
                session.delete(t)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()


class LastUpdate(Base):
    __tablename__ = "last_update"
    id = Column(Integer, primary_key=True)
    symbol = Column(String(32))
    data_type = Column(Integer)
    last_update_date = Column(DateTime)
    __table_args__ = (Index("symbol_date_type_index", "symbol", "data_type", unique=True), )


class LastUpdateSqlStorage(LastUpdateStorage, SqlStorage):

    def __save(self, symbol, data_type, last_update_date):
        session = self.Session()
        try:
            t = session.query(LastUpdate).filter(LastUpdate.symbol == symbol)\
                .filter(LastUpdate.data_type == data_type).first()
            if t:
                t.last_update_date = last_update_date
            else:
                session.add(LastUpdate(symbol=symbol, data_type=data_type,
                                       last_update_date=last_update_date))
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def __load(self, symbol, data_type):
        session = self.Session()
        try:
            t = session.query(LastUpdate).filter(LastUpdate.symbol == symbol)\
                .filter(LastUpdate.data_type == data_type).first()
            if t is None:
                last_update_date = None
            else:
                last_update_date = t.last_update_date
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
        return last_update_date

    def load(self, symbol, data_type):
        return self.__load(symbol, data_type)

    def save(self, symbol, last_update_date, data_type):
        self.__save(symbol, data_type, last_update_date)


class BaseIndex(Base):
    __tablename__ = "base_index"
    id = Column(Integer, primary_key=True)
    symbol = Column(String(32))
    date = Column(DateTime)
    change = Column(Float)
    ma5 = Column(Float)
    ma20 = Column(Float)
    ma60 = Column(Float)
    vol5 = Column(Integer)
    vol20 = Column(Integer)
    vol60 = Column(Integer)
    __table_args__ = (Index("symbol_date_index", "symbol", "date", unique=True), )


class BaseIndexSqlStorage(BaseIndexStorage, SqlStorage):
    def save(self, base_indexs):
        if not isinstance(base_indexs, list):
            base_indexs = [base_indexs]
        session = self.Session()
        try:
            for base_index in base_indexs:
                session.add(BaseIndex(**base_index.__dict__))
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def load(self, symbol, start_date=None, end_date=None):
        session = self.Session()
        try:
            records = session.query(BaseIndex).filter(BaseIndex.symbol == symbol)
            if start_date:
                records = records.filter(BaseIndex.date >= start_date)
            if end_date:
                records = records.filter(BaseIndex.date <= end_date)
            ret = []
            for t in records:
                ret.append(base.BaseIndex(t.symbol, t.date, t.change, t.ma5, t.ma20, t.ma60, t.vol5, t.vol20, t.vol60))
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
        return ret

    def load_last(self, symbol, limit):
        session = self.Session()
        try:
            records = session.query(BaseIndex).filter(BaseIndex.symbol == symbol).order_by(BaseIndex.date.desc()).limit(limit)
            ret = []
            for t in records:
                ret.append(base.BaseIndex(t.symbol, t.date, t.change, t.ma5, t.ma20, t.ma60, t.vol5, t.vol20, t.vol60))
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
        return ret

    def delete(self, symbol, start_date=None, end_date=None):
        session = self.Session()
        try:
            records = session.query(BaseIndex).filter(BaseIndex.symbol == symbol)
            if start_date:
                records = records.filter(StockDay.date >= start_date)
            if end_date:
                records = records.filter(StockDay.date <= end_date)
            for t in records:
                session.delete(t)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()


class RawYearFiscalReport(Base):
    __tablename__ = "raw_year_fiscal_report"
    id = Column(Integer, primary_key=True)
    symbol = Column(String(32))
    fiscal_period = Column(String(6))
    content = Column(Text)
    __table_args__ = (Index("symbol_period_index", "symbol", "fiscal_period", unique=True),)


class RawYearFiscalReportStorage(object):
    def save(self, reports):
        raise NotImplementedError

    def load(self, symbol, start_period=None, end_period=None):
        raise NotImplementedError

    def load_symbols(self):
        raise NotImplementedError


class RawYearFiscalReportSqlStorage(RawYearFiscalReportStorage, SqlStorage):
    def save(self, reports):
        if not isinstance(reports, list):
            reports = [reports]
        session = self.Session()
        try:
            for report in reports:
                session.add(RawYearFiscalReport(symbol=report.symbol, fiscal_period=report.fiscal_period,
                                                content=report.content))
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def load(self, symbol, start_period=None, end_period=None):
        session = self.Session()
        try:
            records = session.query(RawYearFiscalReport).filter(RawYearFiscalReport.symbol == symbol)
            if start_period is not None:
                records = records.filter(RawYearFiscalReport.fiscal_period >=start_period)
            if end_period is not None:
                records = records.filter(RawYearFiscalReport.fiscal_period <= end_period)
            ret = []
            for t in records:
                ret.append(base.RawFiscalReport(symbol=t.symbol, fiscal_period=t.fiscal_period, content=t.content))
        except:
            session.rollback()
            raise
        finally:
            session.close()
        return ret

    def load_symbols(self):
        session = self.Session()
        try:
            records = session.query(RawYearFiscalReport.symbol).group_by(RawYearFiscalReport.symbol).all()
        except:
            session.rollback()
            raise
        finally:
            session.close()
        return [x[0] for x in records]


class RawQuarterFiscalReport(Base):
    __tablename__ = "raw_quarter_fiscal_report"
    id = Column(Integer, primary_key=True)
    symbol = Column(String(32))
    fiscal_period = Column(String(6))
    content = Column(Text)
    __table_args__ = (Index("symbol_period_index", "symbol", "fiscal_period", unique=True),)


class RawQuarterFiscalReportStorage(object):
    def save(self, reports):
        raise NotImplementedError

    def load(self, symbol, start_period=None, end_period=None):
        raise NotImplementedError

    def load_symbols(self):
        raise NotImplementedError


class RawQuarterFiscalReportSqlStorage(RawQuarterFiscalReportStorage, SqlStorage):
    def save(self, reports):
        if not isinstance(reports, list):
            reports = [reports]
        session = self.Session()
        try:
            for report in reports:
                session.add(RawQuarterFiscalReport(symbol=report.symbol, fiscal_period=report.fiscal_period,
                                                content=report.content))
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def load(self, symbol, start_period=None, end_period=None):
        session = self.Session()
        try:
            records = session.query(RawQuarterFiscalReport).filter(RawQuarterFiscalReport.symbol == symbol)
            if start_period is not None:
                records = records.filter(RawQuarterFiscalReport.fiscal_period >=start_period)
            if end_period is not None:
                records = records.filter(RawQuarterFiscalReport.fiscal_period <= end_period)
            ret = []
            for t in records:
                ret.append(base.RawFiscalReport(symbol=t.symbol, fiscal_period=t.fiscal_period, content=t.content))
        except:
            session.rollback()
            raise
        finally:
            session.close()
        return ret

    def load_symbols(self):
        session = self.Session()
        try:
            records = session.query(RawQuarterFiscalReport.symbol).group_by(RawQuarterFiscalReport.symbol).all()
        except:
            session.rollback()
            raise
        finally:
            session.close()
        return [x[0] for x in records]


class StockInfoDetail(Base):
    __tablename__ = "stock_info_details"
    symbol = Column(String(32), primary_key=True)
    cik = Column(String(32))
    exchange = Column(String(32))
    industry = Column(String(32))
    sector = Column(String(32))
    siccode = Column(String(16))
    city = Column(String(32))


class StockInfoDetailStorage(object):
    def save(self, stock_info):
        raise NotImplementedError

    def load(self, symbol):
        raise NotImplementedError

    def load_all(self):
        raise NotImplementedError


class StockInfoDetailSqlStorage(StockInfoDetailStorage, SqlStorage):
    def save(self, stock_info_details):
        if not isinstance(stock_info_details, list):
            stock_info_details = [stock_info_details]
        session = self.Session()
        try:
            for t in stock_info_details:
                session.add(StockInfoDetail(symbol=t.symbol, cik=t.cik, exchange=t.exchange, industry=t.industry,
                                                 sector=t.sector, siccode=t.siccode, city=t.city))
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def load_all(self):
        session = self.Session()
        try:
            ret = []
            for t in session.query(StockInfoDetail):
                ret.append(StockInfoDetail(symbol=t.symbol, cik=t.cik, exchange=t.exchange, industry=t.industry,
                                                 sector=t.sector, siccode=t.siccode, city=t.city))
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
        return ret

    def load(self, symbol):
        session = self.Session()
        try:
            t = session.query(StockInfoDetail).filter(StockInfoDetail.symbol == symbol).first()
            if t is None:
                ret = None
            else:
                ret = base.StockInfoDetail(symbol=t.symbol, cik=t.cik, exchange=t.exchange, industry=t.industry,
                                     sector=t.sector, siccode=t.siccode, city=t.city)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
        return ret


class SECFillingStorage(object):
    def save(self, sec_filling):
        raise NotImplementedError

    def load(self, cik, start_date=None, end_date=None):
        raise NotImplementedError


class SECFilling(Base):
    __tablename__ = "sec_fillings"
    url = Column(String(128), primary_key=True)
    company_name = Column(String(64))
    cik = Column(String(16))
    date = Column(DateTime)
    form_type = Column(String(16))
    __table_args__ = (
        Index("cik_date", "cik", "date", unique=False),
    )


class SECFillingSqlStorage(SECFillingStorage, SqlStorage):
    def load(self, cik, form_type=None, start_date=None, end_date=None):
        session = self.Session()
        try:
            records = session.query(SECFilling).filter(SECFilling.cik == cik)
            if start_date is not None:
                records = records.filter(SECFilling.date >= start_date)
            if end_date is not None:
                records = records.filter(SECFilling.date <= end_date)
            if form_type is not None:
                records = records.filter(SECFilling.form_type == form_type)
            ret = []
            for t in records:
                ret.append(base.SECFilling(company_name=t.company_name, cik=t.cik, form_type=t.form_type,
                                           date=t.date, url=t.url))
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
        return ret

    def save(self, sec_fillings):
        session = self.Session()
        if not isinstance(sec_fillings, list):
            sec_fillings = [sec_fillings]
        try:
            for t in sec_fillings:
                if not t.url.endswith('htm'):
                    print(t.url)
                session.add(SECFilling(company_name=t.company_name, cik=t.cik, form_type=t.form_type,
                                       date=t.date, url=t.url))
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()


def create_sql_table(engine):
    Base.metadata.create_all(engine)


def drop_sql_table(engine):
    Base.metadata.drop_all(engine)
