from __future__ import absolute_import
from wallstreet.storage import StockDaySqlStorage, StockInfoSqlStorage, LastUpdateSqlStorage, BaseIndexSqlStorage
from wallstreet.storage import LastUpdateStorage, RawYearFiscalReportSqlStorage, StockInfoDetailSqlStorage
from wallstreet.tasks.celery import app, engine, Session
from celery.utils.log import get_task_logger
from wallstreet.base import StockDay, StockInfo, BaseIndex, RawFiscalReport, StockInfoDetail
import traceback
from wallstreet import base
from dateutil.parser import parse
from collections import defaultdict

logger = get_task_logger(__name__)


@app.task
def load_all_stock_info():
    stock_info_storage = StockInfoSqlStorage(engine, Session)
    stock_infos = stock_info_storage.load_all()
    return [x.serializable_obj() for x in stock_infos]


@app.task
def save_stock_day(stock_days):
    stock_days = [StockDay.from_serializable_obj(x) for x in stock_days]
    if len(stock_days) > 0:
        last_update_date = max([x.date for x in stock_days])
        symbols = dict([(x.symbol, True) for x in stock_days])
        assert len(symbols) == 1
        stock_day_storage = StockDaySqlStorage(engine, Session)
        try:
            stock_day_storage.save(stock_days)
            last_update_storage = LastUpdateSqlStorage(engine, Session)
            last_update_storage.save(stock_days[0].symbol, last_update_date, LastUpdateStorage.STOCK_DAY)
        except Exception as exc:
            logger.error(traceback.format_exc())
            raise
        logger.debug("ok, symbol = {0}".format(stock_days[0].symbol))


@app.task
def load_last_update_date(symbol, date_type):
    storage = LastUpdateSqlStorage(engine, Session)
    t = storage.load(symbol, date_type)
    logger.debug("ok, symbol = {0}, date_type = {1}".format(symbol, date_type))
    return t is None and "1970-01-01" or t.strftime("%Y-%m-%d")


@app.task
def save_stock_info(stock_infos):
    stock_infos = [StockInfo.from_serializable_obj(x) for x in stock_infos]
    if len(stock_infos) > 0:
        stock_info_storage = StockInfoSqlStorage(engine, Session)
        try:
            stock_info_storage.save(stock_infos)
        except Exception as exc:
            logger.debug(traceback.format_exc())
            raise
        logger.debug("ok, exchange = {0}".format(stock_infos[0].exchange))


@app.task
def load_last_stock_days(symbol, limit, end_date):
    end_date = parse(end_date)
    storage = StockDaySqlStorage(engine, Session)
    stock_days = storage.load_last(symbol, limit, end_date)
    logger.debug("ok, symbol = {0}, total = {1}".format(symbol, len(stock_days)))
    return [x.serializable_obj() for x in stock_days]


@app.task
def compute_base_index(symbol, limit, last_update_date, end_date):
    last_update_date = parse(last_update_date)
    end_date = parse(end_date)
    storage = StockDaySqlStorage(engine, Session)
    stock_days = storage.load_last(symbol, limit, end_date)
    if len(stock_days) == 0:
        logger.debug("no stock day, symbol = {0}, last_update_date = {1}, limit = {2}".
                     format(symbol, last_update_date, limit))
        return
    stock_days.sort(key=lambda x: x.date, reverse=True)
    if len(stock_days) < 2:
        logger.debug("less then 2 days, symbol = {0}, last_update_date = {1}, limit = {2}".
                     format(symbol, last_update_date, limit))
        return
    last_update_index = len(stock_days) - 1
    for index, stock_day in enumerate(stock_days):
        if stock_day.date == last_update_date:
            last_update_index = index
            break
    base_indexs = []
    for index in range(last_update_index - 1, -1, -1):
        base_index = base.BaseIndex()
        t = stock_days[index:index + 60]
        base_index.update(t)
        base_indexs.append(base_index.serializable_obj())
    logger.debug("ok, symbol = {0}, last_update_date = {1}, limit = {2}".format(symbol, last_update_date, limit))
    save_stock_base_index(base_indexs)


@app.task
def save_stock_base_index(stock_base_indexs):
    stock_base_indexs = [BaseIndex.from_serializable_obj(x) for x in stock_base_indexs]
    if len(stock_base_indexs) > 0:
        last_update_date = max([x.date for x in stock_base_indexs])
        symbols = dict([(x.symbol, True) for x in stock_base_indexs])
        assert len(symbols) == 1
        storage = BaseIndexSqlStorage(engine, Session)
        try:
            storage.save(stock_base_indexs)
            last_update_storage = LastUpdateSqlStorage(engine, Session)
            last_update_storage.save(stock_base_indexs[0].symbol, last_update_date, LastUpdateStorage.STOCK_BASE_INDEX)
        except Exception as exc:
            logger.error(traceback.format_exc())
            raise
        logger.debug("ok, symbol = {0}".format(stock_base_indexs[0].symbol))


@app.task
def clear_stock(symbol):
    storage = StockDaySqlStorage(engine, Session)
    storage.delete(symbol)
    storage = BaseIndexSqlStorage(engine, Session)
    storage.delete(symbol)
    logger.debug("ok, symbol ={0}".find(symbol))


@app.task
def save_stock_year_fiscal(reports):
    reports = [RawFiscalReport.from_serializable_obj(x) for x in reports]
    if len(reports) > 0:
        counter = defaultdict(int)
        for report in reports:
            counter[report.symbol] += 1
        try:
            storage = RawYearFiscalReportSqlStorage(engine, Session)
            storage.save(reports)
        except Exception as exc:
            logger.error(traceback.format_exc())
            raise
        logger.debug("ok,{0}".format(counter))


@app.task
def load_symbols_has_no_year_fiscal_report():
    fiscal_storage = RawYearFiscalReportSqlStorage(engine, Session)
    symbols = fiscal_storage.load_symbols()
    symbols = set(symbols)
    stock_info_storage = StockInfoSqlStorage(engine, Session)
    stock_infos = stock_info_storage.load_all()
    ret = []
    for stock_info in stock_infos:
        if stock_info.symbol not in symbols:
            ret.append(stock_info.symbol)
    return ret


@app.task
def load_symbols_has_no_stock_info_details():
    detail_storage = StockInfoDetailSqlStorage(engine, Session)
    details = detail_storage.load_all()
    symbols = set([x.symbol for x in details])
    stock_info_storage = StockInfoSqlStorage(engine, Session)
    stock_infos = stock_info_storage.load_all()
    ret = []
    for stock_info in stock_infos:
        if stock_info.symbol not in symbols:
            ret.append(stock_info.symbol)
    return ret


@app.task
def save_stock_info_detail(details):
    details = [StockInfoDetail.from_serializable_obj(x) for x in details]
    if len(details) > 0:
        try:
            storage = StockInfoDetailSqlStorage(engine, Session)
            storage.save(details)
        except Exception as exc:
            logger.error(traceback.format_exc())
            raise
        logger.debug("ok")