from __future__ import absolute_import
from wallstreet.storage import StockDaySqlStorage, StockInfoSqlStorage, LastUpdateSqlStorage, BaseIndexSqlStorage
from wallstreet.storage import LastUpdateStorage, RawYearFiscalReportSqlStorage, StockInfoDetailSqlStorage
from wallstreet.storage import SECFillingSqlStorage
from wallstreet.tasks.celery import app, engine_Session, RecordFailureTask
from celery.utils.log import get_task_logger
from wallstreet.base import StockDay, StockInfo, BaseIndex, RawFiscalReport, StockInfoDetail
import traceback
from wallstreet import base
from dateutil.parser import parse
from collections import defaultdict

logger = get_task_logger(__name__)


@app.task(base=RecordFailureTask)
def load_all_stock_symbols():
    stock_info_storage = StockInfoSqlStorage(engine_Session.engine, engine_Session.Session)
    stock_infos = stock_info_storage.load_all()
    return [x.symbol for x in stock_infos]


@app.task(base=RecordFailureTask)
def save_stock_day(stock_days):
    stock_days = [StockDay.from_serializable_obj(x) for x in stock_days]
    if len(stock_days) > 0:
        last_update_date = max([x.date for x in stock_days])
        symbols = dict([(x.symbol, True) for x in stock_days])
        assert len(symbols) == 1
        stock_day_storage = StockDaySqlStorage(engine_Session.engine, engine_Session.Session)
        try:
            stock_day_storage.save(stock_days)
            last_update_storage = LastUpdateSqlStorage(engine_Session.engine, engine_Session.Session)
            last_update_storage.save(stock_days[0].symbol, last_update_date, LastUpdateStorage.STOCK_DAY)
        except Exception as exc:
            logger.error(traceback.format_exc())
            raise
        logger.debug("ok, symbol = {0}".format(stock_days[0].symbol))


@app.task(base=RecordFailureTask)
def load_last_update_date(symbol, date_type):
    storage = LastUpdateSqlStorage(engine_Session.engine, engine_Session.Session)
    t = storage.load(symbol, date_type)
    logger.debug("ok, symbol = {0}, date_type = {1}".format(symbol, date_type))
    return t is None and "1970-01-01" or t.strftime("%Y-%m-%d")


@app.task(base=RecordFailureTask)
def save_stock_info(stock_infos):
    stock_infos = [StockInfo.from_serializable_obj(x) for x in stock_infos]
    if len(stock_infos) > 0:
        stock_info_storage = StockInfoSqlStorage(engine_Session.engine, engine_Session.Session)
        try:
            stock_info_storage.save(stock_infos)
        except Exception as exc:
            logger.debug(traceback.format_exc())
            raise
        logger.debug("ok, exchange = {0}".format(stock_infos[0].exchange))


@app.task(base=RecordFailureTask)
def load_last_stock_days(symbol, limit, end_date):
    end_date = parse(end_date)
    storage = StockDaySqlStorage(engine_Session.engine, engine_Session.Session)
    stock_days = storage.load_last(symbol, limit, end_date)
    logger.debug("ok, symbol = {0}, total = {1}".format(symbol, len(stock_days)))
    return [x.serializable_obj() for x in stock_days]


@app.task(base=RecordFailureTask)
def compute_base_index(symbol, limit, last_update_date, end_date):
    last_update_date = parse(last_update_date)
    end_date = parse(end_date)
    storage = StockDaySqlStorage(engine_Session.engine, engine_Session.Session)
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


@app.task(base=RecordFailureTask)
def save_stock_base_index(stock_base_indexs):
    stock_base_indexs = [BaseIndex.from_serializable_obj(x) for x in stock_base_indexs]
    if len(stock_base_indexs) > 0:
        last_update_date = max([x.date for x in stock_base_indexs])
        symbols = dict([(x.symbol, True) for x in stock_base_indexs])
        assert len(symbols) == 1
        storage = BaseIndexSqlStorage(engine_Session.engine, engine_Session.Session)
        try:
            storage.save(stock_base_indexs)
            last_update_storage = LastUpdateSqlStorage(engine_Session.engine, engine_Session.Session)
            last_update_storage.save(stock_base_indexs[0].symbol, last_update_date, LastUpdateStorage.STOCK_BASE_INDEX)
        except Exception as exc:
            logger.error(traceback.format_exc())
            raise
        logger.debug("ok, symbol = {0}".format(stock_base_indexs[0].symbol))


@app.task(base=RecordFailureTask)
def clear_stock(symbol):
    storage = StockDaySqlStorage(engine_Session.engine, engine_Session.Session)
    storage.delete(symbol)
    storage = BaseIndexSqlStorage(engine_Session.engine, engine_Session.Session)
    storage.delete(symbol)
    logger.debug("ok, symbol ={0}".format(symbol))


@app.task(base=RecordFailureTask)
def save_stock_year_fiscal(reports):
    reports = [RawFiscalReport.from_serializable_obj(x) for x in reports]
    if len(reports) > 0:
        counter = defaultdict(int)
        for report in reports:
            counter[report.symbol] += 1
        try:
            storage = RawYearFiscalReportSqlStorage(engine_Session.engine, engine_Session.Session)
            storage.save(reports)
        except Exception as exc:
            logger.error(traceback.format_exc())
            raise
        logger.debug("ok,{0}".format(counter))


@app.task(base=RecordFailureTask)
def load_symbols_has_no_year_fiscal_report():
    fiscal_storage = RawYearFiscalReportSqlStorage(engine_Session.engine, engine_Session.Session)
    symbols = fiscal_storage.load_symbols()
    symbols = set(symbols)
    stock_info_storage = StockInfoSqlStorage(engine_Session.engine, engine_Session.Session)
    stock_infos = stock_info_storage.load_all()
    ret = []
    for stock_info in stock_infos:
        if stock_info.symbol not in symbols:
            ret.append(stock_info.symbol)
    return ret


@app.task(base=RecordFailureTask)
def load_symbols_has_no_stock_info_details():
    detail_storage = StockInfoDetailSqlStorage(engine_Session.engine, engine_Session.Session)
    details = detail_storage.load_all()
    symbols = set([x.symbol for x in details])
    stock_info_storage = StockInfoSqlStorage(engine_Session.engine, engine_Session.Session)
    stock_infos = stock_info_storage.load_all()
    ret = []
    for stock_info in stock_infos:
        if stock_info.symbol not in symbols:
            ret.append(stock_info.symbol)
    return ret


@app.task(base=RecordFailureTask)
def save_stock_info_detail(details):
    details = [StockInfoDetail.from_serializable_obj(x) for x in details]
    if len(details) > 0:
        try:
            storage = StockInfoDetailSqlStorage(engine_Session.engine, engine_Session.Session)
            storage.save(details)
        except Exception as exc:
            logger.error(traceback.format_exc())
            raise
        logger.debug("ok")


@app.task(base=RecordFailureTask)
def save_stock_fillings_idx(fillings):
    fillings = [base.SECFilling.from_serializable_obj(x) for x in fillings]
    if len(fillings) > 0:
        try:
            storage = SECFillingSqlStorage(engine_Session.engine, engine_Session.Session)
            storage.save(fillings)
            logger.debug("ok")
        except Exception as exc:
            logger.error(traceback.format_exc())
            raise


@app.task(base=RecordFailureTask)
def load_sec_fillings_idx(symbol, form_type=None, start_date=None, end_date=None):
    storage = StockInfoDetailSqlStorage(engine_Session.engine, engine_Session.Session)
    detail = storage.load(symbol)
    if detail is None:
        return []
    if start_date:
        start_date = parse(start_date)
    if end_date:
        end_date = parse(end_date)
    cik = detail.cik.lstrip("0")
    storage = SECFillingSqlStorage(engine_Session.engine, engine_Session.Session)
    fillings = storage.load(cik, form_type, start_date, end_date)
    ret = [x.serializable_obj() for x in fillings]
    return ret