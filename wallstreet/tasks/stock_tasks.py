from __future__ import absolute_import
from wallstreet.crawler.fetcher import RequestsFetcher
from wallstreet.crawler import sec
from wallstreet.crawler.stockapi import YahooHistoryDataAPI, NasdaqStockInfoAPI, EdgarYearReportAPI, EdgarCompanyAPI
from wallstreet.tasks.celery import app, RecordFailureTask
from wallstreet import base
from wallstreet.tasks.storage_tasks import save_stock_day, load_all_stock_info, save_stock_info
from wallstreet.tasks.storage_tasks import load_last_update_date, save_stock_year_fiscal
from wallstreet.tasks.storage_tasks import compute_base_index, clear_stock, load_symbols_has_no_year_fiscal_report
from wallstreet.tasks.storage_tasks import load_symbols_has_no_stock_info_details, save_stock_info_detail
from wallstreet.tasks.storage_tasks import save_stock_fillings_idx, load_sec_fillings_idx
from celery.utils.log import get_task_logger
from wallstreet.storage import LastUpdateStorage
from celery.exceptions import Ignore
from dateutil.parser import parse
from wallstreet import config
import traceback

logger = get_task_logger(__name__)


@app.task(base=RecordFailureTask)
def update_all_stock_info():
    update_stock_info.apply_async(("NASDAQ",), link=save_stock_info.s())
    update_stock_info.apply_async(("NYSE",), link=save_stock_info.s())
    update_stock_info.apply_async(("AMEX",), link=save_stock_info.s())
    logger.debug("update all ok")


@app.task(base=RecordFailureTask, bind=True, max_retries=10, default_retry_delay=30)
def update_stock_info(self, exchange):
    api = NasdaqStockInfoAPI()
    url, method, headers, data = api.get_url_params(exchange)
    fetcher = RequestsFetcher()
    try:
        status_code, content = fetcher.fetch(url, method, headers, data)
        if status_code != 200:
            logger.debug("status_code={0}".format(status_code))
            raise self.retry()
        ret = api.parse_ret(exchange, content)
        logger.debug("ok, exchange={0}, total={1}".format(exchange, len(ret)))
        return [x.serializable_obj() for x in ret]
    except Exception as exc:
        logger.error("exchange = {0}, exc = {1}".format(exchange, traceback.format_exc()))
        raise self.retry(exc=exc)


@app.task(base=RecordFailureTask)
def update_all_stock_day():
    load_all_stock_info.apply_async(link=get_all_stock_history.s())


@app.task(base=RecordFailureTask)
def get_all_stock_history(stocks):
    stocks = [base.StockInfo.from_serializable_obj(x) for x in stocks]
    for stock_info in stocks:
        load_last_update_date.apply_async((stock_info.symbol, LastUpdateStorage.STOCK_DAY),
                                          link=update_stock_history.s(stock_info.symbol))
    logger.debug("ok, len = {0}".format(len(stocks)))


@app.task(base=RecordFailureTask)
def update_stock_history(last_update_date, symbol):
    if base.get_last_after_hour_date() == last_update_date:
        logger.debug("fresh data, no need update, symbol={0}".format(symbol))
        return
    if last_update_date is not None:
        last_update_date = parse(last_update_date)
        get_stock_history.apply_async((symbol, base.get_next_day_str(last_update_date), None, True,
                                       base.get_day_str(last_update_date)), link=save_stock_day.s())
    else:
        get_stock_history.apply_async((symbol, None, None, None, False, None), link=save_stock_day.s())


@app.task(base=RecordFailureTask, bind=True, max_retries=3, default_retry_delay=30)
def get_stock_history(self, symbol, start_date=None, end_date=None, check_dividend=False, last_no_dividend=None,
                      timeout=config.get("fetcher", "timeout")):
    api = YahooHistoryDataAPI()
    if check_dividend and last_no_dividend != start_date:
        assert last_no_dividend is not None
        real_start_date = min(start_date, last_no_dividend)
    else:
        real_start_date = start_date
    url, method, headers, data = api.get_url_params(symbol, real_start_date, end_date)
    fetcher = RequestsFetcher(timeout=timeout)
    #task_counter.new("HISTORY_TASKS")
    try:
        status_code, content = fetcher.fetch(url, method, headers, data)
        if status_code != 200:
            logger.debug("status_code={0}".format(status_code))
            if status_code == 404:
                raise Ignore()
            else:
                raise self.retry()
        ret = api.parse_ret(symbol, content)
        if check_dividend and last_no_dividend != start_date:
            ret.sort(key=lambda x: x.date)
            start_index = len(ret)
            for index, t in enumerate(ret):
                if t.date.strftime("%Y%m%d") == last_no_dividend:
                    if t.adj_factor != 1.0:
                        logger.debug("found dividend, symbol={0}".format(symbol))
                        clear_stock.apply_async((symbol, ),
                                                link=get_stock_history.s(symbol, None, None, False, None, timeout))
                        return get_stock_history((symbol,))
                    if real_start_date < last_no_dividend:
                        break
                if t.date.strftime("%Y%m%d") >= start_date:
                    start_index = index
                    if real_start_date < start_date:
                        break
            ret = ret[start_index:]
        logger.debug("ok, symbol={0}, total={1}".format(symbol, len(ret)))
        #task_counter.succeeded("HISTORY_TASKS")
    except Exception as exc:
        if isinstance(exc, Ignore):
            raise exc
        else:
            logger.error("symbol = {0}, exc = {1}".format(symbol, traceback.format_exc()))
            raise self.retry(exc=exc, timeout=config.get("fetcher", "timeout") * min(self.request.retries+1, 5))
    return [x.serializable_obj() for x in ret]


@app.task(base=RecordFailureTask)
def update_all_stock_base_index():
    load_all_stock_info.apply_async(link=get_all_stock_base_index.s())


@app.task(base=RecordFailureTask)
def get_all_stock_base_index(stocks):
    stocks = [base.StockInfo.from_serializable_obj(x) for x in stocks]
    for stock_info in stocks:
        load_last_update_date.apply_async((stock_info.symbol, LastUpdateStorage.STOCK_BASE_INDEX),
                                          link=update_stock_base_index.s(stock_info.symbol))
    logger.debug("ok")


@app.task(base=RecordFailureTask)
def update_stock_base_index(last_update_date, symbol):
    last_update_date = parse(last_update_date)
    last_after_hour_date = base.get_last_after_hour_date()
    if last_after_hour_date == last_update_date:
        logger.debug("fresh data, no need update, symbol = {0}".format(symbol))
        return
    fetch_days = (last_after_hour_date - last_update_date).days - 1 + 60
    compute_base_index.apply_async((symbol, fetch_days, last_update_date.strftime("%Y-%m-%d"),
                                    last_after_hour_date.strftime("%Y-%m-%d")))
    logger.debug("ok, symbol = {0}, last_update_date = {1}".format(symbol, last_update_date))


@app.task(base=RecordFailureTask)
def update_all_year_fiscal_report():
    load_symbols_has_no_year_fiscal_report.apply_async(link=get_all_stock_year_fiscal.s())


@app.task(base=RecordFailureTask)
def get_all_stock_year_fiscal(symbols):
    logger.debug("len = {0}".format(len(symbols)))
    batch_size = 5
    t = []
    for index, symbol in enumerate(symbols):
        t.append(symbol)
        if len(t) % batch_size == 0:
            get_stock_year_fiscal.apply_async((t,), link=save_stock_year_fiscal.s())
            t = []
    if len(t) > 0:
        get_stock_year_fiscal.apply_async((t,), link=save_stock_year_fiscal.s())


@app.task(base=RecordFailureTask, bind=True, max_retries=3, default_retry_delay=30)
def get_stock_year_fiscal(self, symbols, timeout=30):
    logger.debug("symbols={0}".format(symbols))
    api = EdgarYearReportAPI(config.get("edgar", "core_key"))
    url, method, headers, data = api.get_url_params(symbols=symbols, start_year=1970, end_year=9999)
    try:
        fetcher = RequestsFetcher(timeout=timeout)
        status_code, content = fetcher.fetch(url, method, headers, data)
        if status_code != 200:
            logger.debug("status_code={0}".format(status_code))
            if status_code == 404:
                raise Ignore()
            else:
                raise self.retry()
        ret = api.parse_ret(content)
        logger.debug("ok, symbols={0}".format(symbols))
    except Exception as exc:
        if isinstance(exc, Ignore):
            raise exc
        else:
            logger.error(traceback.format_exc())
            raise self.retry(exc=exc, timeout=60)
    return [x.serializable_obj() for x in ret]


@app.task(base=RecordFailureTask)
def update_all_stock_info_details():
    load_symbols_has_no_stock_info_details.apply_async(link=get_all_stock_info_details.s())


@app.task(base=RecordFailureTask)
def get_all_stock_info_details(symbols):
    logger.debug("len = {0}".format(len(symbols)))
    batch_size = 20
    t = []
    for index, symbol in enumerate(symbols):
        t.append(symbol)
        if len(t) % batch_size == 0:
            get_stock_info_details.apply_async((t,), link=save_stock_info_detail.s())
            t = []
    if len(t) > 0:
        get_stock_info_details.apply_async((t,), link=save_stock_info_detail.s())


@app.task(base=RecordFailureTask, bind=True, max_retries=3, default_retry_delay=30)
def get_stock_info_details(self, symbols, timeout=30):
    logger.debug("symbols={0}".format(symbols))
    api = EdgarCompanyAPI(config.get("edgar", "core_key"))
    url, method, headers, data = api.get_url_params(symbols=symbols)
    try:
        fetcher = RequestsFetcher(timeout=timeout)
        status_code, content = fetcher.fetch(url, method, headers, data)
        if status_code != 200:
            logger.debug("status_code={0}".format(status_code))
            if status_code == 404:
                raise Ignore()
            else:
                raise self.retry()
        ret = api.parse_ret(content)
        logger.debug("ok, symbols={0}".format(symbols))
    except Exception as exc:
        if isinstance(exc, Ignore):
            raise exc
        else:
            logger.error(traceback.format_exc())
            raise self.retry(exc=exc, timeout=60)
    return [x.serializable_obj() for x in ret]


@app.task(base=RecordFailureTask, bind=True, max_retries=3, default_retry_delay=30)
def update_sec_fillings_idx(self, data_dir, year, quarter):
    crawler = sec.SECCrawler(data_dir)
    try:
        status_code, fillings = crawler.load_quarter_idx(
            year, quarter, filter_form_type=["10-K", "10-K/A", "10-K405", "10-K405/A", "10-Q", "10-Q/A",
                                             "20-F", "20-F/A", "8-K", "8-K/A", "6-K", "6-K/A", "3", "3/A",
                                             "4", "4/A", "5", "5/A", "SC 13G/A", "SC 13G", "13F-HR", "13F-HR/A",
                                             "13F-NT", "13F-NT/A", "S-1", "S-1/A", "F-6", "F-6/A", "F-1", "F-1/A",
                                             "POS AM", "S-8", "S-8/A"])
        if status_code != 200:
            logger.debug("status_code={0}".format(status_code))
            if status_code == 404:
                raise Ignore()
            else:
                raise self.retry()
        save_stock_fillings_idx.apply_async(([x.serializable_obj() for x in fillings],))
        logger.debug("ok, year = {0}, quarter = {1}".format(year, quarter))
    except Exception as exc:
        if isinstance(exc, Ignore):
            raise exc
        else:
            logger.error("year = {0}, quarter = {1}, exc = {2}".format(year, quarter, traceback.format_exc()))
            raise self.retry(exc=exc)


@app.task(base=RecordFailureTask)
def update_all_sec_fillings_idx(start_year, start_quarter, end_year, end_quarter):
    for year in range(start_year, end_year + 1):
        for quarter in range(1, 5):
            if year == start_year and quarter < start_quarter:
                continue
            if year == end_year and quarter > end_quarter:
                break
            update_sec_fillings_idx.apply_async((config.get("sec", "idx_dir"), year, quarter))
            logger.debug("new task, year = {0}, quarter = {1}".format(year, quarter))


@app.task(base=RecordFailureTask)
def update_sec_fillings(symbol, filling_type="txt", form_type=None, start_date=None, end_date=None):
    load_sec_fillings_idx.apply_async((symbol, form_type, start_date, end_date), link=download_sec_fillings.s(filling_type))
    logger.debug("ok, symbol = {0}, filling_type = {1}, forme_type = {2}, start_date = {3}, end_date = {4}".
                 format(symbol, filling_type, form_type, start_date, end_date))


@app.task(base=RecordFailureTask)
def download_sec_fillings(fillings, filling_type):
    for filling in fillings:
        download_filling.apply_async((filling, filling_type))


@app.task(base=RecordFailureTask, bind=True, max_retries=3, default_retry_delay=30)
def download_filling(self, filling, filling_type):
    filling = base.SECFilling.from_serializable_obj(filling)
    crawler = sec.SECCrawler(config.get_path("sec", "data_dir"))
    try:
        if filling_type == "txt":
            status_code = crawler.download_txt_filling(filling)
        elif filling_type == "xlbr":
            status_code = crawler.download_xbrl_filling(filling)
        if status_code != 200:
            logger.debug("cik = {0}, id = {1}, status_code = {2}".format(filling.cik, filling.id, status_code))
            if status_code == 404:
                raise Ignore()
            else:
                raise self.retry()
        logger.debug("ok, cik = {0}, id = {1}".format(filling.cik, filling.id))
    except Exception as exc:
        if isinstance(exc, Ignore):
            raise exc
        else:
            logger.error("cik = {0}, id = {1}".format(filling.cik, filling.id, traceback.format_exc()))
            raise self.retry(exc=exc)
