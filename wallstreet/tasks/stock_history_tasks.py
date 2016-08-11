from __future__ import absolute_import
from celery import Task
from wallstreet.crawel.fetcher import RequestsFetcher
from wallstreet.crawel.stockapi import YahooHistoryDataAPI, NasdaqStockInfoAPI
from wallstreet.tasks.celery import app
from wallstreet import base
from wallstreet.tasks.stock_storage_tasks import save_stock_day, load_all_stock_info, save_stock_info
from wallstreet.tasks.stock_storage_tasks import load_last_update_date, load_last_stock_days, save_stock_base_index
from wallstreet.tasks.stock_storage_tasks import compute_base_index
from celery.utils.log import get_task_logger
from wallstreet.storage import LastUpdateStorage
from wallstreet.tasks.task_monitor import task_counter
from wallstreet.notification.notifier import email_notifier
from celery.exceptions import Ignore
from dateutil.parser import parse
from wallstreet import config
import traceback

logger = get_task_logger(__name__)


@app.task
def update_all_stock_info():
    update_stock_info.apply_async(("NASDAQ",), link=save_stock_info.s())
    update_stock_info.apply_async(("NYSE",), link=save_stock_info.s())
    update_stock_info.apply_async(("AMEX",), link=save_stock_info.s())
    logger.debug("update all ok")


@app.task(bind=True, max_retries=10, default_retry_delay=30)
def update_stock_info(self, exchange):
    api = NasdaqStockInfoAPI()
    url, method, headers, data = api.get_url_params(exchange)
    fetcher = RequestsFetcher()
    try:
        status_code, content = fetcher.fetch(url, method, headers, data)
        if status_code != 200:
            logger.debug("status_code={0}".format(status_code))
            raise self.retry()
        ret = api.parse_ret(exchange, content.decode("utf-8"))
        logger.debug("ok, exchange={0}, total={1}".format(exchange, len(ret)))
        return [x.serializable_obj() for x in ret]
    except Exception as exc:
        logger.error(traceback.format_exc())
        raise self.retry(exc=exc)


@app.task
def update_all_stock_day():
    load_all_stock_info.apply_async(link=get_all_stock_history.s())


@app.task
def get_all_stock_history(stocks):
    stocks = [base.StockInfo.from_serializable_obj(x) for x in stocks]
    for stock_info in stocks:
        load_last_update_date.apply_async((stock_info.symbol, LastUpdateStorage.STOCK_DAY),
                                          link=update_stock_history.s(stock_info.symbol))
    logger.debug("ok")


@app.task
def update_stock_history(last_update_date, symbol):
    last_update_date = parse(last_update_date)
    if base.get_last_after_hour_date() == last_update_date:
        logger.debug("fresh data, no need update, symbol={0}".format(symbol))
        return
    get_stock_history.apply_async((symbol, base.get_next_day_str(last_update_date), None, True,
                                   base.get_day_str(last_update_date)), link=save_stock_day.s())


class StockHistoryTasks(Task):
    abstract = True

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        pass


@app.task(base=StockHistoryTasks, bind=True, max_retries=3, default_retry_delay=30)
def get_stock_history(self, symbol, start_date=None, end_date=None, check_dividend=False, last_no_dividend=None):
    api = YahooHistoryDataAPI()
    if check_dividend and last_no_dividend != start_date:
        assert last_no_dividend is not None
        real_start_date = min(start_date, last_no_dividend)
    else:
        real_start_date = start_date
    url, method, headers, data = api.get_url_params(symbol, real_start_date, end_date)
    fetcher = RequestsFetcher(timeout=config.get_int("fetcher", "timeout"))
    task_counter.new("HISTORY_TASKS")
    try:
        status_code, content = fetcher.fetch(url, method, headers, data)
        if status_code != 200:
            logger.debug("status_code={0}".format(status_code))
            if status_code == 404:
                raise Ignore()
            else:
                raise self.retry()
        ret = api.parse_ret(symbol, content.decode("utf-8"))
        if check_dividend and last_no_dividend != start_date:
            ret.sort(key=lambda x: x.date)
            start_index = 0
            for index, t in enumerate(ret):
                if t.date.strftime("%Y%m%d") == last_no_dividend:
                    if t.adj_factor != 1.0:
                        logger.debug("found dividend, symbol={0}".format(symbol))
                        return get_stock_history((symbol,))
                    if real_start_date < last_no_dividend:
                        break
                if t.date.strftime("%Y%m%d") == start_date:
                    start_index = index
                    if real_start_date < start_date:
                        break
            ret = ret[start_index:]
        logger.debug("ok, symbol={0}, total={1}".format(symbol, len(ret)))
        task_counter.succeeded("HISTORY_TASKS")
        return [x.serializable_obj() for x in ret]
    except Exception as exc:
        if isinstance(exc, Ignore):
            raise exc
        else:
            logger.error(traceback.format_exc())
            raise self.retry(exc=exc)

@app.task
def report_tasks():
    t = task_counter.report()
    s = ""
    for item, count in t.items():
        s += "{0}\t{1}\n".format(item, count)
    email_notifier.send_text("wallstreet daily report", s)
    task_counter.reset()


@app.task
def update_all_stock_base_index():
    load_all_stock_info.apply_async(link=get_all_stock_base_index.s())


@app.task
def get_all_stock_base_index(stocks):
    stocks = [base.StockInfo.from_serializable_obj(x) for x in stocks]
    for stock_info in stocks:
        load_last_update_date.apply_async((stock_info.symbol, LastUpdateStorage.STOCK_BASE_INDEX),
                                          link=update_stock_base_index.s(stock_info.symbol))
    logger.debug("ok")


@app.task
def update_stock_base_index(last_update_date, symbol):
    last_update_date = parse(last_update_date)
    last_after_hour_date = base.get_last_after_hour_date()
    if last_after_hour_date == last_update_date:
        logger.debug("fresh data, no need update, symbol = {0}".format(symbol))
        return
    fetch_days = (last_after_hour_date - last_update_date).days - 1 + 60
    compute_base_index.apply_async((symbol, fetch_days, last_after_hour_date.strftime("%Y-%m-%d")))
    logger.debug("ok")
