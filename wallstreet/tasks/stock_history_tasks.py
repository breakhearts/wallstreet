from __future__ import absolute_import
from celery import Task
from wallstreet.crawel.fetcher import RequestsFetcher
from wallstreet.crawel.stockapi import YahooHistoryDataAPI, NasdaqStockInfoAPI
from wallstreet.tasks.celery import app
from wallstreet import base
from wallstreet.tasks.stock_storage_tasks import save_stock_day, load_all_stock_info, save_stock_info
from celery.utils.log import get_task_logger
from wallstreet.tasks.task_monitor import task_counter
from wallstreet.notification.notifier import email_notifier
from celery.exceptions import Ignore
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
        get_stock_history.apply_async((stock_info.symbol, base.get_next_day_str(stock_info.last_update_date)),
                                      link=save_stock_day.s())
    logger.debug("get all stock history all")


class StockHistoryTasks(Task):
    abstract = True

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        pass


@app.task(base=StockHistoryTasks, bind=True, max_retries=3, default_retry_delay=30)
def get_stock_history(self, symbol, start_date=None, end_date=None):
    api = YahooHistoryDataAPI()
    url, method, headers, data = api.get_url_params(symbol, start_date, end_date)
    fetcher = RequestsFetcher()
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
        logger.debug("ok, symbol={0}, total={1}".format(symbol, len(ret)))
        task_counter.succeeded("HISTORY_TASKS")
        return [x.serializable_obj() for x in ret]
    except Exception as exc:
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
