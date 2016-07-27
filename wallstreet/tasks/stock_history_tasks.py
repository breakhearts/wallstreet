from __future__ import absolute_import
from celery import Task
from wallstreet.crawel.fetcher import RequestsFetcher
from wallstreet.crawel.stockapi import YahooHistoryDataAPI
from wallstreet.storage import StockInfoSqlStorage, StockDaySqlStorage
from wallstreet.tasks.celery import app
from wallstreet import base


@app.task
def get_all_stock_history():
    stock_info_storage = StockInfoSqlStorage()
    stock_infos = stock_info_storage.load_all()
    for stock_info in stock_infos:
        get_stock_history.apply_async(get_stock_history, stock_infos.symbol,
                                      base.get_next_day_str(stock_info.last_update_date))


class StockHistoryTasks(Task):
    abstract = True

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        pass


@app.task(base=StockHistoryTasks, bind=True, max_retries=100, default_retry_delay=1)
def get_stock_history(self, symbol, start_date=None, end_date=None):
    api = YahooHistoryDataAPI()
    url, method, headers, data = api.get_url_params(symbol, start_date, end_date)
    fetcher = RequestsFetcher()
    try:
        status_code, content = fetcher.fetch(url, method, headers, data)
        if status_code != 200:
            raise self.retry(exc=status_code)
        return api.parse_ret(symbol, content.decode("utf-8"))
    except Exception as exc:
        raise self.retry(exc=exc)

