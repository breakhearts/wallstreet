from __future__ import absolute_import
from wallstreet.tasks.celery import app
from celery import Task
from wallstreet.crawel.fetcher import RequestsFetcher
from wallstreet.crawel.stockapi import YahooHistoryDataAPI


class StockHistoryTasks(Task):
    abstract = True

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        pass


@app.task(base=StockHistoryTasks, bind=True, max_retries=100, default_retry_delay=1)
def stock_history_task(self, symbol, start_date=None, end_date=None):
    api = YahooHistoryDataAPI()
    url, method, headers, data = api.get_url_params(symbol, start_date, end_date)
    fetcher = RequestsFetcher()
    try:
        status_code, content = fetcher.fetch(url, method, headers, data)
        if status_code != 200:
            raise self.retry()
    except Exception as exc:
        raise self.retry(exc=exc)


