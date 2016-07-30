from wallstreet.storage import StockDaySqlStorage, StockInfoSqlStorage
from wallstreet.tasks.celery import app, engine, Session


@app.task
def load_all_stock_info():
    stock_info_storage = StockInfoSqlStorage(engine, Session)
    stock_infos = stock_info_storage.load_all()
    return stock_infos


@app.task
def save_stock_day(stock_days):
    stock_day_storage = StockDaySqlStorage(engine, Session)
    stock_day_storage.save(stock_days)


@app.task
def save_stock_info(stock_infos):
    stock_info_storage = StockInfoSqlStorage(engine, Session)
    stock_info_storage.save(stock_infos)
