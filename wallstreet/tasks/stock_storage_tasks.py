from __future__ import absolute_import
from wallstreet.storage import StockDaySqlStorage, StockInfoSqlStorage
from wallstreet.tasks.celery import app, engine, Session
from celery.utils.log import get_task_logger
from wallstreet.base import StockDay, StockInfo
import traceback

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
        stock_day_storage = StockDaySqlStorage(engine, Session)
        try:
            stock_day_storage.save(stock_days)
        except Exception as exc:
            logger.error(traceback.format_exc())
        logger.debug("ok, symbol = {0}".format(stock_days[0].symbol))


@app.task
def save_stock_info(stock_infos):
    stock_infos = [StockInfo.from_serializable_obj(x) for x in stock_infos]
    if len(stock_infos) > 0:
        stock_info_storage = StockInfoSqlStorage(engine, Session)
        try:
            stock_info_storage.save(stock_infos)
        except Exception as exc:
            logger.debug(traceback.format_exc())
        logger.debug("ok, exchange = {0}".format(stock_infos[0].exchange))
