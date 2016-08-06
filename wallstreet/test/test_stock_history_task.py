from __future__ import absolute_import
import pytest
from wallstreet import config
config.set_config("storage", "url", config.get_test("storage", "url"))
from wallstreet.tasks.stock_history_tasks import *
from datetime import datetime, timedelta
from wallstreet.storage import *
from wallstreet import base
from wallstreet.test.wrap_celery import engine, Session


@pytest.fixture(scope="module")
def engine_and_session_cls(request):
    create_sql_table(engine)

    def teardown():
        drop_sql_table(engine)
    request.addfinalizer(teardown)


def test_stock_history_task():
    days = get_stock_history("BIDU", start_date="20150218", end_date="20150220")
    days = [base.StockDay.from_serializable_obj(x) for x in days]
    assert len(days) == 3
    day_last = days[0]
    assert day_last.symbol == "BIDU"
    assert day_last.date == datetime(2015, 2, 20)


def test_get_all_stock_history(engine_and_session_cls):
    # insert data
    storage = StockInfoSqlStorage(engine, Session)
    storage.save(base.StockInfo("BIDU", "nasdaq", datetime.today() - timedelta(days=7)))
    storage.save(base.StockInfo("BABA", "nasdaq", datetime.today() - timedelta(days=7)))
    update_all_stock_day()
    # check data
    storage = StockDaySqlStorage(engine, Session)
    t = storage.load("BIDU")
    assert len(t) > 0
    assert t[0].symbol == "BIDU"
    t = storage.load("BABA")
    assert len(t) > 0
    assert t[0].symbol == "BABA"
    storage = StockInfoSqlStorage(engine, Session)
    t = storage.load('BABA')
    assert t.last_update_date > datetime(2016, 1, 1)


def test_update_stock_info():
    t = update_stock_info("NASDAQ")
    t = [base.StockInfo.from_serializable_obj(x) for x in t]
    assert len(t) > 100
    assert t[0].exchange == "NASDAQ"


def test_update_all_stock_info():
    update_all_stock_info()
    storage = StockInfoSqlStorage(engine, Session)
    t = storage.load_all()
    assert len(t) > 1000
