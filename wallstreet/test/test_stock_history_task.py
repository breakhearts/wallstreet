from __future__ import absolute_import
import pytest
from wallstreet.tasks.stock_history_tasks import *
from datetime import datetime, timedelta
from wallstreet.storage import *
from wallstreet.test.wrap_celery import app, engine, Session


@pytest.fixture(scope="module")
def engine_and_session_cls(request):
    create_sql_table(engine)

    def teardown():
        drop_sql_table(engine)
    request.addfinalizer(teardown)


def test_stock_history_task():
    days = get_stock_history("BIDU", start_date="20150218", end_date="20150220")
    assert len(days) == 3
    day_last = days[0]
    assert day_last.symbol == "BIDU"
    assert day_last.date == datetime(2015, 2, 20)


def test_get_all_stock_history(engine_and_session_cls):
    # insert data
    storage = StockInfoSqlStorage(engine, Session)
    storage.save(base.StockInfo("BIDU", "nasdaq", datetime.today() - timedelta(days=7)))
    storage.save(base.StockInfo("BABA", "nasdaq", datetime.today() - timedelta(days=7)))
    load_stocks()
    # check data
    storage = StockDaySqlStorage(engine, Session)
    t = storage.load("BIDU")
    assert len(t) > 0
    assert t[0].symbol == "BIDU"
    t = storage.load("BABA")
    assert len(t) > 0
    assert t[0].symbol == "BABA"
