from __future__ import absolute_import
import pytest
from wallstreet import config
config.set_config("storage", "url", config.get_test("storage", "url"))
from wallstreet.tasks.stock_tasks import *
from wallstreet.tasks.storage_tasks import *
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


def test_get_all_stock_history(engine_and_session_cls):
    # insert data
    storage = StockInfoSqlStorage(engine, Session)
    storage.save(base.StockInfo("BIDU", "nasdaq"))
    storage.save(base.StockInfo("BABA", "nasdaq"))
    storage = LastUpdateSqlStorage(engine, Session)
    storage.save("BIDU", datetime.today() - timedelta(days=7), LastUpdateStorage.STOCK_DAY)
    update_all_stock_day()
    # check data
    storage = StockDaySqlStorage(engine, Session)
    t = storage.load("BIDU")
    assert len(t) > 0
    assert t[0].symbol == "BIDU"
    t = storage.load("BABA")
    assert len(t) > 0
    assert t[0].symbol == "BABA"
    storage = LastUpdateSqlStorage(engine, Session)
    t = storage.load('BABA', LastUpdateStorage.STOCK_DAY)
    assert t > datetime(2016, 1, 1)


def test_update_stock_base_index(engine_and_session_cls):
    stock_days = []
    for i in range(40):
        stock_days.append(base.StockDay("FB", datetime(2005, 1, 1) + timedelta(days=i * 2), 1, 1, 1, 1, 1, 1))
    stock_day_storage = StockDaySqlStorage(engine, Session)
    stock_day_storage.save(stock_days)
    stock_info_storage = StockInfoSqlStorage(engine, Session)
    stock_info_storage.save(base.StockInfo("FB", "nasdaq"))
    update_all_stock_base_index()
    base_index_storage = BaseIndexSqlStorage(engine, Session)
    t = base_index_storage.load("FB")
    assert len(t) == 39
    t = base_index_storage.load_last(symbol="FB", limit=1)
    assert len(t) == 1
    last_update_storage = LastUpdateSqlStorage(engine, Session)
    last_update_date = last_update_storage.load("FB", LastUpdateStorage.STOCK_BASE_INDEX)
    assert last_update_date == datetime(2005, 1, 1) + timedelta(days=39 * 2)
    assert t[0].vol60 == 0
    assert t[0].vol20 == 1


def test_update_stock_info(engine_and_session_cls):
    t = update_stock_info("NASDAQ")
    t = [base.StockInfo.from_serializable_obj(x) for x in t]
    assert len(t) > 100
    assert t[0].exchange == "NASDAQ"


def test_update_all_stock_info(engine_and_session_cls):
    update_all_stock_info()
    storage = StockInfoSqlStorage(engine, Session)
    t = storage.load_all()
    assert len(t) > 1000


def test_update_year_fiscal(engine_and_session_cls):
    get_all_stock_year_fiscal(["BIDU", "AAPL"])
    storage = RawYearFiscalReportSqlStorage(engine, Session)
    t = storage.load("AAPL")
    assert len(t) > 0
    t = storage.load("BIDU")
    assert len(t) > 0
    t = load_symbols_has_no_year_fiscal_report()
    assert len(t) > 0
    assert "AAPL" not in set(t) and "BIDU" not in set(t)


def test_update_stock_info_details(engine_and_session_cls):
    get_all_stock_info_details(["BIDU", "AAPL"])
    storage = StockInfoDetailSqlStorage(engine, Session)
    t = storage.load("AAPL")
    assert t.symbol == "AAPL"
