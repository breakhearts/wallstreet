from __future__ import absolute_import
import pytest
from wallstreet import config
import os
config.set_config("storage", "url", config.get_test("storage", "url"))
config.set_config("sec", "data_dir", config.get_test("sec", "data_dir"))
from wallstreet.tasks.stock_tasks import *
from wallstreet.tasks.storage_tasks import *
from datetime import datetime, timedelta
from wallstreet.storage import *
from wallstreet import base
from wallstreet.test.wrap_celery import engine_Session
import shutil


@pytest.fixture(scope="module")
def engine_and_session_cls(request):
    create_sql_table(engine_Session.engine)

    def teardown():
        drop_sql_table(engine_Session.engine)
        if os.path.exists(config.get_path("sec", "data_dir")):
            shutil.rmtree(config.get_path("sec", "data_dir"))
    request.addfinalizer(teardown)


def test_get_all_stock_history(engine_and_session_cls):
    # insert data
    storage = StockInfoSqlStorage(engine_Session.engine, engine_Session.Session)
    storage.save(base.StockInfo("BIDU", "nasdaq"))
    storage.save(base.StockInfo("BABA", "nasdaq"))
    storage = LastUpdateSqlStorage(engine_Session.engine, engine_Session.Session)
    storage.save("BIDU", datetime.today() - timedelta(days=7), LastUpdateStorage.STOCK_DAY)
    update_all_stock_day()
    # check data
    storage = StockDaySqlStorage(engine_Session.engine, engine_Session.Session)
    t = storage.load("BIDU")
    assert len(t) > 0
    assert t[0].symbol == "BIDU"
    t = storage.load("BABA")
    assert len(t) > 0
    assert t[0].symbol == "BABA"
    storage = LastUpdateSqlStorage(engine_Session.engine, engine_Session.Session)
    t = storage.load('BABA', LastUpdateStorage.STOCK_DAY)
    assert t > datetime(2016, 1, 1)


def test_update_stock_base_index(engine_and_session_cls):
    stock_days = []
    for i in range(40):
        stock_days.append(base.StockDay("FB", datetime(2005, 1, 1) + timedelta(days=i * 2), 1, 1, 1, 1, 1, 1))
    stock_day_storage = StockDaySqlStorage(engine_Session.engine, engine_Session.Session)
    stock_day_storage.save(stock_days)
    stock_info_storage = StockInfoSqlStorage(engine_Session.engine, engine_Session.Session)
    stock_info_storage.save(base.StockInfo("FB", "nasdaq"))
    update_all_stock_base_index()
    base_index_storage = BaseIndexSqlStorage(engine_Session.engine, engine_Session.Session)
    t = base_index_storage.load("FB")
    assert len(t) == 39
    t = base_index_storage.load_last(symbol="FB", limit=1)
    assert len(t) == 1
    last_update_storage = LastUpdateSqlStorage(engine_Session.engine, engine_Session.Session)
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
    storage = StockInfoSqlStorage(engine_Session.engine, engine_Session.Session)
    t = storage.load_all()
    assert len(t) > 1000


def test_update_year_fiscal(engine_and_session_cls):
    get_all_stock_year_fiscal(["BIDU", "AAPL"])
    storage = RawYearFiscalReportSqlStorage(engine_Session.engine, engine_Session.Session)
    t = storage.load("AAPL")
    assert len(t) > 0
    t = storage.load("BIDU")
    assert len(t) > 0
    t = load_symbols_has_no_year_fiscal_report()
    assert len(t) > 0
    assert "AAPL" not in set(t) and "BIDU" not in set(t)


def test_update_stock_info_details(engine_and_session_cls):
    get_all_stock_info_details(["BIDU", "AAPL"])
    storage = StockInfoDetailSqlStorage(engine_Session.engine, engine_Session.Session)
    t = storage.load("AAPL")
    assert t.symbol == "AAPL"


def test_update_sec_fillings(engine_and_session_cls):
    storage = SECFillingSqlStorage(engine_Session.engine, engine_Session.Session)
    storage.save(
        base.SECFilling(company_name="Facebook", cik="1326801", form_type="8-K", date=datetime(2015, 1, 31), id="0001326801-16-000060"))
    storage = StockInfoDetailSqlStorage(engine_Session.engine, engine_Session.Session)
    storage.save(base.StockInfoDetail("FB", "NASDAQ", "0001326801", "XX", "sector", "siccode", "city"))
    update_sec_fillings(symbol="FB", filling_type="txt", form_type="8-K")
    import os
    assert os.path.getsize(os.path.join(config.get_path("sec", "data_dir"),
                                        os.path.join("fillings/{0}/{1}.txt").format("1326801", "0001326801-16-000060") )) > 0