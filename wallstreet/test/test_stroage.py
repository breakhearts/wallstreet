from __future__ import absolute_import
import pytest
from wallstreet.storage import *
from wallstreet import base
from datetime import datetime
from wallstreet import config


@pytest.fixture(scope="module")
def engine_and_session_cls(request):
    engine, session_cls = create_sql_engine_and_session_cls(config.get_test("storage", "url"))
    create_sql_table(engine)

    def teardown():
        drop_sql_table(engine)
    request.addfinalizer(teardown)

    return engine, session_cls


class TestStockInfoSqlStorage:
    def test_save_load(self, engine_and_session_cls):
        engine, session_cls = engine_and_session_cls
        storage = StockInfoSqlStorage(engine, session_cls)
        storage.save(base.StockInfo("BIDU", "nasdaq"))
        storage.save(base.StockInfo("BIDU", "sz"))
        t = storage.load_all()
        assert len(t) == 1
        assert t[0].exchange == 'sz'
        storage.save([base.StockInfo("BABA", "nasdaq"), base.StockInfo("QIHU", "nasdaq")])
        t = storage.load_all()
        assert len(t) == 3
        t = storage.load("BABA")
        assert t.symbol == "BABA"


class TestStockDaySqlStorage:
    def test_save_load(self, engine_and_session_cls):
        engine, session_cls = engine_and_session_cls
        storage = StockDaySqlStorage(engine, session_cls)
        storage.save(base.StockDay("BIDU", datetime(2015, 2, 13), 13.1231, 21.1234, 22.12312, 32.1234, 1022313, 1.0))
        storage.save([base.StockDay("BIDU", datetime(2015, 2, 14), 13.1231, 21.1234, 22.12312, 32.1234, 1022313, 1.0),
                      base.StockDay("BIDU", datetime(2015, 2, 15), 13.1231, 21.1234, 22.12312, 32.1234, 1022313, 1.0)])
        storage.save(base.StockDay("BIDU", datetime(2015, 2, 16), 13.1231, 21.1234, 22.12312, 32.1234, 1022313, 1.0))
        storage.save(base.StockDay("BIDU", datetime(2015, 2, 17), 13.1231, 21.1234, 22.12312, 32.1234, 1022313, 1.0))
        storage.save(base.StockDay("BABA", datetime(2015, 2, 18), 13.1231, 21.1234, 22.12312, 32.1234, 1022313, 1.0))
        storage.save(base.StockDay("BABA", datetime(2015, 2, 14), 13.1231, 21.1234, 22.12312, 32.1234, 1022313, 1.0))
        t = storage.load("BIDU", datetime(2015, 2, 13), datetime(2015, 2, 15))
        assert len(t) == 3


class TestLastUpdate:
    def test_save_load(self, engine_and_session_cls):
        engine, session_cls = engine_and_session_cls
        storage = LastUpdateSqlStorage(engine, session_cls)
        storage.save_stock_day("BIDU", datetime(2015, 2, 13))
        t = storage.load_stock_day("BIDU")
        assert t == datetime(2015, 2, 13)
        storage.save_stock_day("BIDU", datetime(2014, 2, 14))
        t = storage.load_stock_day("BIDU")
        assert t == datetime(2014, 2, 14)
