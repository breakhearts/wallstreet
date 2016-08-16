from __future__ import absolute_import
import pytest
from wallstreet.storage import *
from wallstreet import base
from datetime import datetime, timedelta
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
        storage.save(base.StockDay("BIDU", datetime(2015, 2, 20), 13.1231, 21.1234, 22.12312, 32.1234, 1022313, 1.0))
        storage.save([base.StockDay("BIDU", datetime(2015, 2, 14), 13.1231, 21.1234, 22.12312, 32.1234, 1022313, 1.0),
                      base.StockDay("BIDU", datetime(2015, 2, 15), 13.1231, 21.1234, 22.12312, 32.1234, 1022313, 1.0)])
        storage.save(base.StockDay("BIDU", datetime(2015, 2, 16), 13.1231, 21.1234, 22.12312, 32.1234, 1022313, 1.0))
        storage.save(base.StockDay("BIDU", datetime(2015, 2, 17), 13.1231, 21.1234, 22.12312, 32.1234, 1022313, 1.0))
        storage.save(base.StockDay("BABA", datetime(2015, 2, 18), 13.1231, 21.1234, 22.12312, 32.1234, 1022313, 1.0))
        storage.save(base.StockDay("BABA", datetime(2015, 2, 14), 13.1231, 21.1234, 22.12312, 32.1234, 1022313, 1.0))
        t = storage.load("BIDU", datetime(2015, 2, 13), datetime(2015, 2, 15))
        assert len(t) == 2
        t = storage.load_last("BIDU", 3)
        assert len(t) == 3
        assert t[0].date == datetime(2015,2,20)
        assert t[1].date == datetime(2015,2,17)
        assert t[2].date == datetime(2015,2,16)
        t = storage.load_last("BIDU", 1, end_date=datetime(2015, 2, 18))
        assert len(t) == 1
        assert t[0].date == datetime(2015, 2, 17)
        storage.delete('BIDU')
        t = storage.load("BIDU")
        assert len(t) == 0


class TestLastUpdate:
    def test_save_load(self, engine_and_session_cls):
        engine, session_cls = engine_and_session_cls
        storage = LastUpdateSqlStorage(engine, session_cls)
        storage.save("BIDU", datetime(2015, 2, 13), LastUpdateStorage.STOCK_DAY)
        t = storage.load("BIDU", LastUpdateStorage.STOCK_DAY)
        assert t == datetime(2015, 2, 13)
        storage.save("BIDU", datetime(2014, 2, 14), LastUpdateStorage.STOCK_DAY)
        t = storage.load("BIDU", LastUpdateStorage.STOCK_DAY)
        assert t == datetime(2014, 2, 14)


class TestBaseIndexSqlStorage:
    def test_save_load(self, engine_and_session_cls):
        engine, session_cls = engine_and_session_cls
        storage = BaseIndexSqlStorage(engine, session_cls)
        t = []
        for i in range(30):
            t.append(base.StockDay("BIDU", datetime(2015, 2, 20) - timedelta(days=i),
                                   13.1231, 1, 22.12312, 1, 100, 1))
        for i in range(30, 60):
            t.append(base.StockDay("BIDU", datetime(2015, 2, 20) - timedelta(days=i),
                                   13.1231, 1, 22.12312, 1, 100, 0.5))
        index = base.BaseIndex()
        index.update(t)
        assert index.ma5 == 1
        assert index.ma20 == 1
        assert index.ma60 == 0.75
        assert index.vol5 == 100
        assert index.vol20 == 100
        assert index.vol60 == 100
        storage.save(index)
        index1 = base.BaseIndex()
        index1.update(t[1:])
        index2 = base.BaseIndex()
        index2.update(t[40:])
        storage.save([index1, index2])
        t = storage.load("BIDU", datetime(2015, 2, 19), datetime(2015, 2, 20))
        assert len(t) == 2
        t = storage.load_last("BIDU", 2)
        assert len(t) == 2
        storage.delete('BIDU')
        t = storage.load("BIDU")
        assert len(t) == 0