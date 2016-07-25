import pytest
from wallstreet.storage import *
from wallstreet import base


@pytest.fixture(scope="module")
def engine_and_session_cls(request):
    engine, session_cls = create_sql_engine_and_session_cls("mysql+pymysql://root@localhost/wallstreet_test")
    drop_sql_table(engine)
    create_sql_table(engine)
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


class TestStockDaySqlStorage:
    def test_save_load(self, engine_and_session_cls):
        engine, session_cls = engine_and_session_cls
        storage = StockDaySqlStorage(engine, session_cls)
        storage.save(base.StockDay("BIDU", "2015-02-13", 13.1231, 21.1234, 22.12312, 32.1234, 1022313, 1.0))
        storage.save([base.StockDay("BIDU", "2015-02-14", 13.1231, 21.1234, 22.12312, 32.1234, 1022313, 1.0),
                      base.StockDay("BIDU", "2015-02-15", 13.1231, 21.1234, 22.12312, 32.1234, 1022313, 1.0)])
        storage.save(base.StockDay("BIDU", "2015-02-16", 13.1231, 21.1234, 22.12312, 32.1234, 1022313, 1.0))
        storage.save(base.StockDay("BIDU", "2015-02-17", 13.1231, 21.1234, 22.12312, 32.1234, 1022313, 1.0))
        storage.save(base.StockDay("BABA", "2015-02-13", 13.1231, 21.1234, 22.12312, 32.1234, 1022313, 1.0))
        storage.save(base.StockDay("BABA", "2015-02-14", 13.1231, 21.1234, 22.12312, 32.1234, 1022313, 1.0))
        t = storage.load("BIDU", "2015-02-13", "2015-02-15")
        assert len(t) == 3
