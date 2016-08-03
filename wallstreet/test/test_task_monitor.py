from __future__ import absolute_import
from wallstreet import config
from wallstreet.tasks.task_monitor import TaskCounter
import pytest


@pytest.fixture(scope="module")
def counter(request):
    c = TaskCounter(config.get_test("counter", "host"),config.get_test_int("counter", "port"),
                    config.get_test_int("counter", "db"))

    def teardown():
        c.reset()
    request.addfinalizer(teardown)
    return c


class TestTaskCounter:
    def test_all(self, counter):
        counter.reset()
        assert len(counter.report()) == 0
        counter.new("test1")
        counter.new("test1")
        counter.new("test1")
        counter.succeeded("test1")
        counter.succeeded("test1")
        counter.failed("test1")
        counter.new("test2")
        counter.new("test2")
        counter.new("test2")
        counter.succeeded("test2")
        counter.failed("test2")
        counter.failed("test2")
        r = counter.report()
        assert r["test1.sent"] == 3
        assert r["test1.succeeded"] == 2
        assert r["test1.failed"] == 1
        assert r["test2.sent"] == 3
        assert r["test2.succeeded"] == 1
        assert r["test2.failed"] == 2