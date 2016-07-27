from wallstreet.tasks.stock_history_tasks import *
from datetime import datetime


def test_stock_history_task():
    days = get_stock_history("BIDU", start_date="20150218", end_date="20150220")
    assert len(days) == 3
    day_last = days[0]
    assert day_last.symbol == "BIDU"
    assert day_last.date == datetime(2015, 2, 20)