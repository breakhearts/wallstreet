from __future__ import absolute_import
from wallstreet.tasks.celery import app
from wallstreet.tasks.stock_tasks import update_all_stock_info, get_all_stock_history