from __future__ import absolute_import
from celery import Celery
from wallstreet.storage import create_sql_engine_and_session_cls
from wallstreet import config

app = Celery("task")
app.config_from_object("wallstreet.tasks.config")

engine, Session = create_sql_engine_and_session_cls(config.get("storage", "url"))
