from __future__ import absolute_import
from celery import Celery
from wallstreet.storage import create_sql_engine_and_session_cls, create_sql_table
from wallstreet import config
from logging.config import dictConfig
from wallstreet.logging import config as log_config

dictConfig(log_config.CELERY_LOGGING)
app = Celery("task")
app.config_from_object("wallstreet.tasks.config")

engine, Session = create_sql_engine_and_session_cls(config.get("storage", "url"))
create_sql_table(engine)
