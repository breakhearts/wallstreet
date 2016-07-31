from __future__ import absolute_import
from wallstreet import storage
from wallstreet import config

engine, Session = storage.create_sql_engine_and_session_cls(config.get("storage", "url"))
storage.create_sql_table(engine)