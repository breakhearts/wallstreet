from wallstreet import config
from wallstreet.tasks.celery import app
from wallstreet.storage import create_sql_engine_and_session_cls

engine, Session = create_sql_engine_and_session_cls(config.get_test("storage", "url"))

app.conf.update(
    CELERY_ALWAYS_EAGER=True
)
