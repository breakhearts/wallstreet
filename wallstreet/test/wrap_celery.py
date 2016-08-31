from wallstreet import config
from wallstreet.tasks.celery import app, engine_Session

app.conf.update(
    CELERY_ALWAYS_EAGER=True
)

engine_Session.init(config.get_test("storage", "url"))