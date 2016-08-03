from wallstreet import config
config.set_config("storage", "url", config.get_test("storage", "url"))
from wallstreet.tasks.celery import *
app.conf.update(
    CELERY_ALWAYS_EAGER=True
)
