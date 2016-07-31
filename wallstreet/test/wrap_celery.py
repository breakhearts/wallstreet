from wallstreet import config
config.set_config("storage", "url", "mysql+pymysql://root@localhost/wallstreet_test")
from wallstreet.tasks.celery import *
app.conf.update(
    CELERY_ALWAYS_EAGER=True
)
