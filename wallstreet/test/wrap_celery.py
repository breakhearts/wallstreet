from wallstreet import config
config.set_config("storage", "url", config.get_test("storage", "url"))
import wallstreet.tasks.celery
try:
	import importlib
	importlib.reload(wallstreet.tasks.celery)
except:
	reload(wallstreet.tasks.celery)
from wallstreet.tasks.celery import app, engine, Session
app.conf.update(
    CELERY_ALWAYS_EAGER=True
)
