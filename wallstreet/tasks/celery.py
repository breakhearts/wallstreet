from __future__ import absolute_import
from celery import Celery, Task
from wallstreet.storage import create_sql_engine_and_session_cls, create_sql_table
from wallstreet import config
from logging.config import dictConfig
from wallstreet.logging import config as log_config
from wallstreet.tasks.task_monitor import task_faiure_recorder
from celery.signals import worker_process_init

dictConfig(log_config.CELERY_LOGGING)
app = Celery("task")
app.config_from_object("wallstreet.tasks.config")


class EngineSession(object):
    def __init__(self):
        self.__engine = None
        self.__Session = None

    @property
    def Session(self):
        return self.__Session

    @property
    def engine(self):
        return self.__engine

    def init(self, url):
        self.__engine, self.__Session = create_sql_engine_and_session_cls(url)
        create_sql_table(self.__engine)


engine_Session = EngineSession()

if app.conf.CELERY_ALWAYS_EAGER:
    engine_Session.init(config.get("storage", "url"))


@worker_process_init.connect
def worker_process_init_handler(sender=None, instance=None, **kwargs):
    engine_Session.init(config.get("storage", "url"))


class RecordFailureTask(Task):
    abstract = True

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        if einfo:
            traceback = str(einfo)
        else:
            traceback = None
        task_faiure_recorder.on_task_failure(self.name, args, kwargs, traceback)