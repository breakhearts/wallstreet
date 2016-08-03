from wallstreet.tasks.celery import app
from wallstreet.tasks.task_monitor import task_monitor

task_monitor(app)
