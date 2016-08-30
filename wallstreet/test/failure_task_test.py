from __future__ import absolute_import
from wallstreet.tasks.celery import app, RecordFailureTask


@app.task(base=RecordFailureTask)
def failure_task(arg1, arg2):
    raise ValueError()
