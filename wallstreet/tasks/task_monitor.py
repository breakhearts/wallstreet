from __future__ import absolute_import
from celery.utils.log import get_task_logger
from wallstreet.base import print_args

logger = get_task_logger(__name__)

task_failure_logger = get_task_logger("task_failure")


class TaskFailureRecorder(object):

    def on_task_failure(self, name, args, kwargs, traceback):
        task_failure_logger.error("ERROR, name = {0}, args = {1}, kwargs = {2}, traceback = {3}"
                                  .format(name, print_args(args), print_args(kwargs), traceback))

task_faiure_recorder = TaskFailureRecorder()