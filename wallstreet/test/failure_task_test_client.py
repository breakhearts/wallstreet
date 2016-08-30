from __future__ import absolute_import
from wallstreet.test.failure_task_test import failure_task

if __name__ == "__main__":
    failure_task.apply_async((1, [{"a": "b"}, (1, 2)]))