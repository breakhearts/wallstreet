from __future__ import absolute_import
import redis
from collections import defaultdict, OrderedDict
from wallstreet import config


class TaskCounter(object):

    def __init__(self, host, port, db):
        self.tasks = defaultdict(int)
        self.r = redis.StrictRedis(host, port, db, socket_connect_timeout=10)

    def reset(self):
        for key in self.r.keys():
            self.r.delete(key)

    def new(self, name):
        self.r.incr(name + ".sent", 1)

    def succeeded(self, name):
        self.r.incr(name + ".succeeded", 1)

    def failed(self, name):
        self.r.incr(name + ".failed", 1)

    def report(self):
        ret = OrderedDict()
        keys = []
        for key in self.r.keys():
            keys.append(key)
        keys.sort()
        for key in keys:
            ret[key.decode("utf-8")] = int(self.r.get(key))
        return ret

task_counter = TaskCounter(host=config.get("counter", "host"), port=config.get_int("counter", "port"),
                           db=config.get("counter", "db"))


def task_monitor(app):
    state = app.events.State()

    def on_task_failed(event):
        state.event(event)
        task = state.tasks.get(event['uuid'])
        task_counter.failed(task.name)

    def on_task_succeeded(event):
        state.event(event)
        task = state.tasks.get(event['uuid'])
        task_counter.succeeded(task.name)

    def on_task_sent(event):
        state.event(event)
        task = state.tasks.get(event['uuid'])
        task_counter.new(task.name)

    with app.connection() as connection:
        recv = app.events.Receiver(connection, handlers={
            'task-sent': on_task_sent,
            'task-failed': on_task_failed,
            'task-succeeded': on_task_succeeded
        })
        recv.capture(limit=None, timeout=None, wakeup=True)
