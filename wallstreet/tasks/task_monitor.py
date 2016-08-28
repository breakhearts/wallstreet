from __future__ import absolute_import
import redis
from collections import defaultdict, OrderedDict
from wallstreet import config
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


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

#task_counter = TaskCounter(host=config.get("counter", "host"), port=config.get_int("counter", "port"),
#                           db=config.get("counter", "db"))
