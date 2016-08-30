from __future__ import absolute_import
import os
from wallstreet import base
from wallstreet import config

LOG_ROOT = os.path.abspath(os.path.dirname(__file__) + "../../../logs")
base.wise_mk_dir(LOG_ROOT)


CELERY_LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'normal': {
                'format': '[%(asctime)s]%(levelname)s,%(funcName)s,%(lineno)d,%(message)s'
            },
        },
    'handlers': {
        'console': {
            'formatter': 'normal',
            'class': 'logging.StreamHandler',
            'level': 'DEBUG'
        },
        'socket': {
            'formatter': 'normal',
            'class': 'logging.handlers.SocketHandler',
            'level': 'DEBUG',
            'host': config.get("log_server", "host"),
            'port': config.get_int("log_server", "port")
        }
    },
    'loggers': {
        'wallstreet.tasks.stock_tasks': {
            'handlers': ['socket'],
            'level': 'DEBUG'
        },
        'wallstreet.tasks.storage_tasks': {
            'handlers': ['socket'],
            'level': 'DEBUG'
        },
        'task_failure': {
            'handlers': ['socket'],
            'level': 'ERROR'
        }
    }
}

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'normal': {
                'format': '[%(asctime)s]%(levelname)s,%(funcName)s,%(lineno)d,%(message)s'
            },
        },
    'handlers': {
        'console': {
            'formatter': 'normal',
            'class': 'logging.StreamHandler',
            'level': 'DEBUG'
        },
        'stock_tasks': {
            'formatter': 'normal',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'level': "DEBUG",
            'filename': os.path.join(LOG_ROOT, "stock_tasks.log"),
            'when': "D",
            'interval': 1
        },
        'storage_tasks': {
            'formatter': 'normal',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'level': "DEBUG",
            'filename': os.path.join(LOG_ROOT, "storage_tasks.log"),
            'when': "D",
            'interval': 1
        },
        'task_failure': {
            'formatter': 'normal',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'level': "ERROR",
            'filename': os.path.join(LOG_ROOT, "task_failure.log"),
            'when': "D",
            'interval': 1
        }
    },
    'loggers': {
        'wallstreet.tasks.stock_tasks': {
            'handlers': ['console', 'stock_tasks'],
            'level': 'DEBUG'
        },
        'wallstreet.tasks.storage_tasks': {
            'handlers': ['console', 'storage_tasks'],
            'level': 'DEBUG'
        },
        'task_failure': {
            'handlers': ['console', 'task_failure'],
            'level': 'ERROR'
        }
    }
}