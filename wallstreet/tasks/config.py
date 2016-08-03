from __future__ import absolute_import
from celery.schedules import crontab
from wallstreet import config
from kombu import Queue, Exchange

BROKER_URL = config.get("celery", "broker_url")
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_REDIRECT_STDOUTS_LEVEL = "info"
CELERY_IGNORE_RESULT = True
CELERY_DISABLE_RATE_LIMITS = True
BROKER_TRANSPORT_OPTIONS = {'fanout_prefix': True, 'fanout_patterns': True, 'visibility_timeout': 43200}
CELERY_TIMEZONE = 'US/Eastern'


CELERYBEAT_SCHEDULE = {
    'update_all_stock_info': {
        'task': 'wallstreet.tasks.stock_history_tasks.update_all_stock_info',
        'schedule': crontab(hour=4, minute=0, day_of_week='1-5')
    },
    'get_all_stock_history': {
        'task': 'wallstreet.tasks.stock_history_tasks.update_all_stock_day',
        'schedule': crontab(hour=4, minute=30, day_of_week='1-5')
    }
}

CELERY_DEFAULT_QUEUE = 'default'
CELERY_QUEUES = (
    Queue('default', Exchange('default', routing_key='default')),
    Queue('stock_history_tasks', Exchange('stock_history_tasks', type='topic'), routing_key='stock_history_tasks.#'),
    Queue('stock_storage_tasks', Exchange('stock_storage_tasks', type='topic'), routing_key='stock_storage_tasks.#')
)

CELERY_ROUTES = {
    'wallstreet.tasks.stock_history_tasks.update_all_stock_info':{
        'queue': 'stock_history_tasks',
        'routing_key': 'stock_history_tasks.update_all_stock_info'
    },
    'wallstreet.tasks.stock_history_tasks.update_stock_info': {
        'queue': 'stock_history_tasks',
        'routing_key': 'stock_history_tasks.update_stock_info'
    },
    'wallstreet.tasks.stock_history_tasks.update_all_stock_day': {
        'queue': 'stock_history_tasks',
        'routing_key': 'stock_history_tasks.update_all_stock_day'
    },
    'wallstreet.tasks.stock_history_tasks.get_all_stock_history': {
        'queue': 'stock_history_tasks',
        'routing_key': 'stock_history_tasks.get_all_stock_history'
    },
    'wallstreet.tasks.stock_storage_tasks.load_all_stock_info': {
        'queue': 'stock_storage_tasks',
        'routing_key': 'stock_storage_tasks.load_all_stock_info'
    },
    'wallstreet.tasks.stock_storage_tasks.save_stock_day': {
        'queue': 'stock_storage_tasks',
        'routing_key': 'stock_storage_tasks.save_stock_day'
    },
    'wallstreet.tasks.stock_storage_tasks.save_stock_info': {
        'queue': 'stock_storage_tasks',
        'routing_key': 'stock_storage_tasks.save_stock_info'
    }
}