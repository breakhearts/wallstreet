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
#CELERY_DISABLE_RATE_LIMITS = True
BROKER_TRANSPORT_OPTIONS = {'fanout_prefix': True, 'fanout_patterns': True, 'visibility_timeout': 43200}
CELERY_TIMEZONE = 'US/Eastern'

CELERYBEAT_SCHEDULE = {
    'update_all_stock_info': {
        'task': 'wallstreet.tasks.stock_tasks.update_all_stock_info',
        'schedule': crontab(hour=0, minute=0, day_of_week='1-5')
    },
    'update_all_stock_history': {
        'task': 'wallstreet.tasks.stock_tasks.update_all_stock_day',
        'schedule': crontab(hour=0, minute=30, day_of_week='1-5')
    },
    'update_all_stock_base_index': {
        'task': 'wallstreet.tasks.stock_tasks.update_all_stock_base_index',
        'schedule': crontab(hour=4, minute=0, day_of_week='1-5')
    }
}

CELERY_DEFAULT_QUEUE = 'default'
CELERY_QUEUES = (
    Queue('default', Exchange('default', routing_key='default')),
    Queue('stock_tasks', Exchange('stock_tasks', type='topic'), routing_key='stock_tasks.#'),
    Queue('storage_tasks.read', Exchange('storage_tasks.read', type='topic'), routing_key='storage_tasks.read'),
    Queue('storage_tasks.write', Exchange('storage_tasks.write', type='topic'), routing_key='storage_tasks.write'),
    Queue('sec_tasks', Exchange('sec_tasks', type='topic'), routing_key='sec_tasks.#')
)

CELERY_ROUTES = {
    #history
    'wallstreet.tasks.stock_tasks.update_all_stock_info':{
        'queue': 'stock_tasks',
        'routing_key': 'stock_tasks.update_all_stock_info'
    },
    'wallstreet.tasks.stock_tasks.update_stock_info': {
        'queue': 'stock_tasks',
        'routing_key': 'stock_tasks.update_stock_info'
    },
    'wallstreet.tasks.stock_tasks.update_all_stock_day': {
        'queue': 'stock_tasks',
        'routing_key': 'stock_tasks.update_all_stock_day'
    },
    'wallstreet.tasks.stock_tasks.update_stock_history': {
        'queue': 'stock_tasks',
        'routing_key': 'stock_tasks.update_stock_history'
    },
    'wallstreet.tasks.stock_tasks.get_all_stock_history': {
        'queue': 'stock_tasks',
        'routing_key': 'stock_tasks.get_all_stock_history'
    },
    'wallstreet.tasks.stock_tasks.get_stock_history': {
        'queue': 'stock_tasks',
        'routing_key': 'stock_tasks.get_stock_history'
    },
    'wallstreet.tasks.stock_tasks.update_all_stock_base_index': {
        'queue': 'stock_tasks',
        'routing_key': 'stock_tasks.update_all_stock_base_index'
    },
    'wallstreet.tasks.stock_tasks.get_all_stock_base_index': {
        'queue': 'stock_tasks',
        'routing_key': 'stock_tasks.get_all_stock_base_index'
    },
    'wallstreet.tasks.stock_tasks.update_stock_base_index': {
        'queue': 'stock_tasks',
        'routing_key': 'stock_tasks.update_stock_base_index'
    },
    'wallstreet.tasks.stock_tasks.update_all_year_fiscal_report': {
        'queue': 'stock_tasks',
        'routing_key': 'stock_tasks.update_all_year_fiscal_report'
    },
    'wallstreet.tasks.stock_tasks.get_all_stock_year_fiscal': {
        'queue': 'stock_tasks',
        'routing_key': 'stock_tasks.get_all_stock_year_fiscal'
    },
    'wallstreet.tasks.stock_tasks.get_stock_year_fiscal': {
        'queue': 'stock_tasks',
        'routing_key': 'stock_tasks.get_stock_year_fiscal'
    },
    'wallstreet.tasks.stock_tasks.update_all_stock_info_details': {
        'queue': 'stock_tasks',
        'routing_key': 'stock_tasks.update_all_stock_info_details'
    },
    'wallstreet.tasks.stock_tasks.get_all_stock_info_details': {
        'queue': 'stock_tasks',
        'routing_key': 'stock_tasks.get_all_stock_info_details'
    },
    'wallstreet.tasks.stock_tasks.get_stock_info_details': {
        'queue': 'stock_tasks',
        'routing_key': 'stock_tasks.get_stock_info_details'
    },
    #sec
    'wallstreet.tasks.stock_tasks.update_sec_fillings_idx': {
        'queue': 'sec_tasks',
        'routing_key': 'sec_tasks.update_sec_fillings_idx'
    },
    'wallstreet.tasks.stock_tasks.update_all_sec_fillings_idx': {
        'queue': 'sec_tasks',
        'routing_key': 'sec_tasks.update_all_sec_fillings_idx'
    },
    'wallstreet.tasks.stock_tasks.update_sec_fillings': {
        'queue': 'sec_tasks',
        'routing_key': 'sec_tasks.update_sec_fillings'
    },
    'wallstreet.tasks.stock_tasks.download_sec_fillings': {
        'queue': 'sec_tasks',
        'routing_key': 'sec_tasks.download_sec_fillings'
    },
    'wallstreet.tasks.stock_tasks.download_filling': {
        'queue': 'sec_tasks',
        'routing_key': 'sec_tasks.download_filling'
    },
    #storage read
    'wallstreet.tasks.storage_tasks.load_all_stock_info': {
        'queue': 'storage_tasks.read',
        'routing_key': 'storage_tasks.read'
    },
    'wallstreet.tasks.storage_tasks.load_last_update_date': {
        'queue': 'storage_tasks.read',
        'routing_key': 'storage_tasks.read'
    },
    'wallstreet.tasks.storage_tasks.load_last_stock_days': {
        'queue': 'storage_tasks.read',
        'routing_key': 'storage_tasks.read'
    },
    'wallstreet.tasks.storage_tasks.load_symbols_has_no_year_fiscal_report': {
        'queue': 'storage_tasks.read',
        'routing_key': 'storage_tasks.read'
    },
    'wallstreet.tasks.storage_tasks.load_symbols_has_no_stock_info_details': {
        'queue': 'storage_tasks.read',
        'routing_key': 'storage_tasks.read'
    },
    'wallstreet.tasks.storage_tasks.load_sec_fillings_idx': {
        'queue': 'storage_tasks.read',
        'routing_key': 'storage_tasks.read'
    },
    # storage write
    'wallstreet.tasks.storage_tasks.save_stock_day': {
        'queue': 'storage_tasks.write',
        'routing_key': 'storage_tasks.write'
    },
    'wallstreet.tasks.storage_tasks.save_stock_info': {
        'queue': 'storage_tasks.write',
        'routing_key': 'storage_tasks.write'
    },
    'wallstreet.tasks.storage_tasks.save_stock_base_index': {
        'queue': 'storage_tasks.write',
        'routing_key': 'storage_tasks.write'
    },
    'wallstreet.tasks.storage_tasks.compute_base_index': {
        'queue': 'storage_tasks.write',
        'routing_key': 'storage_tasks.write'
    },
    'wallstreet.tasks.storage_tasks.clear_stock': {
        'queue': 'storage_tasks.write',
        'routing_key': 'storage_tasks.write'
    },
    'wallstreet.tasks.storage_tasks.save_stock_year_fiscal': {
        'queue': 'storage_tasks.write',
        'routing_key': 'storage_tasks.write'
    },
    'wallstreet.tasks.storage_tasks.save_stock_info_detail': {
        'queue': 'storage_tasks.write',
        'routing_key': 'storage_tasks.write'
    },
    'wallstreet.tasks.storage_tasks.save_stock_fillings_idx': {
        'queue': 'storage_tasks.write',
        'routing_key': 'storage_tasks.write'
    },
}

CELERY_ANNOTATIONS = {
    'wallstreet.tasks.stock_tasks.get_stock_history': {'rate_limit': '1/s'},
    'wallstreet.tasks.stock_tasks.get_stock_year_fiscal': {'rate_limit': '1/s'},
    'wallstreet.tasks.stock_tasks.get_stock_info_details': {'rate_limit': '1/s'}
}
