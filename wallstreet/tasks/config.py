from __future__ import absolute_import
from wallstreet import config

BROKER_URL = config.get("celery", "broker_url")
CELERY_RESULT_BACKEND = config.get("celery", "celery_result_backend")
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_REDIRECT_STDOUTS_LEVEL = "info"
CELERY_IGNORE_RESULT = True
CELERY_DISABLE_RATE_LIMITS = True
BROKER_TRANSPORT_OPTIONS = {'fanout_prefix': True, 'fanout_patterns': True, 'visibility_timeout': 43200}
CELERY_TIMEZONE = 'US/Eastern'