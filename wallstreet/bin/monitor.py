from wallstreet.tasks.celery import app
from wallstreet.tasks.task_monitor import task_monitor

if __name__ == "__main__":
    task_monitor(app)
