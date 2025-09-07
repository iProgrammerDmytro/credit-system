import os

from celery import Celery
from celery.schedules import schedule

from .settings import CELERY_BROKER_URL

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
app = Celery("config")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()

app.conf.beat_schedule = {
    "sweep-stale-reservations": {
        "task": "credits.tasks.sweep_pending_tx",
        "schedule": schedule(60.0),
    }
}

# Sensible worker knobs
app.conf.update(
    task_acks_late=True,
    worker_prefetch_multiplier=1,  # fair scheduling for maintenance tasks
    timezone="UTC",
    broker_url=CELERY_BROKER_URL,
    result_backend=None,  # no results needed for this job
)

celery_app = app
