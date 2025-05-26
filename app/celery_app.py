from celery import Celery
from celery.schedules import crontab
from app.config import CELERY_BROKER, CELERY_BACKEND

INCLUDE = [
    "app.insights_async",
    "app.meta_ads_library",
]

celery = Celery(
    "tasks",
    broker=CELERY_BROKER,
    backend=CELERY_BACKEND,
    include=INCLUDE,
)


celery.conf.update(
    result_extended=True,
    task_track_started=True,
    result_expires=0,
    timezone="Asia/Kolkata",
    celery_timezone="UTC",
    task_serializer="json",
    result_serializer="json",
    accept_content=["application/json", "application/x-python-serialize"],
)

