from typing import Optional

from celery import shared_task

from .services import sweep_stale_reservations


@shared_task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_jitter=True,
    retry_kwargs={"max_retries": 5},
    acks_late=True,
    time_limit=60,
)
def sweep_pending_tx(self, chunk_size: Optional[int] = 500) -> int:
    total = sweep_stale_reservations(chunk_size=chunk_size)
    return total
