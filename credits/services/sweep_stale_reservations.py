import logging
from datetime import datetime, timedelta
from typing import Optional

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from ..models import CreditTransaction, TxStatus
from .reverse_reservation import reverse_reservation

logger = logging.getLogger(__name__)


def sweep_stale_reservations(
    *, now: Optional[datetime] = None, chunk_size: Optional[int] = 500
):
    ttl = int(getattr(settings, "CREDIT_RESERVATION_TTL", 300))
    now = now or timezone.now()
    cutoff = now - timedelta(seconds=ttl)

    total = 0

    while True:
        with transaction.atomic():
            qs = (
                CreditTransaction.objects.filter(
                    tx_status=TxStatus.PENDING, created_at__lt=cutoff
                )
                .select_for_update(skip_locked=True)
                .order_by("id")[:chunk_size]
            )
            batch = list(qs)

            if not batch:
                break

            for tx in batch:
                reverse_reservation(tx, reason="expired")

            total += len(batch)

    logger.info("sweep.done total=%s", total)

    return total
