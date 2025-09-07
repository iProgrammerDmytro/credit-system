from datetime import timedelta
from uuid import uuid4

from django.utils import timezone

from ..models import CreditTransaction, Wallet
from ..services import ReserveCreditsService, top_up

import logging

logger = logging.getLogger(__name__)

def make_stale(
    wallet_id: int,
    *,
    count: int = 20,
    amount: int = 1,
    seconds_ago: int = 600,
    ensure_funds: bool = True,
    tag: str = "seed-stale",
) -> list[int]:
    """
    Create *new* PENDING debit reservations and then backdate them so they are stale.
    Returns list of created tx IDs.
    """
    w = Wallet.objects.only("id", "balance").get(id=wallet_id)

    # ensure the wallet can afford the reservations (realistic seed)
    needed = count * amount
    if ensure_funds and w.balance < needed:
        top_up(w, needed - w.balance, note=f"{tag}-topup")

    svc = ReserveCreditsService()
    tx_ids: list[int] = []
    now = timezone.now()
    stale_ts = now - timedelta(seconds=seconds_ago)

    # create reservations (this debits balance)
    for _ in range(count):
        key = f"stale-{uuid4().hex[:12]}"
        tx = svc(w, amount=amount, idempotency_key=key, note=tag)
        tx_ids.append(tx.id)

    # backdate them in bulk (single UPDATE)
    CreditTransaction.objects.filter(pk__in=tx_ids).update(created_at=stale_ts)
    logger.info(
        f"[make_stale] wallet={wallet_id} created={len(tx_ids)} amount={amount} seconds_ago={seconds_ago}"
    )
    return tx_ids
