from django.db import transaction
from django.db.models import F

from credits.enums import TxType
from credits.models import CreditTransaction, TxStatus, Wallet


def reverse_reservation(
    tx: CreditTransaction, reason: str = "auto-reverse"
) -> CreditTransaction:
    with transaction.atomic():
        curr = CreditTransaction.objects.select_for_update().get(pk=tx.pk)
        if curr.tx_status != TxStatus.PENDING:
            return curr  # nothing to do

        # mark original pending as reversed
        curr.tx_status = TxStatus.REVERSED
        curr.note = reason
        curr.save(update_fields=["tx_status", "note"])

        # refund the wallet and write REFUND entry
        Wallet.objects.filter(id=curr.wallet_id).update(
            balance=F("balance") + (-curr.delta)
        )
        CreditTransaction.objects.create(
            wallet_id=curr.wallet_id,
            delta=(-curr.delta),  # positive refund
            tx_type=TxType.REFUND,
            tx_status=TxStatus.COMMITTED,
            note=f"refund of tx {curr.id}: {reason}",
        )

        return curr
