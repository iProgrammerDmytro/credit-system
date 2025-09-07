from django.db import transaction

from credits.models import CreditTransaction, TxStatus


def commit_reservation(tx: CreditTransaction) -> CreditTransaction:
    with transaction.atomic():
        # make sure it's still pending (no double-commit)
        pending = CreditTransaction.objects.select_for_update().get(pk=tx.pk)
        if pending.tx_status != TxStatus.PENDING:
            return pending

        pending.tx_status = TxStatus.COMMITTED
        pending.save(update_fields=["tx_status"])
        return pending
