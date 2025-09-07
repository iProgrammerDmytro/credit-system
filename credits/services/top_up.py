from django.db import transaction
from django.db.models import F

from credits.models import CreditTransaction, TxStatus, TxType, Wallet


def top_up(wallet: Wallet, amount: int, note: str = "top-up") -> CreditTransaction:
    if amount <= 0:
        raise ValueError("Top up amount must be > 0")

    with transaction.atomic():
        Wallet.objects.filter(id=wallet.id).update(balance=F("balance") + amount)

        return CreditTransaction.objects.create(
            wallet_id=wallet.id,
            delta=amount,
            tx_type=TxType.CREDIT,
            tx_status=TxStatus.COMMITTED,
            note=note,
        )
