from typing import Optional, Tuple

from django.db import transaction
from django.db.models import F

from credits.exceptions import InsufficientCredits
from credits.models import CreditTransaction, TxStatus, TxType, Wallet


class ReserveCreditsService:
    """
    Reserve (debit) credits with idempotency and conditional decrement.

    Behavior:
      - amount <= 0 -> ValueError
      - With idempotency_key:
          * get_or_create PENDING DEBIT ledger row
          * if existing -> return it (no second debit)
          * if created -> conditionally decrement wallet balance (>= amount) else raise InsufficientCredits
      - Without idempotency_key:
          * create PENDING DEBIT ledger row
          * conditionally decrement wallet balance (>= amount) else raise InsufficientCredits
    """

    def __call__(
        self,
        wallet: Wallet,
        amount: int = 1,
        idempotency_key: Optional[str] = None,
        request_id: Optional[str] = None,
        note: str = "api-request",
    ) -> CreditTransaction:
        self._validate_amount(amount)

        with transaction.atomic():
            if idempotency_key:
                tx, created = self._get_or_create_tx(
                    wallet_id=wallet.id,
                    amount=amount,
                    idempotency_key=idempotency_key,
                    request_id=request_id,
                    note=note,
                )
                if not created:
                    # idempotent hit: do NOT debit again
                    return tx
            else:
                tx = self._create_tx(
                    wallet_id=wallet.id,
                    amount=amount,
                    request_id=request_id,
                    note=note,
                )

            updated = self._conditional_decrement(wallet_id=wallet.id, amount=amount)
            if updated == 0:
                # rollback both the newly-created tx and the (attempted) decrement
                raise InsufficientCredits("Insufficient credits")

            self._after_success(tx)
            return tx

    def _validate_amount(self, amount: int) -> None:
        if amount <= 0:
            raise ValueError("Amount must be > 0")

    def _tx_defaults(self, amount: int, request_id: Optional[str], note: str) -> dict:
        return {
            "delta": -amount,
            "tx_type": TxType.DEBIT,
            "tx_status": TxStatus.PENDING,
            "request_id": request_id,
            "note": note,
        }

    def _get_or_create_tx(
        self,
        *,
        wallet_id: int,
        amount: int,
        idempotency_key: str,
        request_id: Optional[str],
        note: str,
    ) -> Tuple[CreditTransaction, bool]:
        """
        Insert-first for idempotency. Concurrent calls with same key will
        result in exactly one row; others 'get' that row (no IntegrityError).
        """
        return CreditTransaction.objects.get_or_create(
            wallet_id=wallet_id,
            idempotency_key=idempotency_key,
            defaults=self._tx_defaults(amount, request_id, note),
        )

    def _create_tx(
        self,
        *,
        wallet_id: int,
        amount: int,
        request_id: Optional[str],
        note: str,
    ) -> CreditTransaction:
        """Create a fresh PENDING DEBIT when no idempotency key is provided."""
        return CreditTransaction.objects.create(
            wallet_id=wallet_id,
            **self._tx_defaults(amount, request_id, note),
        )

    def _conditional_decrement(self, *, wallet_id: int, amount: int) -> int:
        """
        Optimistic single-row decrement. Succeeds only if balance >= amount.
        Returns number of rows updated (0 or 1).
        """
        return Wallet.objects.filter(id=wallet_id, balance__gte=amount).update(
            balance=F("balance") - amount
        )

    def _after_success(self, tx: CreditTransaction) -> None:
        """
        Hook for side-effects (metrics, outbox, transaction.on_commit handlers).
        """
        return


def reserve_credits(
    wallet: Wallet,
    amount: int = 1,
    idempotency_key: Optional[str] = None,
    request_id: Optional[str] = None,
    note: str = "api-request",
) -> Optional[CreditTransaction]:
    """
    Optimistic, single-row conditional decrement:
      UPDATE wallet SET balance = balance - amount
      WHERE id = ? AND balance >= amount
    If it updates 0 rows, there wasn't enough credit.

    Then we write a PENDING ledger entry. This is short-lived and either COMMIT or REVERSED later.
    Idempotency: if (wallet,idempotency_key) already exists, return that tx (no second charge).
    """
    if amount <= 0:
        raise ValueError("Amount must be > 0")

    with transaction.atomic():
        if idempotency_key:
            # One of the racers will create; the other will "get"
            tx, created = CreditTransaction.objects.get_or_create(
                wallet_id=wallet.id,
                idempotency_key=idempotency_key,
                defaults={
                    "delta": -amount,
                    "tx_type": TxType.DEBIT,
                    "tx_status": TxStatus.PENDING,
                    "request_id": request_id,
                    "note": note,
                },
            )

            if not created:
                return tx

        else:
            # No idempotency key, create a new reservation\
            tx = CreditTransaction.objects.create(
                wallet_id=wallet.id,
                delta=-amount,
                tx_type=TxType.DEBIT,
                tx_status=TxStatus.PENDING,
                request_id=request_id,
                note=note,
            )

        # Only the creator performs the conditional decrement.
        updated = Wallet.objects.filter(id=wallet.id, balance__gte=amount).update(
            balance=F("balance") - amount
        )

        if updated == 0:
            # Not enough credits: rollback the just-created tx (atomic() ensures rollback)
            raise InsufficientCredits("Insufficient credits")

        return tx
