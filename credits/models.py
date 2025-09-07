import secrets

from django.db import models
from django.utils import timezone

from .enums import TxStatus, TxType


class Wallet(models.Model):
    """
    One balance per customer/project/account.
    """

    name = models.CharField(max_length=140, unique=True)
    balance = models.BigIntegerField(default=0)  # store as integer credits
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [models.Index(fields=["balance"])]

    def __str__(self):
        return f"{self.name} (bal={self.balance})"


class ApiKey(models.Model):
    """
    Simple HMAC-less API key for demo. In production, prefer HMAC or JWT.
    """

    wallet = models.ForeignKey(
        Wallet, on_delete=models.CASCADE, related_name="api_keys"
    )
    key = models.CharField(max_length=64, unique=True, db_index=True)
    is_active = models.BooleanField(default=True)
    label = models.CharField(max_length=140, blank=True)
    created_at = models.DateTimeField(default=timezone.now)

    @staticmethod
    def generate() -> str:
        return secrets.token_hex(32)

    def __str__(self):
        return f"{self.label or self.key[:6]}… → {self.wallet_id}"


class CreditTransaction(models.Model):
    """
    Immutable(ish) ledger. We write a pending DEBIT when reserving,
    COMMIT it on success, REVERSE (and write REFUND entry) on failure.
    """

    wallet = models.ForeignKey(
        Wallet, on_delete=models.CASCADE, related_name="transactions"
    )
    delta = models.BigIntegerField()  # positive for CREDIT/REFUND, negative for DEBIT
    tx_type = models.CharField(max_length=16, choices=TxType.choices)  # TxType.*
    tx_status = models.CharField(max_length=16, choices=TxStatus.choices)  # TxStatus.*

    idempotency_key = models.CharField(max_length=64, blank=True, null=True)
    request_id = models.CharField(max_length=64, blank=True, null=True)
    note = models.CharField(max_length=240, blank=True)
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        indexes = [
            models.Index(fields=["wallet", "created_at"]),
            models.Index(fields=["idempotency_key"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["wallet", "idempotency_key"],
                name="unique_wallet_idempotency_key_not_null",
                condition=~models.Q(idempotency_key=None),
            ),
        ]

    def __str__(self):
        return f"tx[{self.pk}] {self.tx_type} {self.delta} {self.tx_status}"
