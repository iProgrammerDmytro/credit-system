from django.db.models import TextChoices
from django.utils.translation import gettext_lazy as _


class TxStatus(TextChoices):
    PENDING = "pending", _("Pending")  # reserved, not yet committed
    COMMITTED = "committed", _("Committed")  # final, counts toward usage
    REVERSED = "reversed", _("Reversed")  # auto or manual reversal of a pending


class TxType(TextChoices):
    DEBIT = "debit", _("Debit")
    CREDIT = "credit", _("Credit")
    REFUND = "refund", _("Refund")
