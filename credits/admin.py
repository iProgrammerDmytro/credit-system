from django.contrib import admin

from .models import ApiKey, CreditTransaction, Wallet


@admin.register(Wallet)
class WalletAdmin(admin.ModelAdmin):
    list_display = ("id", "name", "balance", "updated_at")
    search_fields = ("name",)


@admin.register(ApiKey)
class ApiKeyAdmin(admin.ModelAdmin):
    def get_queryset(self, request):
        return super().get_queryset(request).select_related("wallet")

    list_display = ("id", "label", "wallet", "is_active", "created_at")
    search_fields = ("label", "key")


@admin.register(CreditTransaction)
class TxAdmin(admin.ModelAdmin):
    def get_queryset(self, request):
        return super().get_queryset(request).select_related("wallet")

    list_display = (
        "id",
        "wallet",
        "delta",
        "tx_type",
        "tx_status",
        "idempotency_key",
        "created_at",
    )
    list_filter = ("tx_status", "tx_type")
    search_fields = ("idempotency_key", "request_id", "note")
