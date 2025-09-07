from typing import Optional

from django.http import JsonResponse
from django.views.decorators.http import require_GET

from .decorators import charge_one_credit
from .models import Wallet


@require_GET
@charge_one_credit()
def echo(request) -> JsonResponse:
    # pretend to do useful work...
    return JsonResponse({"ok": True, "message": "Service did its job!"})


@require_GET
def balance(request) -> JsonResponse:
    wallet: Optional[Wallet] = getattr(request, "wallet", None)
    if not wallet:
        return JsonResponse({"detail": "API key required"}, status=401)

    return JsonResponse({"wallet": wallet.name, "balance": wallet.balance})
