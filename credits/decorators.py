import uuid
from functools import wraps

from django.http import HttpRequest, HttpResponse, JsonResponse

from .exceptions import InsufficientCredits
from .services import ReserveCreditsService, commit_reservation, reverse_reservation


def charge_one_credit(get_wallet=lambda req, *a, **kw: getattr(req, "wallet", None)):
    """
    Wrap a view: reserve 1 credit before work; commit on 2xx/3xx; refund otherwise.
    Guarantees concurrency-safety (optimistic conditional decrement) and
    'successful-request-only' net billing with a sweeper for crashes.
    """

    def decorator(view_func):
        @wraps(view_func)
        def _wrapped(request: HttpRequest, *args, **kwargs) -> HttpResponse:
            wallet = get_wallet(request, *args, **kwargs)
            if not wallet:
                return JsonResponse({"detail": "API key required"}, status=401)

            idem = request.headers.get("Idempotency-Key")
            try:
                # initialize the callable service
                reserve_credits = ReserveCreditsService()

                tx = reserve_credits(
                    wallet,
                    amount=1,
                    idempotency_key=idem,
                    request_id=str(uuid.uuid4()),
                    note="api-request",
                )
            except InsufficientCredits:
                return JsonResponse({"detail": "Insufficient credits"}, status=402)

            try:
                response = view_func(request, *args, **kwargs)
                success = 200 <= getattr(response, "status_code", 500) < 400
            except Exception:
                reverse_reservation(tx, reason="exception")
                raise

            if success:
                commit_reservation(tx)
            else:
                reverse_reservation(
                    tx, reason=f"http {getattr(response, 'status_code', 500)}"
                )
            return response

        return _wrapped

    return decorator
