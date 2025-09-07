from django.http import HttpRequest, HttpResponse

from .models import ApiKey


class ApiKeyMiddleware:
    """
    Resolves request.wallet from header `X-API-Key`. If invalid, leaves it None.
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request: HttpRequest) -> HttpResponse:
        key = request.headers.get("X-API-Key")
        request.wallet = None

        if key:
            api_key = (
                ApiKey.objects.select_related("wallet")
                .filter(key=key, is_active=True)
                .first()
            )

            if api_key:
                request.wallet = api_key.wallet

        return self.get_response(request)
