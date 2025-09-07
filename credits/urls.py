from django.urls import path

from .views import balance, echo

app_name = "credits"


urlpatterns = [
    path("echo", echo, name="echo"),
    path("balance", balance, name="balance"),
]
