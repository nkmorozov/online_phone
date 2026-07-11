# routing - маршрутизация
# это как urls.py, только для WebSocket

from django.urls import path
from . import consumers
websocket_urlpatterns = [
    path('ws/call/<uuid:room_id>/', consumers.CallConsumer.as_asgi()),
]

