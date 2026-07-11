from django.urls import path
from . import views

app_name = 'calls'

urlpatterns = [
    path('', views.home, name='home'),
    path('call/<uuid:room_id>/', views.call_room, name='call_room'),
]   