from django.shortcuts import render
from django.urls import reverse
import uuid

def home(request):
    room_id = uuid.uuid4()
    room_path = reverse('calls:call_room', args=[room_id])
    call_link = request.build_absolute_uri(room_path)

    return render(request, 'calls/home.html', {
        'call_link': call_link,
    })

def call_room(request, room_id):
    return render(request, 'calls/call_room.html', {
        'room_id': room_id,
    })
