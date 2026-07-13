import json
from collections import defaultdict

from channels.generic.websocket import AsyncWebsocketConsumer


ROOM_LIMIT = 2
ROOM_USERS = defaultdict(dict)


class CallConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        self.room_id = str(self.scope['url_route']['kwargs']['room_id'])
        self.room_users = ROOM_USERS[self.room_id]
        self.joined_room = False

        await self.accept()

        if len(self.room_users) >= ROOM_LIMIT:
            await self.send_payload({
                'type': 'room_full',
                'message': 'Комната уже занята',
            })
            await self.close()
            return

        self.is_initiator = len(self.room_users) == 0
        self.room_users[self.channel_name] = self
        self.joined_room = True

        await self.send_payload({
            'type': 'connection_ready',
            'is_initiator': self.is_initiator,
        })

        await self.send_room_status()

    async def disconnect(self, close_code):
        if getattr(self, 'joined_room', False):
            self.room_users.pop(self.channel_name, None)

            if not self.room_users:
                ROOM_USERS.pop(self.room_id, None)
            else:
                await self.send_room_status()

    async def receive(self, text_data):
        data = json.loads(text_data)
        await self.send_to_room(data, exclude_self=True)

    async def send_room_status(self):
        await self.send_to_room({
            'type': 'room_status',
            'users_count': len(self.room_users),
        })

    async def send_to_room(self, message, exclude_self=False):
        for channel_name, connection in list(self.room_users.items()):
            if exclude_self and channel_name == self.channel_name:
                continue

            await connection.send_payload(message)

    async def send_payload(self, data):
        await self.send(text_data=json.dumps(data))