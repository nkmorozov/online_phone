import json
from collections import defaultdict

from channels.generic.websocket import AsyncWebsocketConsumer


ROOM_USERS = defaultdict(set)


class CallConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        self.room_id = str(self.scope['url_route']['kwargs']['room_id'])
        self.room_group_name = f'call_{self.room_id}'
        self.is_initiator = len(ROOM_USERS[self.room_id]) == 0

        if len(ROOM_USERS[self.room_id]) >= 2:
            await self.accept()
            await self.send(text_data=json.dumps({
                'type': 'room_full',
                'message': 'Комната уже занята',
            }))
            await self.close()
            return

        ROOM_USERS[self.room_id].add(self.channel_name)

        await self.channel_layer.group_add(self.room_group_name, self.channel_name)
        await self.accept()

        await self.send(text_data=json.dumps({
            'type': 'connection_ready',
            'is_initiator': self.is_initiator,
        }))

        await self.send_room_status()

    async def disconnect(self, close_code):
        if hasattr(self, 'room_id'):
            ROOM_USERS[self.room_id].discard(self.channel_name)
            await self.channel_layer.group_discard(self.room_group_name, self.channel_name)
            await self.send_room_status()

    async def receive(self, text_data):
        data = json.loads(text_data)

        await self.channel_layer.group_send(
            self.room_group_name,
            {
                'type': 'signal_message',
                'sender': self.channel_name,
                'message': data,
            },
        )

    async def send_room_status(self):
        await self.channel_layer.group_send(
            self.room_group_name,
            {
                'type': 'room_status',
                'users_count': len(ROOM_USERS[self.room_id]),
            },
        )

    async def room_status(self, event):
        await self.send(text_data=json.dumps({
            'type': 'room_status',
            'users_count': event['users_count'],
        }))

    async def signal_message(self, event):
        if event['sender'] != self.channel_name:
            await self.send(text_data=json.dumps(event['message']))