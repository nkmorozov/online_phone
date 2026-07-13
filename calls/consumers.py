import json

from channels.generic.websocket import AsyncWebsocketConsumer
from django.conf import settings
from redis.asyncio import Redis


ROOM_LIMIT = 2
ROOM_TTL_SECONDS = 6 * 60 * 60

redis_client = Redis.from_url(settings.REDIS_URL, decode_responses=True)

JOIN_ROOM_SCRIPT = """
local key = KEYS[1]
local channel_name = ARGV[1]
local ttl = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])

redis.call('SREM', key, channel_name)

local previous_count = redis.call('SCARD', key)

if previous_count >= limit then
    return {-1, previous_count}
end

redis.call('SADD', key, channel_name)
redis.call('EXPIRE', key, ttl)

return {previous_count, previous_count + 1}
"""


class CallConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        self.room_id = str(self.scope['url_route']['kwargs']['room_id'])
        self.room_group_name = f'call_{self.room_id}'
        self.room_users_key = f'call:{self.room_id}:users'
        self.joined_room = False

        previous_count, users_count = await self.add_user_to_room()

        if previous_count == -1:
            await self.accept()
            await self.send(text_data=json.dumps({
                'type': 'room_full',
                'message': 'Комната уже занята',
            }))
            await self.close()
            return

        self.is_initiator = previous_count == 0
        self.joined_room = True

        await self.channel_layer.group_add(self.room_group_name, self.channel_name)
        await self.accept()

        await self.send(text_data=json.dumps({
            'type': 'connection_ready',
            'is_initiator': self.is_initiator,
        }))

        await self.send_room_status(users_count)

    async def disconnect(self, close_code):
        if not getattr(self, 'joined_room', False):
            return

        users_count = await self.remove_user_from_room()

        await self.channel_layer.group_discard(self.room_group_name, self.channel_name)
        await self.send_room_status(users_count)

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

    async def add_user_to_room(self):
        previous_count, users_count = await redis_client.eval(
            JOIN_ROOM_SCRIPT,
            1,
            self.room_users_key,
            self.channel_name,
            ROOM_TTL_SECONDS,
            ROOM_LIMIT,
        )

        return int(previous_count), int(users_count)

    async def remove_user_from_room(self):
        await redis_client.srem(self.room_users_key, self.channel_name)
        users_count = await redis_client.scard(self.room_users_key)

        if users_count == 0:
            await redis_client.delete(self.room_users_key)
        else:
            await redis_client.expire(self.room_users_key, ROOM_TTL_SECONDS)

        return int(users_count)

    async def get_room_users_count(self):
        return int(await redis_client.scard(self.room_users_key))

    async def send_room_status(self, users_count=None):
        if users_count is None:
            users_count = await self.get_room_users_count()

        await self.channel_layer.group_send(
            self.room_group_name,
            {
                'type': 'room_status',
                'users_count': users_count,
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