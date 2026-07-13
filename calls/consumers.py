import json

from channels.generic.websocket import AsyncWebsocketConsumer
from redis.asyncio import Redis


ROOM_LIMIT = 2
ROOM_TTL_SECONDS = 60 * 60

redis_client = None
active_connections = {}

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


def get_redis_client():
    global redis_client

    if redis_client is None:
        from django.conf import settings
        redis_client = Redis.from_url(settings.REDIS_URL, decode_responses=True)

    return redis_client


class CallConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        self.room_id = str(self.scope['url_route']['kwargs']['room_id'])
        self.room_users_key = f'call:{self.room_id}:users'
        self.joined_room = False

        await self.accept()

        previous_count, users_count = await self.add_user_to_room()

        if previous_count == -1:
            await self.send_json({
                'type': 'room_full',
                'message': 'Комната уже занята',
            })
            await self.close()
            return

        self.is_initiator = previous_count == 0
        self.joined_room = True
        active_connections[self.channel_name] = self

        await self.send_json({
            'type': 'connection_ready',
            'is_initiator': self.is_initiator,
        })

        await self.send_room_status(users_count)

    async def disconnect(self, close_code):
        active_connections.pop(self.channel_name, None)

        if not getattr(self, 'joined_room', False):
            return

        users_count = await self.remove_user_from_room()
        await self.send_room_status(users_count)

    async def receive(self, text_data):
        data = json.loads(text_data)
        await get_redis_client().expire(self.room_users_key, ROOM_TTL_SECONDS)
        await self.send_to_room(data, exclude_self=True)

    async def add_user_to_room(self):
        previous_count, users_count = await get_redis_client().eval(
            JOIN_ROOM_SCRIPT,
            1,
            self.room_users_key,
            self.channel_name,
            ROOM_TTL_SECONDS,
            ROOM_LIMIT,
        )

        return int(previous_count), int(users_count)

    async def remove_user_from_room(self):
        redis = get_redis_client()

        await redis.srem(self.room_users_key, self.channel_name)
        users_count = await redis.scard(self.room_users_key)

        if users_count == 0:
            await redis.delete(self.room_users_key)
        else:
            await redis.expire(self.room_users_key, ROOM_TTL_SECONDS)

        return int(users_count)

    async def send_room_status(self, users_count):
        await self.send_to_room({
            'type': 'room_status',
            'users_count': users_count,
        })

    async def send_to_room(self, message, exclude_self=False):
        channel_names = await get_redis_client().smembers(self.room_users_key)

        for channel_name in channel_names:
            if exclude_self and channel_name == self.channel_name:
                continue

            connection = active_connections.get(channel_name)

            if connection:
                await connection.send_json(message)
                continue

            await self.channel_layer.send(channel_name, {
                'type': 'relay_message',
                'message': message,
            })

    async def relay_message(self, event):
        await self.send_json(event['message'])

    async def send_json(self, data):
        await self.send(text_data=json.dumps(data))