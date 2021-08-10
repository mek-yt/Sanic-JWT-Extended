from typing import Any, Dict, Optional

import aioredis
import ujson


class RedisConnection:  # pragma: no cover
    redis: Optional[aioredis.Redis] = None
    connection_info: Dict[str, Any]

    @classmethod
    async def _get_redis_connection(cls):
        if cls.redis and not cls.redis.closed:
            return cls.redis

        cls.redis = await aioredis.from_url(cls.connection_info)
        return cls.redis

    @classmethod
    async def initialize(cls, connection_info):
        cls.connection_info = connection_info
        await cls._get_redis_connection()

    @classmethod
    async def release(cls):
        if cls.redis:
            del cls.redis
        cls.redis = None

    @classmethod
    async def set(cls, key: str, value: Any, **kwargs) -> None:
        async with cls._get_redis_connection().client() as connection:
            dumped_value = ujson.dumps(value)
            await connection.set(key, dumped_value, **kwargs)

    @classmethod
    async def get(cls, key: str) -> Any:
        async with cls._get_redis_connection().client() as connection:
            value = await connection.get(key)
            value = ujson.loads(value) if value else None
            return value

    @classmethod
    async def delete(cls, *keys: str):
        async with cls._get_redis_connection().client() as connection:
            await connection.delete(*keys)
