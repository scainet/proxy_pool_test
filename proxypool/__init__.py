import os
import logging
import asyncio
import time
import aioredis
from typing import Optional

from pydantic import HttpUrl, IPvAnyNetwork

PROXYPOOL_LIST_PREFIX = os.environ.get('PROXYPOOL_REDIS_PREFIX', 'proxypool:list')

DEFAULT_THROTTLE_DELAY = 3
DEFAULT_PENALTY_DELAY = 15
DEFAULT_PROXY_QUERY_LOOP_SLEEP = 0.5

REDIS_LUA_ZPOPMINSCORE = """
    local response = nil
    local key = KEYS[1]
    local score = ARGV[1]
    local number_elements = redis.call('ZCOUNT', key, '-inf', score)
    if number_elements >= 1 then
        response =  redis.call('ZPOPMIN', key)[1]
    end
    return response
"""


class ProxyPool:

    def __init__(self, redis: aioredis.Redis, host: HttpUrl, throttle_delay: int = DEFAULT_THROTTLE_DELAY):
        self.redis: aioredis.Redis = redis
        self.zpopminscore = self.redis.register_script(REDIS_LUA_ZPOPMINSCORE)
        self.key: str = f'{PROXYPOOL_LIST_PREFIX}:{host}'
        self.host: str = host
        self.penalty_delay: int = 0
        self.throttle_delay: int = throttle_delay
        self.ip: Optional[IPvAnyNetwork] = None

    async def __aenter__(self):
        proxy = await self.zpopminscore([self.key], [int(time.time())])
        while not proxy:
            logging.warning('Espero a que tengamos proxies libres')
            await asyncio.sleep(DEFAULT_PROXY_QUERY_LOOP_SLEEP)
            proxy = await self.zpopminscore([self.key], [int(time.time())])
        self.ip = proxy
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.redis.zadd(self.key, {self.ip: int(time.time() + self.throttle_delay + self.penalty_delay)})

    async def penalize(self, delay=DEFAULT_PENALTY_DELAY):
        self.penalty_delay = delay
        logging.warning( f'Penalizando proxy {self.ip} {self.penalty_delay} segundos para el host {self.host}')


async def reset_proxy_pool(redis, host: HttpUrl, proxies_list):
    key = f'{PROXYPOOL_LIST_PREFIX}:{host}'
    await redis.delete(key)
    for proxy in proxies_list:
        await redis.zadd(key, {proxy: int(time.time())})
        logging.warning(f'Agregando proxy {proxy} para host {host}')
