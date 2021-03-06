import asyncio
import aioredis
import random

from proxypool import ProxyPool, reset_proxy_pool

MOCK_PROXIES = ['192.168.1.120', '192.168.1.130', '192.168.1.140', '192.168.1.150', '192.168.1.160']

async def extraction_task(redis_client, task):
    for x in range(random.randrange(1, 5)):
        async with ProxyPool(redis=redis_client, host='www.google.com') as proxy:
            print(f'{task}:usando proxy {proxy.ip}')
            if random.random() < 0.3:
                await proxy.penalize()
    print(f'{task}:Fin tarea')


async def main():
    redis_client = aioredis.from_url("redis://localhost")
    await reset_proxy_pool(redis=redis_client, host='www.google.com', proxies_list=MOCK_PROXIES)
    for x in range(100):
        async with ProxyPool(redis=redis_client, host='www.google.com') as proxy:
            print(f'usando proxy {proxy.ip}')
            if random.random() < 0.1:
                await proxy.penalize()


async def main2():
    redis_client = aioredis.from_url("redis://localhost")
    await reset_proxy_pool(redis=redis_client, host='www.google.com', proxies_list=MOCK_PROXIES)
    await asyncio.gather(*[extraction_task(redis_client, x) for x in range(10)])

if __name__ == '__main__':
   asyncio.run(main2())
