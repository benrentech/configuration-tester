import asyncio
import aiohttp
import aioredis
from config import REDIS_URL, REDIS_QUEUE_NAME

async def worker(redis, session, worker_id):
    print(f"Worker {worker_id} started.")
    while True:
        variant_json = await redis.rpop(REDIS_QUEUE_NAME)
        if variant_json is None:
            await asyncio.sleep(0.1)
            continue

        try:
            async with session.post("https://httpbin.org/post", data=variant_json) as resp:
                if resp.status != 200:
                    print(f"[Worker {worker_id}] Failed: HTTP {resp.status}")
        except Exception as e:
            print(f"[Worker {worker_id}] Error: {e}")


async def main():
    redis = await aioredis.from_url(REDIS_URL)
    async with aiohttp.ClientSession() as session:
        workers = [
            asyncio.create_task(worker(redis, session, i))
            for i in range(10)
        ]
        await asyncio.gather(*workers)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Shutting down.")
