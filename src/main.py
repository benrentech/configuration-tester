import aioredis
import asyncio
from producer import GenerateVariants
from config import REDIS_URL, REDIS_QUEUE_NAME

async def main():
    print("Hello from config-testerv2!")
    redis = aioredis.from_url(REDIS_URL)
    producer = GenerateVariants(redis, "reference/output.json", seed=42)
    



if __name__ == "__main__":
    asyncio.run(main()) 