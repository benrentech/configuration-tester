import asyncio
from redis.asyncio import Redis
from producer import GenerateVariants
from config import REDIS_URL, REDIS_QUEUE_NAME

async def main():
    print("Hello from config-testerv2!")
    redis = await Redis(host=REDIS_URL, port=6379, decode_responses=True)
    # producer = GenerateVariants(redis, "reference/output.json", seed=42)
    



if __name__ == "__main__":
    asyncio.run(main()) 