import asyncio
# from redis import Redis
import fakeredis
from producer import GenerateVariants
from config import REDIS_URL, REDIS_QUEUE_NAME

def main():
    print("Hello from config-testerv2!")
    # redis = Redis(host=REDIS_URL, port=6379, decode_responses=True)
    redis = fakeredis.FakeRedis(server_type="redis")
    producer = GenerateVariants(redis, "reference/output.json", seed=42)
    producer.start()
    
if __name__ == "__main__":
    main()