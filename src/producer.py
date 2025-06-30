import orjson
import random
from config import REDIS_QUEUE_NAME

class GenerateVariants():
    def __init__(self, file_path, redis, seed=None):
        self.redis = redis
        if seed is not None:
            random.seed(seed)

        with open(file_path, "r") as f:
            data = orjson.loads(f.read())
            self.attribute_options = self.get_attribute_options(data)

    async def start(self):
        await self.generate_and_enqueue()

    async def generate_and_enqueue(self, count=100000):
        print(f"Generating and enqueuing {count} variants...")
        for _ in range(count):
            variant = {k: random.choice(v) for k, v in self.attribute_options.items()}
            await self.redis.lpush(REDIS_QUEUE_NAME, orjson.dumps(variant, option=orjson.OPT_SORT_KEYS))
        print("Done.")

    @staticmethod
    def get_attribute_options():
        with open("reference/output.json", "r") as f:
            data = orjson.loads(f.read())

        attribute_options = {}
        for obj in data['d']["Pages"]:
            for screen in obj["Screens"]:

                screen_options = screen["ScreenOptions"]
                if not screen_options:
                    continue

                selectable_values = screen_options[0]["SelectableValues"]
                if not selectable_values:
                    continue

                opts = []
                for val in selectable_values:
                    opts.append(val["Caption"])
                attribute_options[screen_options[0]["Caption"]] = opts

        return attribute_options
    