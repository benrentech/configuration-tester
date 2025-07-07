import asyncio
import aiosqlite
import aiohttp

class Sender:
    def __init__(self, db_path, endpoint_url):
        self.db_path = db_path
        self.endpoint_url = endpoint_url

    async def db_reader(self, queue):
        batch_size = 100
        async with aiosqlite.connect(self.db_path) as db:
            while True:
                async with db.execute(
                    """
                    DELETE FROM queue
                    WHERE id IN (
                        SELECT id FROM queue ORDER BY id LIMIT ?
                    ) RETURNING id, data
                    """,
                    (batch_size,)
                ) as cursor:
                    rows = await cursor.fetchall()
                
                if not rows:
                    print("No more variants in the queue.")
                    break

                for variant in rows:
                    await queue.put(variant)

                await db.commit()
        # Signal to workers no more data
        await queue.put(None)


    async def _worker(self, queue, session, worker_id):
        while True:
            variant = await queue.get()
            if variant is None:
                await queue.put(None)  # Pass sentinel to other workers
                break
            async with session.post(self.endpoint_url, data=variant[1]) as response:
                if response.status == 200:
                    print(f"Worker {worker_id} successfully sent variant {variant[0]}.")
                else:
                    print(f"Worker {worker_id} failed to send variant: {response.status}")

    async def start(self, num_workers):
        queue = asyncio.Queue(maxsize=num_workers * 10)
        async with aiohttp.ClientSession() as session:
            reader = asyncio.create_task(self.db_reader(queue))
            worker_tasks = [
                asyncio.create_task(self._worker(queue, session, i))
                for i in range(num_workers)
            ]
            await reader
            await asyncio.gather(*worker_tasks)
        print("All workers have finished processing.")
    
    def run(self, num_workers=10):
        asyncio.run(self.start(num_workers))
