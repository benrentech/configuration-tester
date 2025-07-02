import asyncio
import sqlite3
import aiohttp

class SendRequests:
    def __init__(self, db_path, endpoint_url):
        self.db_path = db_path
        self.endpoint_url = endpoint_url

    async def db_queue_reader(self, queue):
        conn = sqlite3.connect(self.db_path)
        try:
            count = 0
            while True:
                result = conn.execute(
                    "DELETE FROM queue WHERE id = (SELECT id FROM queue ORDER BY id LIMIT 1) RETURNING id, data"
                ).fetchone()
                if result is None:
                    print("No more variants in the queue.")
                    break
                await queue.put(result)
                count += 1
                if count % 10 == 0:
                    conn.commit()
            conn.commit()
        finally:
            await queue.put(None)  # Sentinel to signal end of queue
            conn.close()

    async def sender_worker(self, queue, session, worker_id):
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
            reader_task = asyncio.create_task(self.db_queue_reader(queue))
            worker_tasks = [
                asyncio.create_task(self.sender_worker(queue, session, i))
                for i in range(num_workers)
            ]
            await reader_task
            await asyncio.gather(*worker_tasks)
        print("All workers have finished processing.")
    
    def start_async(self, num_workers=10):
        asyncio.run(self.start(num_workers))
