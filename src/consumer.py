import asyncio
import aiosqlite
import aiohttp

class Sender:
    def __init__(self, db_path, endpoint_url):
        self.db_path = db_path
        self.endpoint_url = endpoint_url

    async def _db_reader(self, read_queue):
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
                    await read_queue.put(variant)

                await db.commit()
        # Signal to workers no more data
        await read_queue.put(None)

    async def _db_writer(self, write_queue):
        async with aiosqlite.connect(self.db_path) as db:
            while True:
                item = await write_queue.get()
                if item is None:
                    break
                await db.execute("INSERT INTO finished_variants (id, data) VALUES (?, ?)", item)
                await db.commit()

    # TODO: push info to queue
    async def _worker(self, read_queue, write_queue, session, worker_id):
        while True:
            variant = await read_queue.get()
            v_id, data = variant

            # Signal other works to stop and shut down worker
            if variant is None:
                await read_queue.put(None)
                break

            async with session.post(self.endpoint_url, data=data) as resp:
                if resp.status == 200:
                    # Sending to response to db, currently placeholder
                    data = await resp.json()
                    await write_queue.put((v_id, data))

                    print(f"Worker {worker_id} successfully sent variant {v_id}.")
                else:
                    print(f"Worker {worker_id} failed to send variant {v_id}: status {resp.status}")

    async def start(self, num_workers):
        read_queue = asyncio.Queue(maxsize=num_workers * 10)
        write_queue = asyncio.Queue()

        async with aiohttp.ClientSession() as session:
            reader = asyncio.create_task(self._db_reader(read_queue))
            writer = asyncio.create_task(self._db_writer(write_queue))

            worker_tasks = [
                asyncio.create_task(self._worker(read_queue, session, i))
                for i in range(num_workers)
            ]
            await reader
            await asyncio.gather(*worker_tasks)
            await write_queue.put(None)  # Signal writer to shut down
            await writer

            print("All variants have finished processing.")
    
    def run(self, num_workers=10):
        asyncio.run(self.start(num_workers))
