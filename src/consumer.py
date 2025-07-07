import asyncio
import aiosqlite
import httpx
import orjson
# TODO: Better error handling on bad request
# TODO: Verify finished table is ok

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
        BATCH_SIZE = 100
        buffer = []

        async with aiosqlite.connect(self.db_path) as db:
            while True:
                item = await write_queue.get()
                if item is None:
                    break

                buffer.append(item)

                if len(buffer) >= BATCH_SIZE:
                    await db.executemany("INSERT INTO finished_variants (id, data) VALUES (?, ?)", buffer)
                    await db.commit()
                    print(f"Inserted batch of {len(buffer)} variants into db")
                    buffer.clear()

            # Write remaining items
            if buffer:
                await db.executemany("INSERT INTO finished_variants (id, data) VALUES (?, ?)", buffer)
                await db.commit()
                print(f"Inserted final batch of {len(buffer)} variants into db")

    async def _worker(self, read_queue, write_queue, client, worker_id):
        while True:
            variant = await read_queue.get()

            # Signal other works to stop and shut down worker
            if variant is None:
                await read_queue.put(None)
                break

            v_id, data = variant

            resp = await client.post(self.endpoint_url, data=data)
            if resp.status_code == 200:
                # Sending to response to db, currently placeholder
                await write_queue.put((v_id, orjson.dumps(resp.json())))

                print(f"Worker {worker_id} successfully sent variant {v_id}.")
            else:
                print(f"Worker {worker_id} failed to send variant {v_id}: status {resp.status_code}")

    async def start(self, num_workers):
        read_queue = asyncio.Queue(maxsize=num_workers * 10)
        write_queue = asyncio.Queue()

        async with httpx.AsyncClient(http2=True) as client:
            reader = asyncio.create_task(self._db_reader(read_queue))
            writer = asyncio.create_task(self._db_writer(write_queue))

            worker_tasks = [
                asyncio.create_task(self._worker(read_queue, write_queue, client, i))
                for i in range(num_workers)
            ]
            await reader
            await asyncio.gather(*worker_tasks)
            await write_queue.put(None)  # Signal writer to shut down
            await writer

            print("All variants have finished processing.")
    
    def run(self, num_workers=10):
        asyncio.run(self.start(num_workers))
