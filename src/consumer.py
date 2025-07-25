import asyncio
import aiosqlite
from runner import Runner


class Sender:
    def __init__(self, db_path, endpoint):
        self.db_path = db_path
        self.endpoint = endpoint
        self.conn = None

    async def _db_reader(self, read_queue):
        batch_size = 100
        while True:
            async with self.conn.execute(
                """
                DELETE FROM queue
                WHERE id IN (
                    SELECT id FROM queue ORDER BY id LIMIT ?
                ) RETURNING id, data
                """,
                (batch_size,),
            ) as cursor:
                rows = await cursor.fetchall()

            if not rows:
                print("No more variants in the queue.")
                break

            for variant in rows:
                await read_queue.put(variant)

            await self.conn.commit()
        # Signal to workers no more data
        await read_queue.put(None)

    async def _db_writer(self, write_queue):
        BATCH_SIZE = 100
        QUERY = "INSERT INTO finished_variants (id, data) VALUES (?, ?)"
        buffer = []

        while True:
            item = await write_queue.get()
            if item is None:
                break

            buffer.append(item)

            if len(buffer) >= BATCH_SIZE:
                await self.conn.executemany(QUERY, buffer)
                await self.conn.commit()
                print(f"Inserted batch of {len(buffer)} variants into database")
                buffer.clear()

        # Write remaining items
        if buffer:
            await self.conn.executemany(QUERY, buffer)
            await self.conn.commit()
            print(f"Inserted final batch of {len(buffer)} variants into database")

    async def _worker(self, read_queue, write_queue, runner, worker_id):
        while True:
            variant = await read_queue.get()

            # Signal other works to stop and shut down worker
            if variant is None:
                await read_queue.put(None)
                break

            v_id, options = variant

            await runner.send(options)

    async def _init_db(self):
        self.conn = await aiosqlite.connect(self.db_path)
        await self.conn.execute("PRAGMA journal_mode=WAL;")
        await self.conn.execute("PRAGMA temp_store=memory;")
        await self.conn.execute("PRAGMA synchronous=NORMAL;")

    async def _start_common(self, num_workers, runner):
        read_queue = asyncio.Queue(maxsize=num_workers * 10)
        write_queue = asyncio.Queue()
        await self._init_db()

        reader = asyncio.create_task(self._db_reader(read_queue))
        writer = asyncio.create_task(self._db_writer(write_queue))

        worker_tasks = [
            asyncio.create_task(self._worker(read_queue, write_queue, runner, i))
            for i in range(num_workers)
        ]

        await reader
        await asyncio.gather(*worker_tasks)
        await write_queue.put(None)  # Signal writer to shut down
        await writer
        await self.conn.close()
        print("All variants have finished processing.")

    async def start(self, num_workers, runner):
        async with runner.client:
            await self._start_common(num_workers, runner)

    def run(self, num_workers=10, runner=None):
        if runner is None:
            runner = Runner(self.endpoint)
        asyncio.run(self.start(num_workers, runner))
