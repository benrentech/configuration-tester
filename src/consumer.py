import asyncio
import aiosqlite
from runner import Runner


class Sender:
    """
    Asynchronously processes and sends data variants from a SQLite database queue to a remote endpoint.
    """

    def __init__(self, db_path, endpoint):
        """
        Initialize the Sender.

        Args:
            db_path (str): Path to the SQLite database.
            endpoint (str): URL or identifier for the destination endpoint.
        """
        self.db_path = db_path
        self.endpoint = endpoint
        self.conn = None

    async def _db_reader(self, read_queue):
        """
        Asynchronously reads variants from the `queue` table in batches and places them into a queue for processing.

        Args:
            read_queue (asyncio.Queue): Queue to send variants to workers.
        """
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
        """
        Asynchronously writes processed variants to the `finished_variants` table in batches.

        Args:
            write_queue (asyncio.Queue): Queue containing processed variants from workers.
        """
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
        """
        Processes items from `read_queue` by sending them using `runner`, and places results into `write_queue`.

        Args:
            read_queue (asyncio.Queue): Queue containing variants to process.
            write_queue (asyncio.Queue): Queue to store processed results.
            runner (Runner): Runner instance used to send the data.
            worker_id (int): ID of the worker for logging purposes.
        """
        while True:
            variant = await read_queue.get()

            # Signal other workers to stop and shut down this worker
            if variant is None:
                await read_queue.put(None)
                break

            v_id, options = variant

            await runner.send(options)
            print(f"Worker {worker_id} finished sending variant {v_id}")

            await write_queue.put((v_id, options))

    async def _init_db(self):
        """
        Initializes the SQLite database connection and sets performance-related PRAGMA settings.
        """
        self.conn = await aiosqlite.connect(self.db_path)
        await self.conn.execute("PRAGMA journal_mode=WAL;")
        await self.conn.execute("PRAGMA temp_store=memory;")
        await self.conn.execute("PRAGMA synchronous=NORMAL;")

    async def _start_common(self, num_workers, runner):
        """
        Manages the lifecycle of the reader, workers, and writer.

        Args:
            num_workers (int): Number of concurrent worker tasks.
            runner (Runner): Runner instance used by workers to send data.
        """
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
        """
        Starts the asynchronous pipeline with the given runner and number of workers.

        Args:
            num_workers (int): Number of concurrent worker tasks.
            runner (Runner): Runner instance used to send data.
        """
        async with runner.client:
            await self._start_common(num_workers, runner)

    def run(self, num_workers=10, runner=None):
        """
        Entry point for running the Sender. Starts the event loop.

        Args:
            num_workers (int, optional): Number of concurrent workers. Defaults to 10.
            runner (Runner, optional): Optional preconfigured runner. If None, a new one is created.
        """
        if runner is None:
            runner = Runner(self.endpoint)
        asyncio.run(self.start(num_workers, runner))
