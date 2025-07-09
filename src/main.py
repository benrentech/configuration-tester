import orjson
from producer import GenerateVariants, MultiGenerator
from consumer import Sender
import sqlite3
from pprint import pprint

# Meant for local testing
def create_tables(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT,
            hash TEXT UNIQUE
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS finished_variants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT
        )
    """)
    
    conn.commit()
    conn.close()
    
def print_rows():
    conn = sqlite3.connect("variants.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM queue LIMIT 2")
    for row in cursor.fetchall():
        pprint(orjson.loads(row[1]))
    conn.close()

def multi_file_gen():
    files = MultiGenerator.get_file_paths("reference/")
    producer = MultiGenerator(files, "variants.db")
    producer.generate()

def single_gen():
    producer = GenerateVariants("reference/output.json", "variants.db", seed=42)
    producer.generate()

def send_variants():
    sender = Sender("variants.db", "https://httpbin.org/post")
    sender.run(num_workers=50)

def main():
    create_tables("variants.db")
    single_gen()
    send_variants()

if __name__ == "__main__":
    main()