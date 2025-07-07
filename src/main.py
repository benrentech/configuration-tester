from producer import GenerateVariants
from consumer import Sender
import sqlite3

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
    
def main():
    create_tables("variants.db")
    producer = GenerateVariants("reference/output.json", "variants.db", seed=42)
    producer.generate()

    sender = Sender("variants.db", "https://httpbin.org/post")
    sender.run(num_workers=50)
    
if __name__ == "__main__":
    main()