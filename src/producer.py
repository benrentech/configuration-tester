import hashlib
import sqlite3
import orjson
import random

class GenerateVariants():
    def __init__(self, file_path, db_path, seed=None):
        if seed:
            random.seed(seed)
        self.db_path = db_path
        
        with open(file_path, "r", encoding="utf8") as f:
            data = orjson.loads(f.read())
        self.attribute_options = self.get_attribute_options(data)

    def generate(self, count=1000):
       conn = sqlite3.connect(self.db_path)
       self.generate_and_enqueue(conn, count)
       conn.close()

    def generate_and_enqueue(self, conn, count):
        print(f"Generating and enqueuing {count} variants...")
        cursor = conn.cursor()  

        for i in range(count):
            variant = {k: random.choice(v) for k, v in self.attribute_options.items()}
            variant_dump = orjson.dumps(variant, option=orjson.OPT_SORT_KEYS)
            hash_str = hashlib.sha256(variant_dump).hexdigest()
            
            cursor.execute("SELECT 1 FROM queue WHERE hash = ?", (hash_str,))
            if not cursor.fetchone():
                cursor.execute("INSERT INTO queue (hash, data) VALUES (?, ?)", (hash_str, variant_dump))
                
            if (i + 1) % 10 == 0:
                conn.commit()
            print(f"Enqueued variant {i + 1}/{count}")

        conn.commit()
        print("Finished generating variants.")

    @staticmethod
    def get_attribute_options(data):
        attribute_options = {}
        for obj in data['d']["Pages"]:
            for screen in obj["Screens"]:

                screen_options = screen["ScreenOptions"]
                if not screen_options:
                    continue

                selectable_values = screen_options[0]["SelectableValues"]
                if not selectable_values:
                    continue

                options = [val["Caption"] for val in selectable_values]
                attribute_options[screen_options[0]["Caption"]] = options

        return attribute_options
    

class MultiFileVariantGenerator:
    def __init__(self, file_paths, db_path, seed=None):
        if seed:
            random.seed(seed)
        self.file_paths = file_paths
        self.db_path = db_path

    def generate(self, count_per_file=1000):
        for path in self.file_paths:
            print(f"\nProcessing file: {path}")
            gen = GenerateVariants(path, self.db_path)
            gen.generate(count=count_per_file)