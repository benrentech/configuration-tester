import hashlib
import os
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
        commit_interval = 10

        for i in range(count):
            variant = {k: random.choice(v) for k, v in self.attribute_options.items()}
            variant_dump = orjson.dumps(variant, option=orjson.OPT_SORT_KEYS)
            hash_str = hashlib.sha256(variant_dump).hexdigest()
            
            # Checks for duplicates before adding
            cursor.execute("SELECT 1 FROM queue WHERE hash = ?", (hash_str,))
            if not cursor.fetchone():
                cursor.execute("INSERT INTO queue (hash, data) VALUES (?, ?)", (hash_str, variant_dump))
                
            if (i + 1) % commit_interval == 0:
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
                screen_options = screen_options[0]

                # Options must be a list since random.choice is called on it
                match screen_options["DisplayType"]:
                    case "CheckBox":
                        options = [True, False]
                    case "TextBox":
                        options = ["test"]
                    case "NumericTextBox":
                        options = [0]
                    # Most common case, handles everything with multiple options like checkboxes
                    case _:
                        selectable_values = screen_options["SelectableValues"]
                        options = [val["Caption"] for val in selectable_values]

                if len(options) == 0:
                    continue

                attribute_options[screen_options["Name"]] = options

        return attribute_options
    

class MultiGenerator:
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

    @staticmethod
    def get_file_paths(folder_path):
        return [
            os.path.join(folder_path, f)
            for f in os.listdir(folder_path)
            if os.path.isfile(os.path.join(folder_path, f))
        ]
