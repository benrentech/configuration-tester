import json
from pprint import pprint
import random
import sqlite3

def main():
    # create_db()
    permute_file()

def permute_file():
    conn = sqlite3.connect('queue.db')
    random.seed(32)
    with open("reference\output.json", "r", encoding="utf-8") as file:
        attribute_options = get_attribute_options(json.load(file))
        cursor = conn.cursor()

        for _ in range(100000):
            variant = generate_finished_variant(attribute_options)
            try:
                cursor.execute("INSERT INTO variants (data) VALUES (?)", (json.dumps(variant, sort_keys=True),))
                conn.commit()
            except sqlite3.IntegrityError:
                # print("Duplicate entry found, skipping...")
                continue

    conn.close()
    print("done")

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

            opts = []
            for val in selectable_values:
                opts.append(val["Caption"])
            attribute_options[screen_options[0]["Caption"]] = opts
    return attribute_options

def generate_finished_variant(attribute_options):
    variant = {}
    for key, values in attribute_options.items():
        variant[key] = random.choice(values)
    return variant

def create_db():
    conn = sqlite3.connect('queue.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS variants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL UNIQUE
        )
    ''')
    conn.commit()
    conn.close()
if __name__ == "__main__":
    main()