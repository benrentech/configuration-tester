import orjson
from producer import GenerateVariants, MultiGenerator
import sqlite3
from pprint import pprint

# Everything in main is meant for local testing and isn't necessary to the program
# create_tables is the exception as the database must have the fields mentioned in it.


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


# Prints out the last 2 variants from variants.db
def print_rows(num=2):
    conn = sqlite3.connect("variants.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM queue LIMIT ?", (num,))
    for row in cursor.fetchall():
        pprint(orjson.loads(row[1]))
    conn.close()


# Runs the producer on all files in reference and saves them to variants.db
def gen_multi_file():
    files = MultiGenerator.get_file_paths("reference/")
    producer = MultiGenerator(files, "variants.db")
    producer.generate()


# Converts latest variant to C# code, eg repeated rapidoptions.add(field),
# so that the variant can easily be tested with the C# Infor library
def get_latest_entry_as_csharp_list(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT data FROM queue
        ORDER BY id DESC
        LIMIT 1
    """)
    row = cursor.fetchone()
    conn.close()

    if not row:
        return "// No data found."

    data_json = row[0]
    try:
        data_dict = orjson.loads(data_json)
        if not isinstance(data_dict, dict):
            raise ValueError("Data is not a JSON object")
    except Exception as e:
        return f"// Error parsing JSON: {e}"

    # Generate C# lines using the RapidOptionData format
    csharp_lines = []
    for key, value in data_dict.items():
        key_escaped = str(key).replace('"', '\\"')
        value_str = str(value).replace('"', '\\"')

        # Simple data type detection
        if isinstance(value, bool):
            dtype = "DataType.Boolean"
            value_str = "true" if value else "false"
        elif isinstance(value, (int, float)):
            dtype = "DataType.Number"
        else:
            dtype = "DataType.String"

        csharp_lines.append(
            f'rapidOptions.Add(new RapidOptionData {{ Name = "{key_escaped}", DataType = {dtype}, Value = "{value_str}" }});'
        )

    return "\n".join(csharp_lines)


def main():
    db_path = "variants.db"
    create_tables(db_path)

    file = "officedesk.json"
    producer = GenerateVariants(f"reference/{file}", db_path, seed=41)
    producer.generate(5)

    print(get_latest_entry_as_csharp_list(db_path))


if __name__ == "__main__":
    main()
