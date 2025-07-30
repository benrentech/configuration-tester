import hashlib
import os
import sqlite3
import orjson
import random


class GenerateVariants:
    """
    Generates unique configuration variants based on screen option data and enqueues them into a SQLite database.
    """

    def __init__(self, file_path, db_path, seed=None):
        """
        Initializes the variant generator by reading a JSON configuration file and parsing attribute options.

        Args:
            file_path (str): Path to the JSON file containing screen option data.
            db_path (str): Path to the SQLite database.
            seed (int, optional): Random seed for reproducibility.
        """
        if seed:
            random.seed(seed)

        self.db_path = db_path

        with open(file_path, "r", encoding="utf8") as f:
            data = orjson.loads(f.read())

        self.attribute_options = self.get_attribute_options(data)

    def generate(self, count=1000):
        """
        Generates a number of unique variants and enqueues them into the `queue` table.

        Args:
            count (int): Number of variants to generate. Defaults to 1000.

        Behavior:
            - Opens a SQLite connection.
            - Delegates generation and insertion to `generate_and_enqueue()`.
            - Closes the database connection afterward.
        """
        conn = sqlite3.connect(self.db_path)
        self.generate_and_enqueue(conn, count)
        conn.close()

    def generate_and_enqueue(self, conn, count):
        """
        Generates and inserts configuration variants into the database, avoiding duplicates.

        Args:
            conn (sqlite3.Connection): Open SQLite connection.
            count (int): Number of variants to generate.

        Behavior:
            - Randomly selects values for each attribute from pre-parsed options.
            - Serializes and hashes the variant using SHA-256.
            - Checks for existing hash in the `queue` table to avoid duplicates.
            - Commits in small batches (every 10 inserts) for performance.
        """
        print(f"Generating and enqueuing {count} variants...")
        cursor = conn.cursor()
        commit_interval = 10

        for i in range(count):
            # Generate one variant using random values for each attribute
            variant = {k: random.choice(v) for k, v in self.attribute_options.items()}
            variant_dump = orjson.dumps(variant, option=orjson.OPT_SORT_KEYS)
            hash_str = hashlib.sha256(variant_dump).hexdigest()

            # Skip if duplicate
            cursor.execute("SELECT 1 FROM queue WHERE hash = ?", (hash_str,))
            if not cursor.fetchone():
                cursor.execute(
                    "INSERT INTO queue (hash, data) VALUES (?, ?)",
                    (hash_str, variant_dump),
                )

            if (i + 1) % commit_interval == 0:
                conn.commit()

            print(f"Enqueued variant {i + 1}/{count}")

        conn.commit()
        print("Finished generating variants.")

    def get_attribute_options(self, data):
        """
        Extracts attribute options from the parsed JSON data.

        Args:
            data (dict): Parsed JSON dictionary from the input file.

        Returns:
            dict[str, list]: A mapping from attribute names to possible option values.

        Behavior:
            - Iterates through each screen in the data.
            - Extracts display type and determines the valid values for that type.
              - `CheckBox` → [True, False]
              - `TextBox` → ["test"]
              - `NumericTextBox` → [12345]
              - Others → values from `SelectableValues` field
            - Skips screens with no options or no values.
        """
        attribute_options = {}

        for obj in data["d"]["Pages"]:
            for screen in obj["Screens"]:
                screen_options = screen["ScreenOptions"]
                if not screen_options:
                    continue
                screen_options = screen_options[0]

                match screen_options["DisplayType"]:
                    case "CheckBox":
                        options = [True, False]
                    case "TextBox":
                        options = ["test"]
                    case "NumericTextBox":
                        options = [12345]
                    case _:
                        selectable_values = screen_options["SelectableValues"]
                        options = [val["Value"] for val in selectable_values]

                if len(options) == 0:
                    continue

                attribute_options[screen_options["Name"]] = options

        return attribute_options


class MultiGenerator:
    """
    Handles batch generation of configuration variants from multiple JSON files.
    """

    def __init__(self, file_paths, db_path, seed=None):
        """
        Initializes a MultiGenerator with multiple JSON files and a common database.

        Args:
            file_paths (list[str]): List of JSON file paths to process.
            db_path (str): Path to the SQLite database.
            seed (int, optional): Seed for reproducible randomness.
        """
        if seed:
            random.seed(seed)

        self.file_paths = file_paths
        self.db_path = db_path

    def generate(self, count_per_file=1000):
        """
        Processes each JSON file and generates variants from it.

        Args:
            count_per_file (int): Number of variants to generate per file. Defaults to 1000.

        Behavior:
            - For each file, instantiates a `GenerateVariants` object.
            - Invokes the `generate()` method to populate the database.
        """
        for path in self.file_paths:
            print(f"\nProcessing file: {path}")
            gen = GenerateVariants(path, self.db_path)
            gen.generate(count=count_per_file)

    @staticmethod
    def get_file_paths(folder_path):
        """
        Retrieves all file paths in the specified folder.

        Args:
            folder_path (str): Path to the directory.

        Returns:
            list[str]: List of full file paths found in the folder.
        """
        return [
            os.path.join(folder_path, f)
            for f in os.listdir(folder_path)
            if os.path.isfile(os.path.join(folder_path, f))
        ]
