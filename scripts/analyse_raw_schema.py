"""

AI Assisted Script.

This module analyzes JSON files in a directory, summarizing schema types and collecting
example field values. The schema analysis identifies data types of fields, while examples
provide representative values for each field.

The results include:
1. A schema summary of all field paths with associated data types.
2. Example values for each field path.

Attributes:
    RAW_DIR (str): The directory containing the raw JSON files to analyze.
    OUTPUT_SCHEMA (str): The output JSON file summarizing the schema types.
    OUTPUT_EXAMPLES (str): The output JSON file containing example values.

Functions:
    type_name(value: Any) -> str:
        Return a string representing the type of a value.

    merge_type_info(type_info: Dict[str, set], new_info: Dict[str, set]):
        Merge type info sets.

    update_example_values(example_map: Dict[str, Any], path: str, value: Any):
        Store one example per field.

    walk_json(obj: Any, path: str = "") -> Dict[str, set]:
        Recursively walk JSON object and return a dict mapping field paths to
        their corresponding data types.

    analyse_raw_json(raw_dir: str):
        Analyze JSON files in the specified directory, generate a schema summary,
        and save example values for fields.

Note:
    - Processes `.json` files in the directory specified by `RAW_DIR`.
    - Skips hidden files or those starting with special system prefixes.
"""
import json
from pathlib import Path
from collections import defaultdict
from typing import Any, Dict

import json5
from tqdm import tqdm

RAW_DIR = "data/raw"
OUTPUT_SCHEMA = "scripts/schema_summary.json"
OUTPUT_EXAMPLES = "scripts/field_examples.json"


def type_name(value: Any) -> str:
    """Return a string representing the type of a value."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "str"
    if isinstance(value, list):
        return "list"
    if isinstance(value, dict):
        return "dict"
    return type(value).__name__


def merge_type_info(type_info: Dict[str, set], new_info: Dict[str, set]):
    """Merge type info sets."""
    for key, types in new_info.items():
        type_info[key].update(types)
    return type_info


def update_example_values(example_map: Dict[str, Any], path: str, value: Any):
    """Store one example per field."""
    if path not in example_map:
        example_map[path] = value


def walk_json(obj: Any, path: str = "") -> Dict[str, set]:
    """
    Recursively walk JSON object and return a dict:
        { "path.to.field": { "str", "int", "null", ... } }
    path is dot-separated for nested structures.
    """
    type_map = defaultdict(set)

    # Base case: not dict or list — simple value
    if not isinstance(obj, (dict, list)):
        type_map[path].add(type_name(obj))
        return type_map

    # Handle dict
    if isinstance(obj, dict):
        for key, value in obj.items():
            field_path = f"{path}.{key}" if path else key
            sub_map = walk_json(value, field_path)
            merge_type_info(type_map, sub_map)
        return type_map

    # Handle list (walk each element)
    if isinstance(obj, list):
        list_path = f"{path}[]" if path else "[]"
        if not obj:
            # Empty list: mark as list[empty]
            type_map[list_path].add("empty_list")
        else:
            for item in obj:
                sub_map = walk_json(item, path)  # same path for elements
                merge_type_info(type_map, sub_map)
        return type_map


def analyse_raw_json(raw_dir: str):
    all_type_info = defaultdict(set)
    example_values = {}

    raw_path = Path(raw_dir)
    files = [f for f in raw_path.iterdir() if f.suffix == ".json" and not f.name.startswith("._")]

    print(f"Scanning {len(files)} JSON files...")

    for file in tqdm(files):
        try:
            with open(file, "r", encoding="utf-8") as fh:
                data = json5.load(fh)

            # files might contain a list of advocates
            if isinstance(data, list):
                for entry in data:
                    type_map = walk_json(entry)
                    merge_type_info(all_type_info, type_map)

                    # Save example values
                    for field_path, types in type_map.items():
                        # safe retrieval — navigate through keys
                        try:
                            parts = field_path.replace("[]", "").split(".")
                            val = entry
                            for p in parts:
                                val = val.get(p) if isinstance(val, dict) else None
                            update_example_values(example_values, field_path, val)
                        except Exception:
                            pass

            elif isinstance(data, dict):
                type_map = walk_json(data)
                merge_type_info(all_type_info, type_map)
                for field_path in type_map:
                    try:
                        parts = field_path.replace("[]", "").split(".")
                        val = data
                        for p in parts:
                            val = val.get(p) if isinstance(val, dict) else None
                        update_example_values(example_values, field_path, val)
                    except Exception:
                        pass

        except Exception as e:
            print(f"Error reading {file}: {e}")

    # Output schema file
    with open(OUTPUT_SCHEMA, "w") as fh:
        json.dump({k: sorted(list(v)) for k, v in all_type_info.items()}, fh, indent=2)

    # Output example values file
    with open(OUTPUT_EXAMPLES, "w") as fh:
        json.dump(example_values, fh, indent=2)

    print(f"\nDone. Wrote:")
    print(f" - {OUTPUT_SCHEMA}")
    print(f" - {OUTPUT_EXAMPLES}")


if __name__ == "__main__":
    analyse_raw_json(RAW_DIR)
