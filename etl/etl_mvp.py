# MVP ETL: Extract, Transform, Load to SQLite
import json
import sqlite3
from pathlib import Path

def extract_sample_ehr(json_path):
    with open(json_path) as f:
        data = json.load(f)
    return data

def transform_ehr(data):
    # Simple transformation: flatten and select fields
    return [
        (entry['id'], entry['name'], entry['birth_date'])
        for entry in data
    ]

def load_to_sqlite(records, db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS patient (
        id TEXT PRIMARY KEY,
        name TEXT,
        birth_date TEXT
    )''')
    c.executemany('INSERT OR REPLACE INTO patient VALUES (?, ?, ?)', records)
    conn.commit()
    conn.close()

if __name__ == "__main__":
    # Example usage
    sample_json = Path(__file__).parent / "sample_ehr.json"
    db_path = Path(__file__).parent.parent / "patients.db"
    data = extract_sample_ehr(sample_json)
    records = transform_ehr(data)
    load_to_sqlite(records, db_path)
    print(f"Loaded {len(records)} records into {db_path}")
