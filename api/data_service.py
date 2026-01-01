# Example: Data API (FastAPI)

# MVP Data API (FastAPI + SQLite)
from fastapi import FastAPI, HTTPException
import sqlite3
from pathlib import Path

app = FastAPI()
DB_PATH = str(Path(__file__).parent.parent / "patients.db")

@app.get("/patients/{patient_id}")
def get_patient(patient_id: str):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, name, birth_date FROM patient WHERE id = ?", (patient_id,))
    row = c.fetchone()
    conn.close()
    if row:
        return {"id": row[0], "name": row[1], "birth_date": row[2]}
    else:
        raise HTTPException(status_code=404, detail="Patient not found")

@app.get("/patients")
def list_patients():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, name, birth_date FROM patient")
    rows = c.fetchall()
    conn.close()
    return [
        {"id": row[0], "name": row[1], "birth_date": row[2]} for row in rows
    ]
