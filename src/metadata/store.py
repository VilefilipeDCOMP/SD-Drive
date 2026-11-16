import base64
import json
from pathlib import Path
import sqlite3

def encode(value):
    if isinstance(value, bytes):
        return {"$bytes": base64.b64encode(value).decode("ascii")}
    if isinstance(value, set):
        return {"$set": sorted(value)}
    if isinstance(value, dict):
        return {key: encode(item) for key, item in value.items()}
    if isinstance(value, list):
        return [encode(item) for item in value]
    return value

def decode(value):
    if isinstance(value, dict):
        if "$bytes" in value:
            return base64.b64decode(value["$bytes"])
        if "$set" in value:
            return set(value["$set"])
        return {key: decode(item) for key, item in value.items()}
    if isinstance(value, list):
        return [decode(item) for item in value]
    return value

class MetadataStore:
    """Snapshot transacional de metadados para a escala do laboratório."""
    def __init__(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.execute("CREATE TABLE IF NOT EXISTS metadata_state (id INTEGER PRIMARY KEY CHECK(id=1), state TEXT NOT NULL)")

    def load(self):
        row = self.connection.execute("SELECT state FROM metadata_state WHERE id=1").fetchone()
        if row is None:
            return None
        return decode(json.loads(row[0]))

    def save(self, state):
        data = json.dumps(encode(state))
        self.connection.execute("INSERT INTO metadata_state VALUES(1, ?) ON CONFLICT(id) DO UPDATE SET state=excluded.state", (data,))

    def close(self):
        self.connection.close()
