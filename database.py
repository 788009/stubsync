import sqlite3
import threading

class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.lock = threading.Lock()
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("""CREATE TABLE IF NOT EXISTS files (rel_path TEXT PRIMARY KEY, mtime INTEGER, size INTEGER)""")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_path ON files(rel_path);")

    def get_folder_records(self, folder_prefix):
        search_pattern = folder_prefix + "/%"
        records = {}
        with self.lock, sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT rel_path, mtime, size FROM files WHERE rel_path LIKE ?", (search_pattern,))
            prefix_len = len(folder_prefix) + 1
            for r, m, s in cursor:
                rem = r[prefix_len:]
                if '/' not in rem: records[rem] = (m, s)
        return records

    def update_batch(self, meta_list):
        with self.lock, sqlite3.connect(self.db_path) as conn:
            conn.executemany("INSERT OR REPLACE INTO files VALUES (?, ?, ?)", meta_list)
    
    def delete_batch(self, keys):
        if not keys: return
        with self.lock, sqlite3.connect(self.db_path) as conn:
            sql = f"DELETE FROM files WHERE rel_path IN ({','.join('?'*len(keys))})"
            conn.execute(sql, keys)