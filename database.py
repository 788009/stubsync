import sqlite3
import threading
from loguru import logger

class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.lock = threading.Lock()
        self._init_db()

    def _init_db(self):
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("""CREATE TABLE IF NOT EXISTS files (rel_path TEXT PRIMARY KEY, mtime INTEGER, size INTEGER)""")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_path ON files(rel_path);")
        except Exception as e:
            # 记录日志但不输出到终端，并抛出异常中止程序
            logger.exception(f"Database initialization failed: {e}")
            raise

    def get_folder_records(self, folder_prefix):
        search_pattern = folder_prefix + "/%"
        records = {}
        try:
            with self.lock, sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("SELECT rel_path, mtime, size FROM files WHERE rel_path LIKE ?", (search_pattern,))
                prefix_len = len(folder_prefix) + 1
                for r, m, s in cursor:
                    rem = r[prefix_len:]
                    if '/' not in rem: records[rem] = (m, s)
            return records
        except Exception as e:
            logger.exception(f"Failed to retrieve folder records for {folder_prefix}: {e}")
            raise

    def update_batch(self, meta_list):
        try:
            with self.lock, sqlite3.connect(self.db_path) as conn:
                conn.executemany("INSERT OR REPLACE INTO files VALUES (?, ?, ?)", meta_list)
        except Exception as e:
            logger.exception(f"Batch update failed: {e}")
            raise
    
    def delete_batch(self, keys):
        if not keys: return
        try:
            with self.lock, sqlite3.connect(self.db_path) as conn:
                sql = f"DELETE FROM files WHERE rel_path IN ({','.join('?'*len(keys))})"
                conn.execute(sql, keys)
        except Exception as e:
            logger.exception(f"Batch delete failed: {e}")
            raise