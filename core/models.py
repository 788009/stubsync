# core/models.py
import time
import threading
from dataclasses import dataclass, field
from typing import Optional

@dataclass
class ProgressData:
    """
    用于传输给 UI 的进度数据包
    包含纯数据，不包含显示逻辑
    """
    phase: str          # 当前阶段: "scanning", "downloading", "extracting"
    current: int        # 当前数值 (字节)
    total: int          # 总数值 (字节)
    speed: float        # 速度 (bytes/s)
    filename: str       # 当前正在处理的文件名
    eta: int = 0        # 剩余秒数估算

@dataclass
class ScanResult:
    """单个目录扫描后的统计结果"""
    folder_path: str
    total_remote_files: int
    total_remote_bytes: int
    
    # 需要传输 (新增/修改)
    sync_files: int
    sync_bytes: int
    
    # 跳过 (无需传输)
    skip_files: int
    skip_bytes: int
    
    # 删除 (本地有但远程无)
    delete_files: int

class BackupStats:
    """
    线程安全的统计计数器
    只负责计数，不负责打印
    """
    def __init__(self):
        self.start_time = time.time()
        self.end_time: Optional[float] = None
        
        self.dirs_total = 0
        self.files_total = 0      
        self.files_copied = 0     
        self.files_skipped = 0    
        self.files_failed = 0     
        self.files_deleted = 0
        self.rows_deleted = 0
        
        self.bytes_total = 0      
        self.bytes_copied = 0     
        self.bytes_skipped = 0    
        
        self._lock = threading.Lock()

    def stop_timer(self):
        self.end_time = time.time()
    
    @property
    def duration(self):
        end = self.end_time if self.end_time else time.time()
        return end - self.start_time

    # --- 线程安全的累加方法 ---
    
    def add_dir(self):
        with self._lock: self.dirs_total += 1

    def add_scanned_file(self, size):
        with self._lock:
            self.files_total += 1
            self.bytes_total += size

    def add_copied(self, size):
        with self._lock:
            self.files_copied += 1
            self.bytes_copied += size

    def add_skipped(self, size):
        with self._lock:
            self.files_skipped += 1
            self.bytes_skipped += size

    def add_failed(self):
        with self._lock: self.files_failed += 1

    def add_deleted_file(self):
        with self._lock: self.files_deleted += 1

    def add_deleted_row(self):
        with self._lock: self.rows_deleted += 1