import time
import sys
import threading
from loguru import logger

# 预定义仅终端输出
console_only = logger.bind(to_file=False)

def format_bytes(size):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"

def format_time(seconds):
    if seconds is None or seconds < 0: return "0:00:00"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}"

# 全局统计（仅用于最后 summary）
class BackupStats:
    def __init__(self):
        self.start_time = time.time()
        self.end_time = None
        
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
        
        self.lock = threading.Lock()

    def add_dir(self):
        with self.lock: self.dirs_total += 1

    def add_file_found(self, size):
        with self.lock:
            self.files_total += 1
            self.bytes_total += size

    def add_skipped(self, size):
        with self.lock:
            self.files_skipped += 1
            self.bytes_skipped += size

    def add_copied(self, count, size):
        with self.lock: 
            self.files_copied += count
            self.bytes_copied += size

    def add_failed(self, count):
        with self.lock: self.files_failed += count

    def finish(self):
        self.end_time = time.time()

# 单批次进度条（视觉层）
class BatchProgressBar:
    def __init__(self, total_size, description="Transmitting"):
        self.total_size = total_size
        self.current_size = 0
        self.start_time = time.time()
        self.last_print_time = 0
        self.description = description
        self.lock = threading.Lock()
        
        # 立即打印 0%
        self._print()

    def update(self, chunk_size):
        with self.lock:
            self.current_size += chunk_size
            now = time.time()
            # 节流：每 0.1 秒刷新一次，或者任务完成时刷新
            if (now - self.last_print_time >= 0.1) or (self.current_size >= self.total_size):
                self.last_print_time = now
                self._print()

    def _print(self):
        elapsed = time.time() - self.start_time
        speed = self.current_size / elapsed if elapsed > 0 else 0
        
        percent = 0
        if self.total_size > 0:
            percent = (self.current_size / self.total_size) * 100
        
        remaining = self.total_size - self.current_size
        eta = remaining / speed if speed > 0 else 0
        
        # 进度条样式: [=====>    ]
        bar_len = 25
        filled_len = int(bar_len * percent // 100)
        bar = '=' * filled_len + '>' + ' ' * (bar_len - filled_len - 1)
        
        msg = (f"\r{self.description}: [{bar}] {percent:5.1f}% | "
               f"{format_bytes(self.current_size)}/{format_bytes(self.total_size)} | "
               f"{format_bytes(speed)}/s | ETA: {format_time(eta)}   ")
        
        # 使用 console_only 配合 raw=True 来模拟 sys.stdout.write
        # raw=True 会忽略格式化字符串，直接输出 msg
        # bind(display="pbar") 是为了通过 main.py 中的过滤器 (filter=lambda r: "display" in r["extra"])
        console_only.bind(display="pbar").opt(raw=True).info(msg)
        sys.stdout.flush()

    def close(self):
        # 强制打印 100% 并换行
        self._print()
        # 输出换行
        console_only.bind(display="pbar_end").opt(raw=True).info("\n")
        sys.stdout.flush()

class RateLimiter:
    def __init__(self, max_mb_s):
        self.max_bytes_s = max_mb_s * 1024 * 1024
        self.last_time = time.time()
        self.transferred = 0

    def update(self, chunk_size):
        if self.max_bytes_s <= 0: return
        self.transferred += chunk_size
        current_time = time.time()
        elapsed = current_time - self.last_time
        expected_time = self.transferred / self.max_bytes_s
        if expected_time > elapsed:
            time.sleep(expected_time - elapsed)
        if elapsed > 10.0:
            self.last_time = time.time()
            self.transferred = 0