# core/utils.py
import time

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

class RateLimiter:
    """速率限制器，用于 SSH/SFTP/FTP 模式"""
    def __init__(self, max_mb_s):
        self.max_bytes_s = max_mb_s * 1024 * 1024
        self.last_time = time.time()
        self.transferred = 0

    def update(self, chunk_size):
        if self.max_bytes_s <= 0: return
        
        self.transferred += chunk_size
        current_time = time.time()
        elapsed = current_time - self.last_time
        
        if elapsed > 1.0:
            expected_time = self.transferred / self.max_bytes_s
            if expected_time > elapsed:
                sleep_time = expected_time - elapsed
                time.sleep(sleep_time)
            
            # Reset window
            self.last_time = time.time()
            self.transferred = 0

class SpeedCalculator:
    """
    单纯用于计算速度和剩余时间的工具类
    替代了原本进度条内部的计算逻辑
    """
    def __init__(self, total_size):
        self.start_time = time.time()
        self.total_size = total_size
        self.processed = 0
        self.last_update_time = time.time()
    
    def update(self, size_inc):
        self.processed += size_inc
    
    def get_metrics(self):
        current_time = time.time()
        elapsed = current_time - self.start_time
        if elapsed < 0.001: elapsed = 0.001
        
        speed = self.processed / elapsed
        remaining_bytes = self.total_size - self.processed
        if remaining_bytes < 0: remaining_bytes = 0
        
        eta = remaining_bytes / speed if speed > 0 else 0
        
        return speed, int(eta)