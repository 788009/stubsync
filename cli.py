import sys
import time
import argparse
import signal
from loguru import logger
from pathlib import Path
import os

from core.manager import BackupManager
from core.events import SyncEventListener
from core.utils import format_bytes, format_time

# === 关键配置：移除控制台输出，只保留文件日志 ===
logger.remove()
logger.add(
    "log/stubsync_cli.log", 
    rotation="10 MB", 
    level="DEBUG", 
    encoding="utf-8",
    enqueue=True
)

class CliEventListener(SyncEventListener):
    def __init__(self):
        self.last_phase = ""
        self.progress_bar_active = False

    def _clear_line(self):
        if self.progress_bar_active:
            sys.stdout.write("\r" + " " * 100 + "\r")
            sys.stdout.flush()
            self.progress_bar_active = False

    def on_phase_changed(self, phase, details=None):
        self._clear_line()
        self.last_phase = phase
        
        if phase == "connecting":
            mode_name = details.get('mode', 'Unknown').upper()
            print(f"正在尝试通过 {mode_name} 连接设备...")
        elif phase == "scanning":
            path = details.get('path', '未知路径') if details else '未知路径'
            print(f"正在扫描远程目录: {path}")
        elif phase == "backup_start":
            print(">>> 开始执行备份任务")
        elif phase == "finished":
            print(">>> 任务流程结束")

    def on_device_verified(self, device_id, mode):
        self._clear_line()
        print(f"✔ 设备验证成功 | ID: {device_id[:8]}... | 模式: {mode.upper()}")

    def on_scan_finished(self, result):
        self._clear_line()
        
        def _fmt(count, size):
            return f"{count:>4} ({format_bytes(size)})"

        lines = []
        lines.append("-" * 60)
        lines.append(f" 目录总览: {result.total_remote_files} 文件 | {format_bytes(result.total_remote_bytes)}")
        lines.append("-" * 60)
        lines.append(f" [+] 待传输: {_fmt(result.sync_files, result.sync_bytes)}")
        lines.append(f" [=] 已跳过: {_fmt(result.skip_files, result.skip_bytes)}")
        lines.append(f" [-] 待删除: {result.delete_files:>4} (本地)")
        lines.append("-" * 60)
        
        for line in lines:
            print(line)

    def on_progress(self, data):
        percent = (data.current / data.total * 100) if data.total > 0 else 0
        bar_len = 25
        filled_len = int(bar_len * percent // 100)
        bar = '=' * filled_len + '>' + ' ' * (bar_len - filled_len - 1)
        
        fname = data.filename
        if len(fname) > 20: fname = "..." + fname[-17:]

        msg = (f"\rSyncing: [{bar}] {percent:5.1f}% | "
               f"{format_bytes(data.current)}/{format_bytes(data.total)} | "
               f"{format_bytes(data.speed)}/s | ETA: {format_time(data.eta)} | {fname}")
        
        sys.stdout.write(msg)
        sys.stdout.flush()
        self.progress_bar_active = True

    def on_file_processed(self, filename, status):
        pass

    def on_log(self, message, level="info"):
        self._clear_line()
        # 简单的日志转发到屏幕
        if level == "info":
            pass # print(message)
        elif level == "warning":
            print(f"[WARNING] {message}")
        elif level == "error":
            print(f"[ERROR] {message}")

    def on_error(self, title, message, critical=False):
        self._clear_line()
        level = "CRITICAL" if critical else "ERROR"
        # 终端显示
        print(f"[{level}] {title}: {message}")
        # 同时记录到文件（因为这是严重错误，值得保留）
        logger.error(f"{title}: {message}")

    def on_task_finished(self, stats, success):
        self._clear_line()
        duration = stats.duration
        
        print("\n" + "="*40)
        # 根据是否成功完成，显示不同的标题
        if success:
            print(f" 任务完成汇总 (耗时: {format_time(duration)})")
        else:
            print(f" 任务中断汇总 (已运行: {format_time(duration)})")
            
        print("="*40)
        print(f" 扫描文件 : {stats.files_total} ({format_bytes(stats.bytes_total)})")
        print(f" 传输成功 : {stats.files_copied} ({format_bytes(stats.bytes_copied)})")
        print(f" 跳过文件 : {stats.files_skipped} ({format_bytes(stats.bytes_skipped)})")
        if stats.files_failed > 0:
            print(f" 传输失败 : {stats.files_failed}")
        
        print(f" 本地删除 : {stats.files_deleted}")
            
        print("="*40 + "\n")

def main():
    parser = argparse.ArgumentParser(description="StubSync CLI")
    parser.add_argument('-c', '--config', default='config.toml', help='Path to config file')
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        logger.error(f"配置文件未找到: {config_path}")
        return

    cli_listener = CliEventListener()
    manager = None # 提前声明
    interrupt_count = 0 # 计数器

    def signal_handler(sig, frame):
        nonlocal interrupt_count
        interrupt_count += 1
        
        sig_name = "Unknown"
        try: sig_name = signal.Signals(sig).name
        except: pass

        if interrupt_count == 1:
            logger.warning(f"\n[!] 捕获信号 {sig_name}，正在停止... (再次按下强制退出)")
            if manager: manager.stop()
        elif interrupt_count >= 2:
            logger.critical(f"\n[!] 收到第二次中断信号，强制退出进程！")
            # 暴力退出，防止死锁
            os._exit(1)

    # 1. 注册信号 (覆盖 SIGINT, SIGTERM, SIGBREAK)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    if sys.platform == 'win32' and hasattr(signal, 'SIGBREAK'):
        signal.signal(signal.SIGBREAK, signal_handler)

    try:
        manager = BackupManager(str(config_path), cli_listener)
        manager.run()
    except Exception as e:
        logger.exception(f"程序运行失败: {e}")

if __name__ == "__main__":
    main()