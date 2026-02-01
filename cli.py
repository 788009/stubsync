import sys
import time
import argparse
import signal
from loguru import logger
from pathlib import Path
import os

# 引入核心组件
from core.manager import BackupManager
from core.events import SyncEventListener
from core.utils import format_bytes, format_time

# --- 日志配置 ---
logger.remove()
logger.add(
    "log/stubsync_cli.log",
    rotation="5 MB",
    retention="10 days",
    level="DEBUG",
    encoding="utf-8",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{line} - {message}"
)
logger.add(
    sys.stdout,
    level="INFO",
    format="{message}",
    filter=lambda record: record["extra"].get("is_console", True)
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
            logger.info(f"正在尝试通过 {mode_name} 连接设备...")
        elif phase == "scanning":
            path = details.get('path', '未知路径') if details else '未知路径'
            logger.info(f"正在扫描远程目录: {path}")
        elif phase == "backup_start":
            logger.info(">>> 开始执行备份任务")
        elif phase == "finished":
            logger.info(">>> 任务流程结束")

    def on_device_verified(self, device_id, mode):
        self._clear_line()
        logger.info(f"✔ 设备验证成功 | ID: {device_id[:8]}... | 模式: {mode.upper()}")

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
            logger.info(line)

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

    def on_task_finished(self, stats, success):
        self._clear_line()
        duration = stats.duration
        
        logger.info("\n" + "="*40)
        # 根据是否成功完成，显示不同的标题
        if success:
            logger.info(f" 任务完成汇总 (耗时: {format_time(duration)})")
        else:
            logger.info(f" 任务中断汇总 (已运行: {format_time(duration)})")
            
        logger.info("="*40)
        logger.info(f" 扫描文件 : {stats.files_total} ({format_bytes(stats.bytes_total)})")
        logger.info(f" 传输成功 : {stats.files_copied} ({format_bytes(stats.bytes_copied)})")
        logger.info(f" 跳过文件 : {stats.files_skipped} ({format_bytes(stats.bytes_skipped)})")
        if stats.files_failed > 0:
            logger.warning(f" 传输失败 : {stats.files_failed}")
        
        logger.info(f" 本地删除 : {stats.files_deleted}")
            
        logger.info("="*40 + "\n")

    def on_error(self, title, message, critical=False):
        self._clear_line()
        level = "CRITICAL" if critical else "ERROR"
        logger.error(f"[{level}] {title}: {message}")

    def on_log(self, message, level="info"):
        self._clear_line()
        if level in ["error", "critical"]:
            logger.error(message)
        elif level == "warning":
            logger.warning(message)
        elif level == "info":
            pass

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