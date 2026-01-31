import argparse
import os
import sys
from loguru import logger
from manager import BackupManager

# --- Loguru Global Configuration ---
logger.remove()  # 移除默认 handler

# 确保日志目录存在
os.makedirs("log", exist_ok=True)

# 1. 文件日志配置：英文，包含详细信息，忽略仅终端显示的日志
logger.add(
    os.path.join("log", "stubsync.log"),
    rotation="5 MB",
    retention="10 days",
    level="DEBUG",
    encoding="utf-8",
    filter=lambda record: record["extra"].get("to_file") is not False,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{line} - {message}"
)

# 2. 终端日志配置：中文，仅显示 display 内容，模拟 print 行为
logger.add(
    sys.stdout,
    level="INFO",
    filter=lambda record: "display" in record["extra"],
    format="{extra[display]}"
)

# 定义仅输出到终端的快捷方式 (用于纯 UI 提示)
console_only = logger.bind(to_file=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="StubSync - 维护本地索引的 Android 增量备份工具")
    parser.add_argument('-c', '--config', default='config.toml', help='Path to config file')
    args = parser.parse_args()
    
    if os.path.exists(args.config):
        # 记录启动信息到日志，并向终端打印 Banner
        console_only.bind(display="\nStubSync - 维护本地索引的 Android 增量备份工具\n").info("Application started, banner displayed.")
        
        try:
            app = BackupManager(args.config)
            app.run()
        except Exception as e:
            # 捕获未处理异常：终端显示中文简述，日志记录英文堆栈
            logger.bind(display=f"程序发生未知错误: {e}").exception(f"An unexpected error occurred: {e}")
            sys.exit(1)
    else:
        # 错误：终端显示中文，日志记录英文
        logger.bind(display=f"未找到配置文件: {args.config}").error(f"Config file not found: {args.config}")