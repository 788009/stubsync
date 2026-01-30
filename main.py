import argparse
import os
from manager import BackupManager

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Android Incremental Backup Tool")
    parser.add_argument('-c', '--config', default='config.toml', help='Path to config file')
    args = parser.parse_args()
    
    if os.path.exists(args.config):
        app = BackupManager(args.config)
        app.run()
    else:
        print(f"Config file not found: {args.config}")