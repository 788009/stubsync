import os
import sys
import shutil
import threading
import queue
import tarfile
import re
import shlex
import signal
import time
from datetime import datetime
from pathlib import Path

# 导入
from utils import BackupStats, BatchProgressBar, format_bytes, format_time, logger
from database import DatabaseManager
from strategies import STRATEGY_MAP, TransferStrategy

try:
    import tomllib
except ImportError:
    try:
        import tomli as tomllib
    except ImportError:
        print("Error: Keep Python >= 3.11 OR 'pip install tomli'")
        sys.exit(1)

class BackupManager:
    def __init__(self, config_path):
        self.load_config(config_path)
        
        self.is_interrupted = False
        self.quota_exceeded = False
        self.max_bytes = self.config.get('max_backup_size_mb', 0) * 1024 * 1024
        self.session_total_bytes = 0 

        self.storage_root = Path(self.config['storage_root'])
        self.data_root = self.storage_root / 'data'
        self.trash_root = self.storage_root / '_trash'
        self.temp_dir = self.storage_root / 'temp_transfers'
        self.db_file = self.storage_root / 'backup.db'
        
        for p in [self.temp_dir, self.data_root]:
            p.mkdir(parents=True, exist_ok=True)
        if self.temp_dir.exists(): shutil.rmtree(self.temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        self.db = DatabaseManager(self.db_file)
        self.stats = BackupStats() 
        self.strategy = self._select_strategy()
        
        signal.signal(signal.SIGINT, self.handle_exit)
        signal.signal(signal.SIGTERM, self.handle_exit)

    def load_config(self, path):
        if not os.path.exists(path):
            print(f"Config not found: {path}")
            sys.exit(1)
        with open(path, 'rb') as f: self.config = tomllib.load(f)
        adv = self.config.get('advanced', {})
        self.queue_size = adv.get('queue_max_size', 3)
        self.batch_max_files = adv.get('batch_max_files', 100)
        self.batch_max_chars = adv.get('batch_max_chars', 7000)
        self.delete_behavior = adv.get('delete_behavior', 'trash')
        if 'excludes' not in self.config: self.config['excludes'] = {}

    def _select_strategy(self) -> TransferStrategy | None:
        conn_config = self.config.get('connection', {})
        modes = conn_config.get('mode', ['adb', 'ssh'])
        if isinstance(modes, str): modes = [modes]
        
        print(f"连接策略: {' -> '.join(modes)}")
        for mode_name in modes:
            mode_key = mode_name.lower().strip()
            strategy_cls: TransferStrategy = STRATEGY_MAP.get(mode_key)
            if not strategy_cls: continue
            
            strategy_instance = strategy_cls(self.config)
            print(f"正在尝试: {mode_key.upper()} ...")
            if strategy_instance.connect():
                print(f"成功连接到: {mode_key.upper()}")
                return strategy_instance
            else:
                print(f"{mode_key.upper()} 失败，尝试下一个...")
        return None

    def should_exclude(self, remote_full_path, filename):
        for pattern in self.config['excludes'].get('paths', []):
            if re.search(pattern, remote_full_path): return True
        for pattern in self.config['excludes'].get('filenames', []):
            if re.search(pattern, filename): return True
        return False

    def smart_chunker(self, files_to_pull, remote_base_folder):
        current_batch = []
        base_cmd_len = 100
        current_cmd_len = base_cmd_len
        for item in files_to_pull:
            fname = item[3]
            arg_len = len(shlex.quote(fname)) + 1 
            is_count_limit = len(current_batch) >= self.batch_max_files
            is_char_limit = (current_cmd_len + arg_len) > self.batch_max_chars
            if (is_count_limit or is_char_limit) and current_batch:
                yield current_batch
                current_batch = []
                current_cmd_len = base_cmd_len
            current_batch.append(item)
            current_cmd_len += arg_len
        if current_batch: yield current_batch

    # 生产者下载逻辑
    def producer_download(self, download_queue: queue.Queue, files_to_pull, remote_base_folder):
        # 将整个列表分批
        batches = list(self.smart_chunker(files_to_pull, remote_base_folder))
        total_batches = len(batches)

        for i, batch in enumerate(batches):
            if self.is_interrupted: break
            
            batch_num = i + 1
            batch_bytes = sum(item[1] for item in batch)
            
            # 创建独立的进度条
            pbar = BatchProgressBar(batch_bytes, description=f"Batch {batch_num}/{total_batches}")
            
            # 下载并传入回调
            downloaded_files = self.strategy.download(
                batch, 
                self.temp_dir, 
                callback=pbar.update,
                stop_signal=lambda: self.is_interrupted
            )
            
            # 结束进度条（换行）
            pbar.close()

            if self.is_interrupted: break

            if downloaded_files:
                meta_info = [(item[4], item[0], item[1]) for item in batch]
                download_queue.put((downloaded_files, meta_info))
                
                # 更新全局统计
                self.stats.add_copied(len(batch), batch_bytes)
            else:
                self.stats.add_failed(len(batch))

        download_queue.put(None)

    def consumer_extract(self, download_queue: queue.Queue, local_data_dir):
        if not local_data_dir.exists(): local_data_dir.mkdir(parents=True, exist_ok=True)
        while True:
            item = download_queue.get()
            if item is None: break
            files, meta_info = item
            try:
                for f_path in files:
                    f_path = Path(f_path)
                    if self.is_interrupted:
                        if f_path.exists(): os.remove(f_path)
                        continue
                    if f_path.name.endswith('.tar'):
                        with tarfile.open(f_path, 'r') as tar: tar.extractall(path=local_data_dir)
                        os.remove(f_path)
                    else:
                        dest_path = local_data_dir / f_path.name
                        shutil.move(str(f_path), str(dest_path))
                self.db.update_batch(meta_info)
            except Exception: pass
            finally: download_queue.task_done()

    def handle_sync_deletions(self, local_data_dir, keys_to_delete):
        if not keys_to_delete: return
        self.db.delete_batch(list(keys_to_delete))
        rel_path = local_data_dir.relative_to(self.data_root)
        trash_dir = self.trash_root / rel_path
        if self.delete_behavior == 'trash': trash_dir.mkdir(parents=True, exist_ok=True)
        for key in keys_to_delete:
            fname = os.path.basename(key)
            local_file = local_data_dir / fname
            if local_file.exists():
                try:
                    if self.delete_behavior == 'trash': shutil.move(str(local_file), str(trash_dir / fname))
                    else: os.remove(local_file)
                except: pass

    # 同步逻辑与信息输出
    def sync_folder(self, remote_folder: str):
        if self.is_interrupted or self.quota_exceeded: return

        rel_folder_path = remote_folder.lstrip('/')
        local_data_dir = self.data_root / rel_folder_path
        self.stats.add_dir()
        
        # 1. 获取远程列表
        remote_entries = self.strategy.list_files(remote_folder)
        remote_files = {} 
        sub_dirs = []
        
        # 统计变量
        folder_stats = {
            'total_remote': 0,
            'total_size': 0,
            'skipped_count': 0,
            'delete_count': 0,
            'transfer_count': 0,
            'transfer_bytes': 0
        }

        for fname, (ts, size, is_dir) in remote_entries.items():
            full_path = f"{remote_folder}/{fname}"
            if self.should_exclude(full_path, fname): continue
            if is_dir: 
                sub_dirs.append(full_path)
            else: 
                remote_files[fname] = (ts, size)
                folder_stats['total_remote'] += 1
                folder_stats['total_size'] += size
                self.stats.add_file_found(size)

        # 2. 对比数据库
        db_records = self.db.get_folder_records(rel_folder_path)
        files_to_pull = []
        keys_to_delete = []

        for fname_in_db in db_records:
            if fname_in_db not in remote_files:
                keys_to_delete.append(f"{rel_folder_path}/{fname_in_db}")
        
        folder_stats['delete_count'] = len(keys_to_delete)
        self.handle_sync_deletions(local_data_dir, keys_to_delete)

        for fname, (ts, size) in remote_files.items():
            key = f"{rel_folder_path}/{fname}"
            should_download = False
            if fname not in db_records: should_download = True 
            else:
                db_ts, db_size = db_records[fname]
                if abs(ts - db_ts) > 2: should_download = True
                else: 
                    self.stats.add_skipped(size)
                    folder_stats['skipped_count'] += 1
            
            if should_download:
                # 检查配额
                if self.max_bytes > 0 and (self.session_total_bytes + size) > self.max_bytes:
                    self.quota_exceeded = True
                    print(f"\n配额已满: {fname}")
                    break
                
                self.session_total_bytes += size
                files_to_pull.append((ts, size, f"{remote_folder}/{fname}", fname, key))
                folder_stats['transfer_bytes'] += size

        folder_stats['transfer_count'] = len(files_to_pull)

        # 3. 输出文件夹信息 (只有当有变化，或者为了展示信息时才输出)
        # 如果你希望只要扫描了就输出，保留下面这段。
        # 如果只想在有下载任务时输出，加上 if files_to_pull or keys_to_delete:
        print("\n" + "="*60)
        print(f" 📂 正在处理目录: {remote_folder}")
        print("-" * 60)
        print(f"    总文件数: {folder_stats['total_remote']:<8} |  需传输: {folder_stats['transfer_count']:<8}")
        print(f"    已跳过:   {folder_stats['skipped_count']:<8} |  需删除: {folder_stats['delete_count']:<8}")
        print(f"    预计传输大小: {format_bytes(folder_stats['transfer_bytes'])}")
        print("="*60)

        # 4. 开始下载
        if files_to_pull and not self.is_interrupted:
            files_to_pull.sort(key=lambda x: x[0])
            dl_queue = queue.Queue(maxsize=self.queue_size) 
            extractor = threading.Thread(target=self.consumer_extract, args=(dl_queue, local_data_dir))
            extractor.daemon = True 
            extractor.start()
            
            self.producer_download(dl_queue, files_to_pull, remote_folder)
            extractor.join()

        # 5. 递归
        if not self.quota_exceeded:
            for sub in sub_dirs:
                if self.is_interrupted: break
                self.sync_folder(sub)

    def handle_exit(self, signum, frame):
        print("\n\n正在停止...")
        self.is_interrupted = True

    def print_summary(self):
        self.stats.finish()
        s = self.stats
        elapsed = s.end_time - s.start_time
        avg_speed = s.bytes_copied / elapsed if elapsed > 0 else 0
        
        print("\n\n" + "#"*60)
        print(f"备份任务摘要")
        print("-" * 60)
        print(f"    总耗时:     {format_time(elapsed)}")
        print(f"    平均速度:   {format_bytes(avg_speed)}/s")
        print("-" * 60)
        print(f"    目录扫描:   {s.dirs_total}")
        print(f"    文件总数:   {s.files_total}")
        print(f"    成功传输:   {s.files_copied} ({format_bytes(s.bytes_copied)})")
        print(f"    已跳过:     {s.files_skipped} ({format_bytes(s.bytes_skipped)})")
        print(f"    失败:       {s.files_failed}")
        print("#" * 60)

    def run(self):
        if not self.strategy: return
        print(f"正在启动备份... (流量限制: {format_bytes(self.max_bytes) if self.max_bytes > 0 else '无限制'})")
        try:
            for source in self.config['sources']:
                if self.is_interrupted: break
                self.sync_folder(source)
        except Exception as e:
            print(f"\n严重错误: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.strategy.disconnect()
            if self.temp_dir.exists(): shutil.rmtree(self.temp_dir, ignore_errors=True)
            self.print_summary()