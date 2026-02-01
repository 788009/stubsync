import os
import shutil
import threading
import queue
import tarfile
import time
import re
from pathlib import Path
import uuid
from datetime import datetime
from loguru import logger
import posixpath

from .utils import format_bytes, SpeedCalculator
from .database import DatabaseManager
from .strategies import STRATEGY_MAP, ConnectionLostError, RemoteIOError
from .events import SyncEventListener
from .models import BackupStats, ProgressData, ScanResult

try:
    import tomllib
except ImportError:
    import tomli as tomllib

class BackupManager:
    def __init__(self, config_path, listener: SyncEventListener):
        self.listener = listener
        self.config_path = config_path
        self.load_config(config_path)
        
        self.is_interrupted = False
        self.stop_event = threading.Event()
        
        self.max_bytes = self.config.get('max_backup_size_mb', 0) * 1024 * 1024
        self.session_total_bytes = 0 
        self.quota_exceeded = False

        self.storage_root = Path(self.config['storage_root'])
        self.data_root = self.storage_root / 'data'
        self.trash_root = self.storage_root / '_trash'
        
        self.session_id = str(uuid.uuid4())[:8]
        self.temp_transfers = self.storage_root / 'temp_transfers' / self.session_id
        
        for p in [self.data_root, self.trash_root, self.temp_transfers]:
            p.mkdir(parents=True, exist_ok=True)

        db_path = self.storage_root / 'stub.db'
        self.db = DatabaseManager(str(db_path))
        self.stats = BackupStats()

        self.exclude_paths = []
        self.exclude_filenames = []
        if 'excludes' in self.config:
            try:
                self.exclude_paths = [re.compile(p) for p in self.config['excludes'].get('paths', [])]
                self.exclude_filenames = [re.compile(f) for f in self.config['excludes'].get('filenames', [])]
            except re.error as e:
                self.listener.on_error("配置错误", f"排除规则正则表达式错误: {e}", critical=True)
        
        # 用户修改：添加 only_print_changes 读取
        self.only_print_changes = self.config['advanced'].get('only_print_changes', False)
        self.strategy = None

    def load_config(self, path):
        try:
            with open(path, "rb") as f:
                self.config = tomllib.load(f)
        except Exception as e:
            self.listener.on_error("配置错误", f"无法加载配置文件: {e}", critical=True)
            raise

    def _init_strategy(self):
        modes = self.config['connection']['mode']
        if isinstance(modes, str): modes = [modes]
            
        for mode in modes:
            if self.stop_event.is_set(): return False
            self.listener.on_phase_changed("connecting", {"mode": mode})
            
            strategy_cls = STRATEGY_MAP.get(mode)
            if not strategy_cls: continue
                
            try:
                strategy = strategy_cls(self.config, self.listener)
                if strategy.connect():
                    self.strategy = strategy
                    self.current_mode = mode 
                    self.listener.on_device_verified("Unknown", mode)
                    return True
            except Exception as e:
                logger.error(f"Mode {mode} failed: {e}")
        
        self.listener.on_error("连接失败", "所有配置的连接模式均尝试失败。", critical=True)
        return False

    def verify_device_identity(self):
        id_file_path = self.config['device']['remote_id_path']
        local_id_path = self.storage_root / ".stubsync_id"
        
        if local_id_path.exists():
            local_id = local_id_path.read_text().strip()
        else:
            local_id = str(uuid.uuid4())
            local_id_path.write_text(local_id)

        try:
            remote_id_content = self.strategy.read_remote_file(id_file_path)
        except Exception as e:
            self.listener.on_error("验证失败", f"无法读取设备ID: {e}", critical=True)
            return False
        
        if not remote_id_content:
            self.listener.on_log(f"Remote ID not found, creating...", level="warning")
            success = self.strategy.write_remote_file(id_file_path, local_id)
            if not success:
                self.listener.on_error("权限错误", "无法写入远程 ID", critical=True)
                return False
            remote_id = local_id
        else:
            remote_id = remote_id_content.strip()

        if local_id == remote_id:
            self.listener.on_log(f"Device ID verified: {remote_id[:8]}")
            return True
        else:
            self.listener.on_error("设备不匹配", f"本地ID: {local_id}\n远程ID: {remote_id}", critical=True)
            return False

    def run(self):
        # 标记任务最终是否算作“成功完成”
        is_success = False
        try:
            if not self._init_strategy(): return
            if not self.verify_device_identity(): return

            self.listener.on_phase_changed("backup_start")
            
            for source in self.config['sources']:
                if self.stop_event.is_set(): break
                self.sync_root_folder(source)
            
            # 如果能运行到这里，且没有被中断，则视为成功
            if not self.stop_event.is_set():
                is_success = True
                self.listener.on_phase_changed("finished")
            else:
                self.listener.on_log("Task interrupted by user.", level="warning")
            
        except (ConnectionLostError, RemoteIOError) as e:
            if self.stop_event.is_set():
                # 如果 stop_event 被置位，说明是用户主动中断。
                # 此时底层抛出的 ConnectionLostError 通常是因为我们 kill 掉了子进程导致的，
                # 这不是错误，而是预期行为。
                logger.info("用户终止操作，正在停止...")
            else:
                # 只有在没有按下停止键的情况下发生的断连，才是真正的事故
                self.listener.on_error("同步中断", f"连接意外断开: {e}", critical=True)
        finally:
            # === 无论如何（包括 Ctrl+C），只要跑过 run，就打印统计 ===
            # 停止计时器
            self.stats.stop_timer()
            
            # 汇报最终统计结果
            # 如果是因为被中断(stop_event)导致退出，success 虽然是 False，
            # 但 stats 里依然包含了中断前已经处理的数据，这对用户很有价值。
            self.listener.on_task_finished(self.stats, success=is_success)

            self.cleanup()
            if self.strategy: self.strategy.disconnect()

    def _check_exclusion(self, full_path, filename):
        for p in self.exclude_paths:
            if p.search(full_path): return True
        for f in self.exclude_filenames:
            if f.search(filename): return True
        return False

    def sync_root_folder(self, root_source):
        db_root_prefix = root_source.lstrip('/')
        stack = [(root_source, db_root_prefix)]
        
        while stack:
            if self.stop_event.is_set(): break
            
            curr_rem_path, curr_db_prefix = stack.pop()
            # 注意：如果开启了 only_print_changes，这里我们还是先通知 Scanning，
            # 具体的 ScanResult 是否打印由 CLI 或 UI 自己决定，或者我们在后面控制 on_scan_finished 的触发。
            # 按照你的需求，我们是在计算出 has_change 后才决定是否调用 on_scan_finished。
            # 但 on_phase_changed('scanning') 通常用于让 UI 显示“正在干活”，建议保留，或者也加判定。
            # 这里为了保持用户体验（知道程序没死），建议保留，除非你希望极致静默。
            self.listener.on_phase_changed("scanning", {"path": curr_rem_path})
            
            try:
                entries = self.strategy.list_dir(curr_rem_path)
            except RemoteIOError:
                self.listener.on_log(f"Skipping inaccessible path: {curr_rem_path}", level="warning")
                continue

            local_records = self.db.get_folder_records(curr_db_prefix)
            
            # 初始化统计与动作列表
            to_download = []
            to_delete = []
            
            stat_rem_files = 0
            stat_rem_bytes = 0
            stat_sync_files = 0
            stat_sync_bytes = 0
            stat_skip_files = 0
            stat_skip_bytes = 0
            
            # --- 1. 遍历远程文件 (一次循环完成统计与填充) ---
            for name, (mtime, size, is_dir) in entries.items():
                full_rem_path = posixpath.join(curr_rem_path, name)
                rel_path = f"{curr_db_prefix}/{name}"
                
                # 排除检查
                if self._check_exclusion(full_rem_path, name):
                    continue
                
                if is_dir:
                    # 目录入栈，准备下一次循环
                    stack.append((full_rem_path, rel_path))
                    self.stats.add_dir()
                else:
                    # 文件处理
                    stat_rem_files += 1
                    stat_rem_bytes += size
                    
                    needs_sync = False
                    if name not in local_records:
                        needs_sync = True
                    else:
                        l_mtime, l_size = local_records[name]
                        if size != l_size or mtime > l_mtime:
                            needs_sync = True
                    
                    if needs_sync:
                        # 配额检查
                        if self.max_bytes > 0 and (self.session_total_bytes + size) > self.max_bytes:
                            if not self.quota_exceeded:
                                self.listener.on_log("Traffic quota exceeded.", level="warning")
                                self.quota_exceeded = True
                            self.stats.add_skipped(size)
                            stat_skip_files += 1
                            stat_skip_bytes += size
                            continue
                        
                        # 加入下载列表
                        to_download.append((name, full_rem_path, rel_path, mtime, size))
                        
                        # 更新统计
                        self.session_total_bytes += size
                        self.stats.add_scanned_file(size)
                        stat_sync_files += 1
                        stat_sync_bytes += size
                    else:
                        # 无需同步
                        self.stats.add_skipped(size)
                        stat_skip_files += 1
                        stat_skip_bytes += size

            # --- 2. 处理本地删除 ---
            remote_filenames = set(entries.keys())
            for loc_name in local_records:
                if '/' in loc_name: continue # 忽略子目录下的文件记录，只看当前层
                if loc_name not in remote_filenames:
                    rel_del = f"{curr_db_prefix}/{loc_name}"
                    to_delete.append(rel_del)

            # --- 3. 汇报与执行 ---
            
            # 用户逻辑：判断是否有变更
            has_change = (len(to_download) > 0) or (len(to_delete) > 0)
            
            if has_change or not self.only_print_changes:
                scan_res = ScanResult(
                    folder_path=curr_rem_path,
                    total_remote_files=stat_rem_files,
                    total_remote_bytes=stat_rem_bytes,
                    sync_files=stat_sync_files,
                    sync_bytes=stat_sync_bytes,
                    skip_files=stat_skip_files,
                    skip_bytes=stat_skip_bytes,
                    delete_files=len(to_delete)
                )
                self.listener.on_scan_finished(scan_res)

            if to_download:
                self._process_downloads(to_download)
            if to_delete:
                self._process_deletions(to_delete)

    def _process_downloads(self, file_list):
        queue_data = queue.Queue(maxsize=self.config['advanced'].get('queue_max_size', 3))
        total_size = sum(f[4] for f in file_list)
        speed_calc = SpeedCalculator(total_size)
        
        batch_limit = self.config['advanced'].get('batch_max_files', 100)
        char_limit = self.config['advanced'].get('batch_max_chars', 7000)
        
        batches = []
        current_batch = []
        current_chars = 0
        
        for item in file_list:
            path_len = len(item[1])
            if len(current_batch) >= batch_limit or (current_chars + path_len) > char_limit:
                batches.append(current_batch)
                current_batch = []
                current_chars = 0
            current_batch.append(item)
            current_chars += path_len
        if current_batch: batches.append(current_batch)

        def consumer():
            # 改为 while True，不再依赖 stop_event
            # 只要队列里有东西，就一直处理，直到拿到 None
            while True:
                try:
                    payload = queue_data.get(timeout=0.5)
                    if payload is None: 
                        break # 只有拿到哨兵才退出，确保处理完所有已下载数据
                    
                    temp_paths, metadata = payload
                    for tp in temp_paths:
                        self._install_files(tp, metadata)
                    queue_data.task_done()
                except queue.Empty:
                    continue
                except Exception: 
                    logger.exception("Consumer error")

        c_thread = threading.Thread(target=consumer, daemon=True)
        c_thread.start()

        current_downloaded = 0
        
        for batch in batches:
            if self.stop_event.is_set(): break
            
            download_items = [(x[0], x[1]) for x in batch]
            meta_map = {x[0]: (x[2], x[3], x[4]) for x in batch}
            
            def on_chunk_downloaded(chunk_size):
                nonlocal current_downloaded
                current_downloaded += chunk_size
                speed_calc.update(chunk_size)
                speed, eta = speed_calc.get_metrics()
                
                self.listener.on_progress(ProgressData(
                    phase="downloading",
                    current=current_downloaded,
                    total=total_size,
                    speed=speed,
                    eta=eta,
                    filename=batch[0][0] 
                ))

            try:
                temps = self.strategy.download(
                    download_items, 
                    self.temp_transfers, 
                    callback=on_chunk_downloaded, 
                    stop_signal=self.stop_event
                )
                
                queue_data.put((temps, meta_map))
                
            except Exception as e:
                logger.error(f"Download batch failed: {e}")
                self.stats.add_failed()
                if isinstance(e, (ConnectionLostError, RemoteIOError)):
                    self.stop_event.set(); queue_data.put(None); c_thread.join(); raise 

        queue_data.put(None)
        c_thread.join()

    def _install_files(self, temp_path, meta_map):
        db_updates = []
        p = Path(temp_path)
        try:
            if temp_path.endswith('.tar'):
                try:
                    with tarfile.open(temp_path, 'r') as tar:
                        for member in tar:
                            if member.isfile():
                                fname = os.path.basename(member.name)
                                if fname not in meta_map: continue
                                rel, mtime, size = meta_map[fname]
                                tgt = self.data_root / rel
                                tgt.parent.mkdir(parents=True, exist_ok=True)
                                f = tar.extractfile(member)
                                with open(tgt, 'wb') as out: shutil.copyfileobj(f, out)
                                os.utime(tgt, (time.time(), mtime))
                                db_updates.append((rel, mtime, size))
                                self.stats.add_copied(size)
                except tarfile.ReadError:
                    if p.exists():
                        with open(p, 'rb') as f_debug: head = f_debug.read(200)
                        logger.error(f"Tar corrupt! Header: {head}")
                    raise
            else:
                fname = p.name
                if fname in meta_map:
                    rel, mtime, size = meta_map[fname]
                    tgt = self.data_root / rel
                    tgt.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(p), str(tgt))
                    os.utime(tgt, (time.time(), mtime))
                    db_updates.append((rel, mtime, size))
                    self.stats.add_copied(size)
            if db_updates: self.db.update_batch(db_updates)
        except Exception as e:
            logger.exception(f"Install error: {e}")
            self.stats.add_failed()
        finally:
            if p.exists(): os.remove(p)

    def _process_deletions(self, delete_keys):
        behavior = self.config['advanced'].get('delete_behavior', 'trash')
        for rel in delete_keys:
            src = self.data_root / rel
            if src.exists():
                if behavior == 'trash':
                    trash_dir = self.trash_root / datetime.now().strftime("%Y-%m-%d")
                    dst = trash_dir / rel
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(src), str(dst))
                elif behavior == 'permanent': os.remove(src)
                self.stats.add_deleted_file()
        self.db.delete_batch(delete_keys)
        self.stats.add_deleted_row()
        self.listener.on_log(f"Deleted {len(delete_keys)} local files.")

    def stop(self):
        self.is_interrupted = True
        self.stop_event.set()
        self.listener.on_log("Stop signal received.", level="warning")

    def cleanup(self):
        if self.temp_transfers.exists():
            shutil.rmtree(self.temp_transfers, ignore_errors=True)