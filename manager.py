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
import uuid
from loguru import logger

# 导入
from utils import BackupStats, BatchProgressBar, format_bytes, format_time
from database import DatabaseManager
from strategies import STRATEGY_MAP, TransferStrategy

# 预定义仅输出到终端的 Logger (用于纯 UI 展示，不记录到文件)
console_only = logger.bind(to_file=False)

try:
    import tomllib
except ImportError:
    try:
        import tomli as tomllib
    except ImportError:
        # 这里虽然没有配置 logger，但 loguru 默认会输出到 stderr
        logger.bind(display="Error: Keep Python >= 3.11 OR 'pip install tomli'").error("Missing required library: tomllib/tomli")
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
        
        self.temp_root = self.storage_root / 'temp_transfers'
        self.db_file = self.storage_root / 'stub.db'
        
        for p in [self.temp_root, self.data_root]:
            p.mkdir(parents=True, exist_ok=True)
        
        if self.temp_root.exists(): shutil.rmtree(self.temp_root)
        self.temp_root.mkdir(parents=True, exist_ok=True)

        self.db = DatabaseManager(self.db_file)
        self.stats = BackupStats() 
        self.strategy = self._select_strategy()
        
        signal.signal(signal.SIGINT, self.handle_exit)
        signal.signal(signal.SIGTERM, self.handle_exit)

        if sys.platform == "win32":
            try:
                signal.signal(signal.SIGBREAK, self.handle_exit)
            except AttributeError:
                pass

    def load_config(self, path):
        if not os.path.exists(path):
            logger.bind(display=f"Config not found: {path}").error(f"Config file missing: {path}")
            sys.exit(1)
        with open(path, 'rb') as f: self.config = tomllib.load(f)
        adv = self.config.get('advanced', {})
        self.queue_size = adv.get('queue_max_size', 3)
        self.batch_max_files = adv.get('batch_max_files', 100)
        self.batch_max_chars = adv.get('batch_max_chars', 7000)
        self.delete_behavior = adv.get('delete_behavior', 'trash')
        self.only_print_changes = adv.get('only_print_changes', False)
        if 'excludes' not in self.config: self.config['excludes'] = {}

    def _select_strategy(self) -> TransferStrategy | None:
        conn_config = self.config.get('connection', {})
        modes = conn_config.get('mode', ['adb', 'ssh'])
        if isinstance(modes, str): modes = [modes]
        
        console_only.bind(display=f"连接策略: {' -> '.join(modes)}").info("Connection strategy sequence: {}", modes)
        for mode_name in modes:
            mode_key = mode_name.lower().strip()
            strategy_cls: TransferStrategy = STRATEGY_MAP.get(mode_key)
            if not strategy_cls: continue
            
            strategy_instance = strategy_cls(self.config)
            console_only.bind(display=f"正在尝试: {mode_key.upper()} ...").info(f"Attempting connection: {mode_key}")
            if strategy_instance.connect():
                logger.bind(display=f"成功连接到: {mode_key.upper()}").info(f"Successfully connected via {mode_key}")
                return strategy_instance
            else:
                logger.bind(display=f"{mode_key.upper()} 失败，尝试下一个...").warning(f"Connection failed: {mode_key}, trying next...")
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
    def producer_download(self, download_queue, files_to_pull, remote_base_folder):
        batches = list(self.smart_chunker(files_to_pull, remote_base_folder))
        total_batches = len(batches)

        try:
            for i, batch in enumerate(batches):
                if self.is_interrupted: break
                
                batch_num = i + 1
                batch_bytes = sum(item[1] for item in batch)
                
                batch_uuid = uuid.uuid4().hex
                batch_temp_dir = self.temp_root / batch_uuid
                batch_temp_dir.mkdir(parents=True, exist_ok=True)

                pbar = BatchProgressBar(batch_bytes, description=f"Batch {batch_num}/{total_batches}")
                
                # 执行下载
                downloaded_files = self.strategy.download(
                    batch, 
                    batch_temp_dir, 
                    callback=pbar.update,
                    stop_signal=lambda: self.is_interrupted
                )
                
                pbar.close()

                if self.is_interrupted: 
                    if batch_temp_dir.exists(): shutil.rmtree(batch_temp_dir, ignore_errors=True)
                    break

                if downloaded_files:                
                    valid_meta = []
                    actual_copied_size = 0
                    actual_copied_count = 0
                    
                    # 判断是 ADB Tar 包还是独立文件列表
                    # ADB 策略返回的是 [xxx.tar]，FTP/SSH 返回的是 [file1, file2...]
                    is_tar_packet = len(downloaded_files) == 1 and downloaded_files[0].name.endswith('.tar')

                    if is_tar_packet:
                        # 情况 A: Tar 包模式 (ADB)
                        # 假设 Tar 包只要生成了，里面就包含了该批次所有文件
                        # (ADB exec-out tar 如果中途失败，通常也是全量失败或截断，较难细粒度控制，暂按全量算)
                        valid_meta = [(item[4], item[0], item[1]) for item in batch]
                        actual_copied_size = batch_bytes
                        actual_copied_count = len(batch)
                    else:
                        # 情况 B: 独立文件模式 (FTP / SSH-SFTP)
                        # 必须过滤：只有在 downloaded_files 里存在的文件，才记录进数据库
                        
                        # 1. 提取所有下载成功的“文件名”
                        downloaded_names = set(f.name for f in downloaded_files)
                        
                        # 2. 遍历计划批次，只保留成功的
                        for item in batch:
                            # item 结构: (ts, size, full_path, fname, key)
                            fname = item[3]
                            if fname in downloaded_names:
                                valid_meta.append((item[4], item[0], item[1]))
                                actual_copied_size += item[1]
                                actual_copied_count += 1
                            else:
                                # 记录失败（虽然在这个批次里没报错，但没下载下来就是失败）
                                self.stats.add_failed(1)

                    # 只有当有有效文件时才提交
                    if valid_meta:
                        download_queue.put((downloaded_files, valid_meta, batch_temp_dir))
                        self.stats.add_copied(actual_copied_count, actual_copied_size)
                    else:
                        # 虽然 downloaded_files 不为空（可能产生了空文件），但匹配不到元数据，视为无效
                        if batch_temp_dir.exists(): shutil.rmtree(batch_temp_dir, ignore_errors=True)

                else:
                    # 整个批次全挂了
                    if batch_temp_dir.exists(): shutil.rmtree(batch_temp_dir, ignore_errors=True)
                    self.stats.add_failed(len(batch))

        finally:
            # 无论正常结束还是抛出异常，一定发送“结束哨兵”
            download_queue.put(None)

    def consumer_extract(self, download_queue, local_data_dir):
        if not local_data_dir.exists(): local_data_dir.mkdir(parents=True, exist_ok=True)
        
        while True:
            try:
                # 设置 0.5 秒超时，这样每 0.5 秒都有机会检查一次 self.is_interrupted
                item = download_queue.get(timeout=0.5)
            except queue.Empty:
                # 如果队列空了
                if self.is_interrupted:
                    # 且收到了停止信号，说明生产者已经挂了，消费者也该撤了
                    break
                # 没信号就继续轮询
                continue

            if item is None: break
            
            # files: 下载成功的本地临时文件路径列表
            # meta_info: Producer 筛选过的元数据列表 [(full_path, ts, size), ...]
            # batch_temp_dir: 这一批次的 UUID 临时目录
            files, meta_info, batch_temp_dir = item
            
            # 建立一个文件名到元数据的映射字典，方便查阅
            # meta_info 的结构是 (远程路径, 时间戳, 大小)
            # 我们需要通过 "文件名" 来关联它们
            # 假设远程路径最后一段是文件名
            meta_map = {Path(m[0]).name: m for m in meta_info}
            
            success_meta_to_db = []

            try:
                # 1. 如果是 Tar 包 (ADB 模式)
                if len(files) == 1 and files[0].name.endswith('.tar'):
                    tar_path = files[0]
                    try:
                        with tarfile.open(tar_path, 'r') as tar:
                            # 2. 解压
                            tar.extractall(path=local_data_dir)
                            
                            # 3. 解压后，遍历 meta_info，检查文件是否真的出现在了硬盘上
                            for name, meta in meta_map.items():
                                final_path = local_data_dir / name
                                if final_path.exists():
                                    success_meta_to_db.append(meta)
                                else:
                                    # 极其罕见：tar 包里居然没这个文件？
                                    logger.bind(display=f"文件丢失: {name} 未在 tar 包中发现").error(f"File missing in tar: {name}")
                                    self.stats.add_failed(1) # 修正统计
                    except Exception as e:
                        logger.bind(display=f"解压失败: {e}").exception(f"Tar extraction failed: {e}")
                        # 整个包都挂了，一个都不写数据库
                        self.stats.add_failed(len(meta_info))

                # 2. 如果是散文件 (FTP/SSH 模式)
                else:
                    for f_path in files:
                        f_path = Path(f_path)
                        fname = f_path.name
                        dest_path = local_data_dir / fname
                        
                        try:
                            # 移动文件
                            shutil.move(str(f_path), str(dest_path))
                            
                            # 只有移动没报错，才把这个文件的元数据加入“待写入名单”
                            if fname in meta_map:
                                success_meta_to_db.append(meta_map[fname])
                                
                        except Exception as e:
                            logger.bind(display=f"移动文件失败 {fname}: {e}").exception(f"Failed to move file {fname}: {e}")
                            self.stats.add_failed(1)

                # 4. 最终：只有真正落地的文件，才更新数据库
                if success_meta_to_db:
                    self.db.update_batch(success_meta_to_db)
                
            except Exception as e:
                logger.bind(display=f"消费者处理批次出错: {e}").exception(f"Consumer batch processing error: {e}")
            finally:
                # 清理 UUID 临时目录
                if batch_temp_dir and batch_temp_dir.exists():
                    shutil.rmtree(batch_temp_dir, ignore_errors=True)
                
                download_queue.task_done()

    def handle_sync_deletions(self, local_data_dir, keys_to_delete):
        if not keys_to_delete: return
        self.db.delete_batch(list(keys_to_delete))
        self.stats.rows_deleted += len(keys_to_delete)
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
                    self.stats.files_deleted += 1
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
                    logger.bind(display=f"\n配额已满: {fname}").warning(f"Quota exceeded at file: {fname}")
                    break
                
                self.session_total_bytes += size
                files_to_pull.append((ts, size, f"{remote_folder}/{fname}", fname, key))
                folder_stats['transfer_bytes'] += size

        folder_stats['transfer_count'] = len(files_to_pull)

        # 3. 输出文件夹信息 (只有当有变化，或者为了展示信息时才输出)
        has_changes = bool(files_to_pull or keys_to_delete)
        if has_changes or not self.only_print_changes:
            msg = (
                f"\n{'='*60}\n"
                f" 📂 正在处理目录: {remote_folder}\n"
                f"{'-' * 60}\n"
                f"    总文件数: {folder_stats['total_remote']:<8} |  需传输: {folder_stats['transfer_count']:<8}\n"
                f"    已跳过:   {folder_stats['skipped_count']:<8} |  需删除: {folder_stats['delete_count']:<8}\n"
                f"    预计传输大小: {format_bytes(folder_stats['transfer_bytes'])}\n"
                f"{'='*60}"
            )
            # 记录详细信息到日志，并显示到终端
            logger.bind(display=msg).info(f"Syncing folder: {remote_folder} | Transfer: {folder_stats['transfer_count']} files ({format_bytes(folder_stats['transfer_bytes'])})")

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
        if self.is_interrupted:
            # 如果已经是 True，说明用户按了第二次，直接强制退出
            console_only.bind(display="\n!!! 强制退出 !!!").warning("Force quitting...")
            # os._exit(1) 比 sys.exit(1) 更暴力，不抛出异常，直接杀掉进程，防止线程卡死
            os._exit(1) 
        else:
            # 第一次按，设置标志位，等待循环结束
            self.is_interrupted = True
            console_only.bind(display="\n\n正在停止... (再次按下 Ctrl+C 强制退出)").info("Signal received, stopping gracefully...")

    def print_summary(self):
        self.stats.finish()
        s = self.stats
        elapsed = s.end_time - s.start_time
        avg_speed = s.bytes_copied / elapsed if elapsed > 0 else 0
        
        display_msg = (
            f"\n\n{'#'*60}\n"
            f"备份任务摘要\n"
            f"{'-' * 60}\n"
            f"    总耗时:     {format_time(elapsed)}\n"
            f"    平均速度:   {format_bytes(avg_speed)}/s\n"
            f"{'-' * 60}\n"
            f"    目录扫描:   {s.dirs_total}\n"
            f"    文件总数:   {s.files_total}\n"
            f"    成功传输:   {s.files_copied} ({format_bytes(s.bytes_copied)})\n"
            f"    已跳过:     {s.files_skipped} ({format_bytes(s.bytes_skipped)})\n"
            f"    删除文件:   {s.files_deleted}\n"
            f"    删除条目:   {s.rows_deleted}\n"
            f"    失败:       {s.files_failed}\n"
            f"{'#' * 60}"
        )
        
        log_msg = (
            f"Backup Summary | Duration: {format_time(elapsed)} | Speed: {format_bytes(avg_speed)}/s | "
            f"Dirs: {s.dirs_total} | Files: {s.files_total} | Copied: {s.files_copied} ({format_bytes(s.bytes_copied)}) | "
            f"Skipped: {s.files_skipped} | Deleted: {s.files_deleted} | Failed: {s.files_failed}"
        )

        logger.bind(display=display_msg).info(log_msg)

    def _get_local_id(self):
        """读取电脑端的 ID"""
        id_file = self.storage_root / ".stubsync_id"
        if id_file.exists():
            return id_file.read_text(encoding='utf-8').strip()
        return None

    def _save_local_id(self, id_str):
        """写入电脑端的 ID"""
        id_file = self.storage_root / ".stubsync_id"
        id_file.write_text(id_str, encoding='utf-8')

    def verify_device_identity(self):
        """
        核心身份验证逻辑
        返回 True 表示验证通过（或已自动建立信任），False 表示验证失败拒绝操作
        """
        if not self.strategy: return False

        # 1. 获取配置的远程路径 (默认为 /sdcard/.stubsync_id)
        remote_id_path = self.config.get('device', {}).get('remote_id_path', '/sdcard/.stubsync_id')
        
        # 2. 读取两端 ID
        local_id = self._get_local_id()
        remote_id = self.strategy.read_remote_file(remote_id_path)
        
        logger.bind(display=f"身份验证 | 本地: {local_id} | 远程: {remote_id}").info(f"Identity Check | Local: {local_id} | Remote: {remote_id}")

        # ---------------------------------------------------------
        # 场景 A: 两边都没有 ID -> 首次初始化
        # ---------------------------------------------------------
        if not local_id and not remote_id:
            logger.bind(display="检测到首次运行，正在生成新设备 ID...").info("First run detected, generating new device ID...")
            new_id = uuid.uuid4().hex
            
            # 尝试写入两端
            if self.strategy.write_remote_file(remote_id_path, new_id):
                self._save_local_id(new_id)
                logger.bind(display=f"身份初始化成功！UUID: {new_id}").info(f"Identity initialized. UUID: {new_id}")
                return True
            else:
                logger.bind(display="无法写入手机端 ID 文件，请检查权限。").error("Failed to write remote ID file. Check permissions.")
                return False

        # ---------------------------------------------------------
        # 场景 B: 手机有 ID，电脑没有 -> 信任手机 (新电脑/新目录接入旧手机)
        # ---------------------------------------------------------
        if not local_id and remote_id:
            # 安全检查：如果本地没有 ID 文件，但数据库却很大，说明可能是配置丢失，需谨慎
            if self.db_file.exists() and self.db_file.stat().st_size > 16384:
                msg = (
                    "警告：本地存在数据库但没有 ID 文件。\n"
                    "为防止数据混淆，拒绝自动信任。请手动确认或删除本地数据库。"
                )
                logger.bind(display=msg).warning("Local DB exists without ID file. Automatic trust refused.")
                return False
            
            logger.bind(display=f"检测到远程设备 ID ({remote_id})，本地未配置，建立信任...").info(f"Adopting remote ID: {remote_id}")
            self._save_local_id(remote_id)
            return True

        # ---------------------------------------------------------
        # 场景 C: 电脑有 ID，手机没有 -> 拒绝 (防止误连新设备覆盖旧备份)
        # ---------------------------------------------------------
        if local_id and not remote_id:
            msg = (
                "严重错误：本地已有备份记录，但远程设备没有 ID 文件！\n"
                "  可能原因 1: 连接到了错误的设备/新设备。\n"
                "  可能原因 2: 手机端 ID 文件被误删。\n"
                "  --> 为保护现有备份，操作已中止。\n"
                f"  (若确认是同一设备，请手动在手机创建文件 {remote_id_path} 内容为: {local_id})"
            )
            logger.bind(display=msg).error(f"FATAL: Local ID exists ({local_id}) but remote is empty. Backup aborted to protect data.")
            return False

        # ---------------------------------------------------------
        # 场景 D: 两边都有 ID -> 正常核对
        # ---------------------------------------------------------
        if local_id == remote_id:
            console_only.bind(display="设备身份验证通过。").info("Device identity verified.")
            return True
        else:
            msg = (
                "FATAL: 设备身份不匹配！\n"
                f"  本地期望: {local_id}\n"
                f"  远程实际: {remote_id}"
            )
            logger.bind(display=msg).critical(f"Identity mismatch! Local: {local_id}, Remote: {remote_id}")
            return False

    def run(self):
        # 1. 选择策略并连接
        if not self.strategy: return
        
        try:
            # 2. 身份验证环节
            # 这里我们把验证放在 try 块里，确保出错能打印
            if not self.verify_device_identity():
                console_only.bind(display="身份验证失败，程序退出。").warning("Identity verification failed. Exiting.")
                return

            start_msg = f"正在启动备份... (流量限制: {format_bytes(self.max_bytes) if self.max_bytes > 0 else '无限制'})"
            console_only.bind(display=start_msg).info(f"Backup starting. Limit: {self.max_bytes}")
            
            for source in self.config['sources']:
                if self.is_interrupted: break
                self.sync_folder(source)
                
        except Exception as e:
            logger.bind(display=f"\n严重错误: {e}").exception(f"Critical error in run loop: {e}")
        finally:
            self.strategy.disconnect()
            if self.temp_root.exists(): shutil.rmtree(self.temp_root, ignore_errors=True)
            self.print_summary()