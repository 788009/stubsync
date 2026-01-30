import os
import time
import subprocess
import shlex
import logging
from abc import ABC, abstractmethod
from utils import RateLimiter
import ftplib
from datetime import datetime, timezone

# 尝试导入 paramiko
try:
    import paramiko
except ImportError:
    print("Warning: 'paramiko' not found. SSH mode will not work.")
    paramiko = None

logger = logging.getLogger(__name__)

class TransferStrategy(ABC):
    def __init__(self, config): self.config = config
    @abstractmethod
    def connect(self) -> bool: pass
    @abstractmethod
    def disconnect(self): pass
    @abstractmethod
    def list_files(self, remote_path) -> dict: pass
    @abstractmethod
    def download(self, items, temp_dir, callback=None, stop_signal=None) -> list: pass

class AdbStrategy(TransferStrategy):
    def connect(self):
        # 简单检测是否有设备
        res = subprocess.run(['adb', 'devices'], capture_output=True, text=True)
        return "device\n" in res.stdout or "\tdevice" in res.stdout

    def disconnect(self): pass

    def list_files(self, remote_path):
        # 【修改前】
        # cmd = ['adb', 'shell', 'stat', '-c', "'%Y/%F/%s/%n'", f"{remote_path}/*"]
        
        # 【修改后】使用 shlex.quote 包裹路径，防止空格截断
        # shlex.quote("/path/with space") -> "'/path/with space'"
        # 我们需要让 * 在引号外面，以便 shell 进行通配符展开
        safe_path = shlex.quote(remote_path)
        cmd = ['adb', 'shell', 'stat', '-c', "'%Y/%F/%s/%n'", f"{safe_path}/*"]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8')
            entries = {}
            for line in result.stdout.strip().split('\n'):
                line = line.strip("'")
                if not line or "No such file" in line: continue
                try:
                    parts = line.split('/', 3)
                    if len(parts) < 4: continue
                    ts, ftype, size = int(parts[0]), parts[1], int(parts[2])
                    fname = parts[3].split('/')[-1]
                    is_dir = 'directory' in ftype.lower()
                    entries[fname] = (ts, size, is_dir)
                except ValueError: continue
            return entries
        except Exception: return {}

    def download(self, items, temp_dir, callback=None, stop_signal=None):
        file_args = " ".join([shlex.quote(item[3]) for item in items])
        remote_base = os.path.dirname(items[0][2])
        tar_cmd = f"cd {shlex.quote(remote_base)} && tar -cf - {file_args}"
        full_cmd = ['adb', 'exec-out', tar_cmd]
        
        temp_tar_path = temp_dir / f"adb_batch_{time.time_ns()}.tar"
        
        try:
            process = subprocess.Popen(full_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            with open(temp_tar_path, 'wb') as f_out:
                while True:
                    if stop_signal and stop_signal():
                        process.terminate()
                        return []

                    chunk = process.stdout.read(64 * 1024) 
                    if not chunk: break
                    f_out.write(chunk)
                    if callback: callback(len(chunk))
            
            process.wait()
            if process.returncode == 0: return [temp_tar_path]
            return []
        except Exception as e:
            logger.error(f"ADB Download Error: {e}")
            return []

class SshStrategy(TransferStrategy):
    def __init__(self, config):
        super().__init__(config)
        self.ssh_conf = config.get('ssh', {})
        self.client = None
        self.sftp = None
        self.limiter = RateLimiter(self.ssh_conf.get('max_speed_mb', 0))

    def connect(self):
        if not paramiko: return False
        host = self.ssh_conf.get('host')
        port = self.ssh_conf.get('port', 8022)
        user = self.ssh_conf.get('username')
        pwd = self.ssh_conf.get('password')
        key = self.ssh_conf.get('key_filename')
        try:
            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            kw = {'hostname': host, 'port': port, 'username': user, 'banner_timeout': 30}
            if key and os.path.exists(key): kw['key_filename'] = key
            elif pwd:
                kw['password'] = pwd
                kw['look_for_keys'] = False
                kw['allow_agent'] = False
            self.client.connect(**kw, timeout=10)
            self.sftp = self.client.open_sftp()
            return True
        except Exception as e:
            logger.error(f"SSH Connect Error: {e}")
            return False

    def disconnect(self):
        if self.sftp: 
            try: self.sftp.close()
            except: pass
        if self.client: 
            try: self.client.close()
            except: pass

    def list_files(self, remote_path):
        if not self.sftp: return {}
        entries = {}
        try:
            for attr in self.sftp.listdir_attr(remote_path):
                fname = attr.filename
                if fname in ['.', '..']: continue
                is_dir = ('d' in str(attr.longname).split()[0])
                entries[fname] = (int(attr.st_mtime), attr.st_size, is_dir)
            return entries
        except FileNotFoundError: return {}
        except Exception: return {}

    def download(self, items, temp_dir, callback=None, stop_signal=None):
        if self.limiter.max_bytes_s > 0: 
            return self._download_sftp_throttled(items, temp_dir, callback, stop_signal)
        else: 
            return self._download_tar_stream(items, temp_dir, callback, stop_signal)

    def _download_sftp_throttled(self, items, temp_dir, callback, stop_signal):
        paths = []
        for item in items:
            if stop_signal and stop_signal(): return paths
            try:
                remote, fname = item[2], item[3]
                local = temp_dir / fname
                with self.sftp.open(remote, 'rb') as rf, open(local, 'wb') as lf:
                    while True:
                        if stop_signal and stop_signal(): return paths
                        chunk = rf.read(64 * 1024)
                        if not chunk: break
                        lf.write(chunk)
                        self.limiter.update(len(chunk))
                        if callback: callback(len(chunk))
                paths.append(local)
            except Exception: pass
        return paths

    def _download_tar_stream(self, items, temp_dir, callback, stop_signal):
        file_args = " ".join([shlex.quote(item[3]) for item in items])
        remote_base = os.path.dirname(items[0][2])
        cmd = f"cd {shlex.quote(remote_base)} && tar -cf - {file_args}"
        temp_tar_path = temp_dir / f"ssh_batch_{time.time_ns()}.tar"
        
        try:
            stdin, stdout, stderr = self.client.exec_command(cmd, get_pty=False)
            with open(temp_tar_path, 'wb') as f_out:
                while True:
                    if stop_signal and stop_signal(): return []
                    chunk = stdout.read(64 * 1024)
                    if not chunk: break
                    f_out.write(chunk)
                    if callback: callback(len(chunk))
            
            if stdout.channel.recv_exit_status() == 0: return [temp_tar_path]
            return []
        except Exception: return []

class FtpStrategy(TransferStrategy):
    def __init__(self, config):
        super().__init__(config)
        self.ftp_conf = config.get('ftp', {})
        self.ftp = None
        # FTP 不支持流式打包，只能逐个下载，复用限速器逻辑
        self.limiter = RateLimiter(self.ftp_conf.get('max_speed_mb', 0))

    def connect(self):
        host = self.ftp_conf.get('host')
        port = self.ftp_conf.get('port', 21)
        user = self.ftp_conf.get('username', 'anonymous')
        pwd = self.ftp_conf.get('password', '')

        if not host:
            logger.error("[FTP] Host not configured")
            return False

        try:
            self.ftp = ftplib.FTP()
            logger.info(f"[FTP] Connecting to {host}:{port}...")
            self.ftp.connect(host, port, timeout=10)
            self.ftp.login(user, pwd)
            # 强制 UTF-8，防止中文乱码 (RFC 2640)
            self.ftp.encoding = "utf-8"
            logger.info("[FTP] Connected.")
            return True
        except Exception as e:
            logger.error(f"[FTP] Connection failed: {e}")
            return False

    def disconnect(self):
        if self.ftp:
            try:
                self.ftp.quit()
            except:
                try: self.ftp.close()
                except: pass

    def list_files(self, remote_path):
        """
        增加了重试机制的 list_files
        """
        if not self.ftp: return {}
        
        # 定义一个内部函数用于执行 MLSD，方便重试
        def _do_mlsd():
            if remote_path == '/': cwd_path = '/'
            else: cwd_path = remote_path.rstrip('/')
            self.ftp.cwd(cwd_path)
            items = self.ftp.mlsd()
            entries = {}
            for name, facts in items:
                if name in ['.', '..']: continue
                is_dir = facts.get('type') == 'dir'
                size = int(facts.get('size', 0))
                mtime_str = facts.get('modify')
                ts = 0
                if mtime_str:
                    try:
                        dt = datetime.strptime(mtime_str, "%Y%m%d%H%M%S")
                        ts = int(dt.replace(tzinfo=timezone.utc).timestamp())
                    except ValueError: pass
                entries[name] = (ts, size, is_dir)
            return entries

        # 第一次尝试
        try:
            return _do_mlsd()
        except (ftplib.error_perm, ftplib.error_temp, TimeoutError, EOFError) as e:
            # 如果是权限错误(550)，通常是真的没这个目录，无需重试
            if "550" in str(e):
                logger.warning(f"[FTP] List failed (NoEntry): {e}")
                return {}
            
            # 其他错误（超时、断开、协议错乱），尝试重连一次
            logger.warning(f"[FTP] List failed ({e}), reconnecting...")
            self.disconnect()
            if self.connect():
                try:
                    return _do_mlsd()
                except Exception as retry_e:
                    logger.error(f"[FTP] Retry list failed: {retry_e}")
                    return {}
            else:
                return {}
        except Exception as e:
            logger.error(f"[FTP] Unknown list error: {e}")
            return {}

    def download(self, items, temp_dir, callback=None, stop_signal=None):
        paths = []
        
        class Interruption(Exception): pass

        for item in items:
            # 1. 检查中断
            if stop_signal and stop_signal(): return paths
            
            # 2. 检查连接存活，如果上一轮断了，这里尝试补救
            if not self.ftp:
                if not self.connect():
                    logger.error("[FTP] Cannot reconnect, skipping batch.")
                    break

            remote_full_path = item[2]
            fname = item[3]
            local_path = temp_dir / fname
            
            try:
                with open(local_path, 'wb') as f:
                    def write_and_check(data):
                        if stop_signal and stop_signal():
                            raise Interruption("Stopped by user")
                        f.write(data)
                        self.limiter.update(len(data))
                        if callback: callback(len(data))
                    
                    self.ftp.retrbinary(f"RETR {remote_full_path}", write_and_check, blocksize=64*1024)
                
                paths.append(local_path)
                
            except Interruption:
                return paths
            except Exception as e:
                # 遇到错误，强制重置连接
                logger.error(f"[FTP] Download error {fname}: {e}")
                
                # 删除可能下载了一半的损坏文件
                if local_path.exists():
                    try: os.remove(local_path)
                    except: pass
                
                # 记录日志并重连
                logger.warning(f"[FTP] Connection tainted. Reconnecting...")
                self.disconnect()
                # 尝试重新建立连接，以便下一个文件能成功
                # 注意：这里我们选择跳过当前出错的文件 (continue)，
                # 如果你想重试当前文件，可以用 while 循环包裹，但为了防死循环，跳过比较安全。
                self.connect()
                    
        return paths

STRATEGY_MAP = {
    'adb': AdbStrategy,
    'ssh': SshStrategy,
    'ftp': FtpStrategy
}