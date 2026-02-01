import os
import time
import subprocess
import shlex
import io
import ftplib
import tempfile
import posixpath
from abc import ABC, abstractmethod
from loguru import logger
from .utils import RateLimiter
from .events import SyncEventListener

try:
    import paramiko
except ImportError:
    paramiko = None

class ConnectionLostError(Exception): pass
class RemoteIOError(Exception): pass

class TransferStrategy(ABC):
    def __init__(self, config, listener: SyncEventListener):
        self.config = config
        self.listener = listener
    
    @abstractmethod
    def connect(self) -> bool: pass
    @abstractmethod
    def disconnect(self): pass
    
    @abstractmethod
    def list_dir(self, remote_path) -> dict: pass

    @abstractmethod
    def download(self, items, temp_dir, callback=None, stop_signal=None) -> list:
        """
        callback: function(bytes_count), 用于实时汇报下载进度
        """
        pass

    @abstractmethod
    def read_remote_file(self, remote_path) -> str: pass
    @abstractmethod
    def write_remote_file(self, remote_path, content) -> bool: pass

class AdbStrategy(TransferStrategy):
    def connect(self):
        try:
            res = subprocess.run(['adb', 'devices'], capture_output=True, encoding='utf-8', errors='ignore')
            return "device\n" in res.stdout or "\tdevice" in res.stdout
        except FileNotFoundError:
            self.listener.on_error("环境错误", "未找到 adb 命令", critical=True)
            return False

    def disconnect(self): pass 

    def list_dir(self, remote_path):
        cmd = ['adb', 'shell', f"find '{remote_path}' -maxdepth 1 -exec stat -c '%n|%Y|%s|%F' {{}} +"]
        res = subprocess.run(cmd, capture_output=True, encoding='utf-8', errors='ignore')
        if res.returncode != 0:
            if "No such file" in res.stderr: raise RemoteIOError(f"Path not found: {remote_path}")
            raise ConnectionLostError(f"ADB Error: {res.stderr}")
        
        entries = {}
        clean_root = remote_path.rstrip('/')
        for line in res.stdout.splitlines():
            try:
                parts = line.strip().split('|')
                if len(parts) < 4: continue
                fpath, mtime, size, ftype = parts[0], int(parts[1]), int(parts[2]), parts[3]
                if fpath.rstrip('/') == clean_root: continue
                name = os.path.basename(fpath)
                is_dir = ('directory' in ftype.lower())
                entries[name] = (mtime, size, is_dir)
            except ValueError: continue
        return entries

    def read_remote_file(self, remote_path):
        cmd = ["adb", "shell", f"cat \"{remote_path}\"; echo separator$?"]
        res = subprocess.run(cmd, capture_output=True, encoding='utf-8', errors='ignore')
        out = res.stdout if res.stdout else ""
        if "separator0" in out: return out.split("separator0")[0]
        elif "separator1" in out: return None
        else: raise ConnectionLostError("ADB interrupted.")

    def write_remote_file(self, remote_path, content):
        tmp_fd, tmp_path = tempfile.mkstemp()
        try:
            with os.fdopen(tmp_fd, 'w', encoding='utf-8') as f: f.write(content)
            res = subprocess.run(['adb', 'push', tmp_path, remote_path], capture_output=True)
            return res.returncode == 0
        finally:
            if os.path.exists(tmp_path): os.remove(tmp_path)

    def download(self, items, temp_dir, callback=None, stop_signal=None):
        if not items: return []
        paths = [f'"{full_path}"' for _, full_path in items]
        file_list_str = ' '.join(paths)
        tar_path = temp_dir / f"{int(time.time())}_{id(items)}.tar"
        
        cmd = ['adb', 'exec-out', f"tar -cf - {file_list_str} 2>/dev/null"]
        
        try:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            with open(tar_path, 'wb') as f:
                while True:
                    if stop_signal and stop_signal.is_set():
                        process.terminate()
                        break
                    
                    # 使用较小的 buffer 以获得更平滑的进度更新
                    chunk = process.stdout.read(32768) 
                    if not chunk: break
                    f.write(chunk)
                    
                    # --- 进度回调 ---
                    if callback: callback(len(chunk))
            
            process.wait()
            
            # 如果是用户叫停，直接返回空，不要抛错
            if stop_signal and stop_signal.is_set():
                return []

            stderr = process.stderr.read().decode('utf-8', errors='ignore')
            if process.returncode != 0:
                raise ConnectionLostError(f"ADB download failed (code {process.returncode}): {stderr}")
            if os.path.getsize(tar_path) == 0:
                 raise ConnectionLostError("ADB download produced empty file.")

            return [str(tar_path)]
        except Exception as e:
            if os.path.exists(tar_path): os.remove(tar_path)
            if stop_signal and stop_signal.is_set():
                return []
            raise e

class SshStrategy(TransferStrategy):
    def connect(self):
        if not paramiko: return False
        try:
            self.ssh = paramiko.SSHClient()
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            cfg = self.config['ssh']
            kw = {'hostname': cfg['host'], 'port': cfg.get('port', 8022), 'username': cfg['username'], 'timeout': 10}
            if cfg.get('password'): kw['password'] = cfg['password']
            if cfg.get('key_filename'): kw['key_filename'] = cfg['key_filename']
            self.ssh.connect(**kw)
            self.sftp = self.ssh.open_sftp()
            self.speed_limit = cfg.get('max_speed_mb', 0)
            return True
        except Exception: return False

    def disconnect(self):
        try: self.sftp.close(); self.ssh.close()
        except: pass

    def list_dir(self, remote_path):
        entries = {}
        try:
            for attr in self.sftp.listdir_attr(remote_path):
                if attr.filename in ['.', '..']: continue
                import stat
                is_dir = stat.S_ISDIR(attr.st_mode)
                entries[attr.filename] = (int(attr.st_mtime), attr.st_size, is_dir)
        except FileNotFoundError: raise RemoteIOError(f"Path not found: {remote_path}")
        except Exception as e: raise ConnectionLostError(f"SSH error: {e}")
        return entries

    def read_remote_file(self, remote_path):
        try:
            with self.sftp.open(remote_path, 'r') as f: return f.read().decode('utf-8').strip()
        except IOError: return None

    def write_remote_file(self, remote_path, content):
        try:
            with self.sftp.open(remote_path, 'w') as f: f.write(content)
            return True
        except IOError: return False

    def download(self, items, temp_dir, callback=None, stop_signal=None):
        limit_mb = self.speed_limit
        use_tar = (limit_mb <= 0) 
        
        if use_tar:
            paths = [f'"{full_path}"' for _, full_path in items]
            cmd = f"tar -cf - {' '.join(paths)} 2>/dev/null"
            tar_path = temp_dir / f"{int(time.time())}_{id(items)}.tar"
            stdin, stdout, stderr = self.ssh.exec_command(cmd)
            with open(tar_path, 'wb') as f:
                while True:
                    if stop_signal and stop_signal.is_set(): break
                    try: chunk = stdout.read(32768)
                    except Exception as e: raise ConnectionLostError(f"SSH stream broken: {e}")
                    if not chunk: break
                    f.write(chunk)
                    # --- 进度回调 ---
                    if callback: callback(len(chunk))

            # 如果是用户叫停，直接返回空，不要抛错
            if stop_signal and stop_signal.is_set():
                return []
            
            if stdout.channel.recv_exit_status() > 1:
                 raise RemoteIOError("SSH Tar failed")
            return [str(tar_path)]
        else:
            downloaded = []
            limiter = RateLimiter(limit_mb)
            for fname, full_path in items:
                if stop_signal and stop_signal.is_set(): break
                local_f = temp_dir / fname
                local_f.parent.mkdir(parents=True, exist_ok=True)
                try:
                    with self.sftp.open(full_path, 'rb') as rf, open(local_f, 'wb') as lf:
                        while True:
                            if stop_signal and stop_signal.is_set(): break
                            chunk = rf.read(32768)
                            if not chunk: break
                            lf.write(chunk)
                            limiter.update(len(chunk))
                            # --- 进度回调 ---
                            if callback: callback(len(chunk))
                    downloaded.append(str(local_f))
                except Exception as e:
                    if stop_signal and stop_signal.is_set(): return []
                    raise ConnectionLostError(f"SFTP error: {e}")
            return downloaded

class FtpStrategy(TransferStrategy):
    def __init__(self, config, listener):
        super().__init__(config, listener)
        self.ftp = None
        self.speed_limit = 0

    def connect(self):
        cfg = self.config['ftp']
        self.speed_limit = cfg.get('max_speed_mb', 0)
        try:
            self.ftp = ftplib.FTP()
            self.ftp.connect(cfg['host'], cfg.get('port', 2121))
            self.ftp.login(cfg.get('username',''), cfg.get('password',''))
            self.ftp.encoding = 'utf-8'
            return True
        except Exception: return False

    def disconnect(self):
        try: self.ftp.quit()
        except: pass

    def list_dir(self, remote_path):
        entries = {}
        try:
            self.ftp.cwd(remote_path)
            for name, facts in self.ftp.mlsd():
                if name in ['.', '..']: continue
                is_dir = (facts.get('type') == 'dir')
                entries[name] = (0, int(facts.get('size', 0)), is_dir)
        except Exception as e:
             if self.connect():
                 try: return self.list_dir(remote_path)
                 except Exception: raise ConnectionLostError("FTP lost")
             else: raise ConnectionLostError("FTP lost")
        return entries

    def read_remote_file(self, remote_path):
        out = io.BytesIO()
        try:
            self.ftp.retrbinary(f"RETR {remote_path}", out.write)
            return out.getvalue().decode('utf-8').strip()
        except: return None

    def write_remote_file(self, remote_path, content):
        inp = io.BytesIO(content.encode('utf-8'))
        try:
            self.ftp.storbinary(f"STOR {remote_path}", inp)
            return True
        except: return False

    def download(self, items, temp_dir, callback=None, stop_signal=None):
        paths = []
        limiter = RateLimiter(self.speed_limit)
        for fname, full_path in items:
            if stop_signal and stop_signal.is_set(): break
            local_path = temp_dir / fname
            local_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                with open(local_path, 'wb') as lf:
                    def cb(data):
                        lf.write(data)
                        limiter.update(len(data))
                        # --- 进度回调 ---
                        if callback: callback(len(data))
                    self.ftp.retrbinary(f"RETR {full_path}", cb)
                paths.append(str(local_path))
            except Exception as e:
                if stop_signal and stop_signal.is_set(): return []
                raise ConnectionLostError(f"FTP error: {e}")
        return paths

STRATEGY_MAP = {
    'adb': AdbStrategy,
    'ssh': SshStrategy,
    'ftp': FtpStrategy
}