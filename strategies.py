import os
import time
import subprocess
import shlex
import logging
from abc import ABC, abstractmethod
from utils import RateLimiter

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

STRATEGY_MAP = {
    'adb': AdbStrategy,
    'ssh': SshStrategy
}