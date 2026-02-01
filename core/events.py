# core/events.py
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from .models import ProgressData, ScanResult, BackupStats

class SyncEventListener(ABC):
    """
    核心事件监听接口 (Protocol)。
    Core 通过此接口通知外部 '发生了什么'，而不关心外部 '如何显示'。
    """

    # --- 1. 状态与生命周期 ---

    @abstractmethod
    def on_phase_changed(self, phase: str, details: Optional[Dict[str, Any]] = None):
        """
        阶段变更通知
        :param phase: 'idle', 'connecting', 'scanning', 'backup_start', 'finished'
        :param details: 附加元数据
        """
        pass

    @abstractmethod
    def on_device_verified(self, device_id: str, mode: str):
        """
        设备身份验证通过
        """
        pass

    # --- 2. 进度与数据 ---

    @abstractmethod
    def on_scan_finished(self, result: ScanResult):
        """
        目录扫描完成，汇报该目录的详细统计数据
        """
        pass

    @abstractmethod
    def on_scan_finished(self, total_files: int, total_size: int, local_files_count: int):
        """
        扫描完成，汇报统计数据
        """
        pass

    @abstractmethod
    def on_progress(self, data: ProgressData):
        """
        高频调用：传输进度更新
        """
        pass

    # --- 3. 结果与错误 ---

    @abstractmethod
    def on_task_finished(self, stats: BackupStats, success: bool):
        """
        任务整体结束
        """
        pass

    @abstractmethod
    def on_error(self, title: str, message: str, critical: bool = False):
        """
        错误通知
        :param critical: True 表示阻断性错误(通常需弹窗)，False 表示警告
        """
        pass

    # --- 4. 调试/日志 ---

    @abstractmethod
    def on_log(self, message: str, level: str = "info"):
        """
        流水账日志 (供 UI 的日志面板使用)
        """
        pass