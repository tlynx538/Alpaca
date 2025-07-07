# execution_tracker.py
import logging
import threading
import time
import gc
from typing import Dict
from enum import Enum
from dataclasses import dataclass
from collections import defaultdict

# Setup logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ExecutionState(Enum):
    BUSY = "busy"
    IDLE = "idle"
    ERROR = "error"
    TIMEOUT = "timeout"
    COMPLETED = "completed"
    FAILED = "failed"

@dataclass
class ExecutionInfo:
    """Thread-safe execution tracking"""
    msg_id: str
    state: ExecutionState
    start_time: float
    last_activity: float
    output_size: int = 0
    max_output_size: int = 1024 * 1024  # 1MB default
    
    def update_activity(self):
        self.last_activity = time.time()
    
    def is_stale(self, max_age: int = 300) -> bool:
        """Check if execution is older than max_age seconds"""
        return time.time() - self.last_activity > max_age
    
    def is_oversized(self) -> bool:
        """Check if output buffer is too large"""
        return self.output_size > self.max_output_size

class SafeOutputBuffer:
    """Thread-safe output buffer with automatic cleanup"""
    def __init__(self, max_size: int = 1024 * 1024):
        self.max_size = max_size
        self.buffer = []
        self.current_size = 0
        self.lock = threading.RLock()
        self.truncated = False
    
    def append(self, data: str) -> bool:
        """Append data, return False if buffer is full"""
        with self.lock:
            data_size = len(data)
            if self.current_size + data_size > self.max_size:
                if not self.truncated:
                    self.buffer.append("\n[Warning: Output truncated due to size limit]\n")
                    self.truncated = True
                return False
            self.buffer.append(data)
            self.current_size += data_size
            return True
    
    def get_content(self) -> str:
        """Get buffer content safely"""
        with self.lock:
            return ''.join(self.buffer)
    
    def clear(self):
        """Clear buffer and reset state"""
        with self.lock:
            self.buffer.clear()
            self.current_size = 0
            self.truncated = False
    
    def size(self) -> int:
        """Get current buffer size"""
        with self.lock:
            return self.current_size

class ExecutionTracker:
    """Manages execution state tracking and cleanup for kernel executions"""
    def __init__(self, max_executions: int = 50):
        """Initialize ExecutionTracker with a limit on maximum executions.
        
        Args:
            max_executions (int): Maximum number of executions to track.
        """
        self.execution_lock = threading.RLock()
        self.executions: Dict[str, ExecutionInfo] = {}
        self.output_buffers: Dict[str, SafeOutputBuffer] = {}
        self.max_executions = max_executions

    def _remove_execution_unsafe(self, msg_id: str):
        """Remove execution without acquiring locks (caller must hold locks)"""
        exec_info = self.executions.pop(msg_id, None)
        buffer = self.output_buffers.pop(msg_id, None)
        if buffer:
            buffer.clear()
        if exec_info:
            logger.debug(f"Removed execution {msg_id} in state {exec_info.state}")

    def _cleanup_stale_executions(self, current_time: float):
        """Clean up executions that are stale or stuck"""
        with self.execution_lock:
            stale_executions = []
            for msg_id, exec_info in self.executions.items():
                # Remove truly stale executions (no activity for 5+ minutes)
                if exec_info.is_stale(300):
                    stale_executions.append(msg_id)
                    continue
                # Handle stuck executions (busy for too long)
                if (exec_info.state == ExecutionState.BUSY and 
                    current_time - exec_info.start_time > 600):  # 10 minutes
                    logger.warning(f"Execution {msg_id} appears stuck, marking as failed")
                    exec_info.state = ExecutionState.FAILED
                    exec_info.update_activity()
            # Remove stale executions
            for msg_id in stale_executions:
                self._remove_execution_unsafe(msg_id)
                logger.info(f"Cleaned up stale execution: {msg_id}")

    def _cleanup_oversized_buffers(self):
        """Clean up buffers that are too large"""
        with self.execution_lock:
            oversized = []
            for msg_id, buffer in self.output_buffers.items():
                if buffer.size() > buffer.max_size:
                    oversized.append(msg_id)
            for msg_id in oversized:
                logger.warning(f"Cleaning up oversized buffer for {msg_id}")
                self._remove_execution_unsafe(msg_id)

    def _cleanup_excess_executions(self):
        """Clean up excess completed executions"""
        with self.execution_lock:
            if len(self.executions) <= self.max_executions:
                return
            # Get completed executions sorted by last activity
            completed = [
                (msg_id, exec_info) 
                for msg_id, exec_info in self.executions.items()
                if exec_info.state in [ExecutionState.COMPLETED, ExecutionState.IDLE, 
                                     ExecutionState.ERROR, ExecutionState.FAILED]
            ]
            # Sort by last activity (oldest first)
            completed.sort(key=lambda x: x[1].last_activity)
            # Remove oldest executions beyond limit
            excess_count = len(self.executions) - self.max_executions
            for i in range(min(excess_count, len(completed))):
                msg_id = completed[i][0]
                self._remove_execution_unsafe(msg_id)
                logger.info(f"Cleaned up excess execution: {msg_id}")

    def _update_execution_state(self, msg_id: str, state: ExecutionState):
        """Safely update execution state"""
        with self.execution_lock:
            if msg_id in self.executions:
                self.executions[msg_id].state = state
                self.executions[msg_id].update_activity()

    def force_cleanup(self):
        """Enhanced force cleanup"""
        with self.execution_lock:
            count = len(self.executions)
            # Clear all buffers first
            for buffer in self.output_buffers.values():
                buffer.clear()
            # Clear all tracking data
            self.executions.clear()
            self.output_buffers.clear()
            logger.info(f"Force cleaned {count} executions")
            # Force garbage collection
            gc.collect()

    def _finalize_execution(self, msg_id: str):
        """Finalize an execution by updating its state to COMPLETED if BUSY"""
        with self.execution_lock:
            if msg_id in self.executions:
                exec_info = self.executions[msg_id]
                if exec_info.state == ExecutionState.BUSY:
                    exec_info.state = ExecutionState.COMPLETED
                exec_info.update_activity()
                logger.info(f"Execution {msg_id} completed in {time.time() - exec_info.start_time:.2f}s")

    def add_execution(self, msg_id: str, state: ExecutionState = ExecutionState.BUSY):
        """Add a new execution to track"""
        with self.execution_lock:
            self.executions[msg_id] = ExecutionInfo(
                msg_id=msg_id,
                state=state,
                start_time=time.time(),
                last_activity=time.time()
            )
            self.output_buffers[msg_id] = SafeOutputBuffer()

    def get_active_count(self) -> int:
        """Return the number of active (BUSY) executions"""
        with self.execution_lock:
            return sum(1 for exec_info in self.executions.values() if exec_info.state == ExecutionState.BUSY)
