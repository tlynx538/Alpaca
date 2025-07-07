# kernel_manage.py
import asyncio
import ast
import re
import queue
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, AsyncGenerator, Union, List, Optional
from jupyter_client import KernelManager as JupyterKernelManager
from fastapi.responses import StreamingResponse
from dataclasses import dataclass
from enum import Enum
from contextlib import asynccontextmanager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ExecutionState(Enum):
    BUSY = "busy"
    IDLE = "idle"
    ERROR = "error"
    TIMEOUT = "timeout"
    COMPLETED = "completed"

@dataclass
class ExecutionInfo:
    msg_id: str
    state: ExecutionState
    start_time: float
    last_activity: float

class ExecutionTracker:
    def __init__(self):
        self._executions: Dict[str, ExecutionInfo] = {}
        self._lock = threading.RLock()

    def add_execution(self, msg_id: str):
        with self._lock:
            self._executions[msg_id] = ExecutionInfo(
                msg_id=msg_id,
                state=ExecutionState.BUSY,
                start_time=time.time(),
                last_activity=time.time()
            )

    def get_active_count(self) -> int:
        with self._lock:
            return sum(1 for info in self._executions.values() 
                      if info.state == ExecutionState.BUSY)

    def update_execution_state(self, msg_id: str, state: ExecutionState):
        with self._lock:
            if msg_id in self._executions:
                self._executions[msg_id].state = state
                self._executions[msg_id].last_activity = time.time()

    def finalize_execution(self, msg_id: str):
        with self._lock:
            if msg_id in self._executions:
                exec_info = self._executions[msg_id]
                if exec_info.state == ExecutionState.BUSY:
                    exec_info.state = ExecutionState.COMPLETED
                exec_info.last_activity = time.time()
                logger.info(f"Execution {msg_id} completed in {time.time() - exec_info.start_time:.2f}s")

class OutputBufferManager:
    def __init__(self, max_size: int = 1024 * 1024):
        self._buffers: Dict[str, SafeOutputBuffer] = {}
        self._lock = threading.RLock()
        self.max_size = max_size

    async def process_stream_message(self, msg: dict, msg_id: str) -> AsyncGenerator[str, None]:
        output_text = self._process_message(msg, msg_id)
        if output_text:
            with self._lock:
                buffer = self._buffers.get(msg_id)
                if not buffer:
                    buffer = SafeOutputBuffer(self.max_size)
                    self._buffers[msg_id] = buffer
                
                if buffer.append(output_text):
                    yield output_text
                else:
                    yield "\n[Output buffer full. Stopping stream.]\n"

    def _process_message(self, msg: dict, msg_id: str) -> str:
        if not isinstance(msg, dict) or 'header' not in msg:
            return ""
        
        parent_header = msg.get('parent_header', {})
        if parent_header.get('msg_id') != msg_id:
            return ""
        
        msg_type = msg['header']['msg_type']
        content = msg.get('content', {})
        
        if msg_type == 'stream':
            return content.get('text', '')
        elif msg_type in ('execute_result', 'display_data'):
            return content.get('data', {}).get('text/plain', '')
        elif msg_type == 'error':
            error_text = f"\n{content.get('ename', 'Error')}: {content.get('evalue', '')}\n"
            if traceback := content.get('traceback', []):
                error_text += '\n'.join(traceback)
            return error_text
        return ""

class SafeOutputBuffer:
    def __init__(self, max_size: int):
        self.max_size = max_size
        self.buffer = []
        self.current_size = 0
        self._lock = threading.RLock()
        self.truncated = False
    
    def append(self, data: str) -> bool:
        with self._lock:
            data_size = len(data)
            if self.current_size + data_size > self.max_size:
                if not self.truncated:
                    self.buffer.append("\n[Output truncated due to size limit]\n")
                    self.truncated = True
                return False
            
            self.buffer.append(data)
            self.current_size += data_size
            return True

class Kernel:
    def __init__(self, kernel_name: str = 'python3'):
        self._km = JupyterKernelManager(kernel_name=kernel_name)
        self._client = None
        self._lock = threading.RLock()
        self._healthy = False
        self._cleanup_lock = threading.Lock()
        self.start_kernel()


    @property
    def client(self):
        with self._lock:
            return self._client

    def start_kernel(self):
        with self._lock:
            try:
                self._km.start_kernel()
                self._client = self._km.client()
                self._start_channels_with_timeout(10)
                self._healthy = True
            except Exception as e:
                logger.error(f"Failed to start kernel: {e}")
                self._healthy = False
                raise

    def _start_channels_with_timeout(self, timeout: int):
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                self._client.start_channels()
                if all(channel.is_alive() for channel in [
                    self._client.shell_channel,
                    self._client.iopub_channel
                ]):
                    return
                time.sleep(0.5)
            except Exception as e:
                logger.warning(f"Channel start attempt failed: {e}")
                time.sleep(0.5)
        raise RuntimeError("Failed to start channels within timeout")

    def is_kernel_alive(self) -> bool:
        with self._lock:
            try:
                return self._healthy and self._km.is_alive() and all(
                    channel.is_alive() for channel in [
                        self._client.shell_channel,
                        self._client.iopub_channel
                    ] if channel is not None
                )
            except Exception:
                return False

    def ensure_channels_active(self):
        with self._lock:
            if not self.is_kernel_alive():
                self._start_channels_with_timeout(5)

    def interrupt_kernel(self):
        """Safely interrupt kernel execution"""
        with self._lock:
            try:
                if hasattr(self._client, 'interrupt'):
                    self._client.interrupt()
                else:
                    # Fallback method for kernels without direct interrupt
                    if hasattr(self._km, 'interrupt_kernel'):
                        self._km.interrupt_kernel()
                    else:
                        self.restart_kernel()  # Full restart if no interrupt available
            except Exception as e:
                logger.error(f"Interrupt failed: {e}")
                raise RuntimeError("Failed to interrupt kernel")

    def restart_kernel(self) -> Dict[str, Any]:
        """Synchronously restart the kernel with proper cleanup"""
        if self._restart_in_progress.is_set():
            return {
                "status": "error",
                "message": "Restart already in progress",
                "timestamp": time.time()
            }

        self._restart_in_progress.set()
        try:
            logger.info("Starting kernel restart procedure")

            # 1. Clean shutdown if possible
            try:
                if hasattr(self._km, 'shutdown_kernel'):
                    self._km.shutdown_kernel(now=True)
                elif hasattr(self._km, '_kill_kernel'):
                    self._km._kill_kernel()
            except Exception as e:
                logger.warning(f"Graceful shutdown failed: {e}")

            # 2. Force kill if still running
            if self._km.is_alive():
                try:
                    if hasattr(self._km, 'kill_kernel'):
                        self._km.kill_kernel()
                    elif hasattr(self._km, '_kill_kernel'):
                        self._km._kill_kernel()
                except Exception as e:
                    logger.error(f"Failed to kill kernel: {e}")
                    raise RuntimeError("Failed to terminate kernel")

            # 3. Clean up client resources
            if self._client and hasattr(self._client, 'stop_channels'):
                try:
                    self._client.stop_channels()
                except Exception as e:
                    logger.warning(f"Error stopping channels: {e}")

            # 4. Start fresh kernel (synchronously)
            try:
                self._km.start_kernel()
                # Create new client with fresh channels
                self._client = self._km.client()
                self._client.start_channels()
                self._healthy = True
                logger.info("Kernel started successfully")
            except Exception as e:
                logger.error(f"Kernel startup failed: {e}")
                self._healthy = False
                raise RuntimeError("Failed to start kernel")

            return {
                "status": "success",
                "message": "Kernel restarted successfully",
                "timestamp": time.time()
            }

        except Exception as e:
            logger.error(f"Kernel restart failed: {e}")
            self._healthy = False
            return {
                "status": "error",
                "message": f"Restart failed: {str(e)}",
                "timestamp": time.time()
            }
        finally:
            self._restart_in_progress.clear()


    def _trigger_kernel_restart(self):
        """Trigger kernel restart in a separate thread to avoid blocking."""
        if self._restart_in_progress.is_set():
            logger.info("Kernel restart already in progress")
            return
        
        restart_thread = threading.Thread(
            target=self._restart_kernel_async,
            daemon=True,
            name="KernelRestart"
        )
        restart_thread.start()

    def shutdown_kernel(self):
        with self._lock:
            try:
                if hasattr(self._client, 'stop_channels'):
                    self._client.stop_channels()
                self._km.shutdown_kernel()
                self._healthy = False
            except Exception as e:
                logger.error(f"Failed to shutdown kernel: {e}")
                raise
    
    def _restart_kernel_async(self):
        """Async kernel restart with proper synchronization."""
        try:
            self._restart_in_progress.set()
            logger.info("Starting kernel restart...")
            with self.kernel_lock:
                try:
                    self._shutdown_kernel_internal()
                    self._km.restart_kernel()
                    self.client = self._km.client()
                    self._start_channels_with_timeout(10)
                    for _ in range(10):
                        if self.client.is_alive():
                            try:
                                self.client.kernel_info()
                                self._kernel_healthy = True
                                logger.info("Kernel restart successful")
                                return
                            except Exception:
                                pass
                        time.sleep(1)
                    logger.error("Kernel restart failed - not responding")
                    self._kernel_healthy = False
                except Exception as e:
                    logger.error(f"Kernel restart failed: {e}")
                    self._kernel_healthy = False
        finally:
            self._restart_in_progress.clear()
    
    def _shutdown_kernel_internal(self):
        """Internal method to shutdown the kernel without recursive calls."""
        logger.info("Shutting down kernel internally...")
        try:
            with self.kernel_lock:
                if hasattr(self.client, 'stop_channels'):
                    self.client.stop_channels()
                if self._km:
                    self.shutdown_kernel()
            logger.info("Kernel shutdown completed internally")
        except Exception as e:
            logger.error(f"Error during internal kernel shutdown: {e}")

    def cleanup(self):
        """Thread-safe kernel cleanup"""
        with self._cleanup_lock:
            try:
                self.flush_channels()
                if hasattr(self._client, 'stop_channels'):
                    self._client.stop_channels()
            except Exception as e:
                logger.warning(f"Cleanup warning: {str(e)}")

    def flush_channels(self):
        """Safely flush all kernel channels"""
        with self._cleanup_lock:
            if not self._client:
                return
            
            try:
                for channel_name in ['iopub_channel', 'shell_channel', 'stdin_channel', 'hb_channel']:
                    channel = getattr(self._client, channel_name, None)
                    if channel and hasattr(channel, 'get_msg'):
                        try:
                            while True:
                                channel.get_msg(timeout=0.1)
                        except (queue.Empty, AttributeError):
                            pass
            except Exception as e:
                logger.warning(f"Channel flush warning: {str(e)}")



