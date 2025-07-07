from jupyter_client.manager import KernelManager
from fastapi.responses import StreamingResponse
from typing import Optional, Dict, Any, AsyncGenerator
import asyncio
import ast
import time
import queue 
import threading
import logging
import gc
import weakref
import re 
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum

# Configure logging
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

class KernelWrapper:
    def __init__(self, kernel_name: str = 'python3', max_executions: int = 50):
        """
        Improved Jupyter Kernel Manager with enhanced safety
        """
        self.max_executions = max_executions
        self.execution_lock = threading.RLock()
        self.kernel_lock = threading.RLock()  # Separate lock for kernel operations
        self._shutdown_event = threading.Event()
        self._restart_in_progress = threading.Event()
        
        # Enhanced execution tracking
        self.executions: Dict[str, ExecutionInfo] = {}
        self.output_buffers: Dict[str, SafeOutputBuffer] = {}
        
        # Kernel health monitoring
        self._last_kernel_check = time.time()
        self._kernel_check_interval = 10  # seconds
        self._kernel_healthy = False
        
        try:
            self._initialize_kernel(kernel_name)
            self._start_background_tasks()
            logger.info(f"Kernel {kernel_name} initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize kernel: {e}")
            raise RuntimeError(f"Failed to initialize kernel: {e}")
    
    def _initialize_kernel(self, kernel_name: str):
        """Initialize kernel with proper error handling"""
        with self.kernel_lock:
            self.kernel_manager = KernelManager(kernel_name=kernel_name)
            self.kernel_manager.start_kernel()
            self.client = self.kernel_manager.client()
            self.client.start_channels()
            
            # Wait for kernel to be ready
            for _ in range(10):  # 10 second timeout
                if self.client.is_alive():
                    self._kernel_healthy = True
                    return
                time.sleep(1)
            
            raise RuntimeError("Kernel failed to start within timeout")
    
    def _start_background_tasks(self):
        """Start background cleanup and monitoring tasks"""
        self._cleanup_thread = threading.Thread(
            target=self._background_cleanup,
            daemon=True,
            name="KernelCleanup"
        )
        self._cleanup_thread.start()
        
        self._health_monitor_thread = threading.Thread(
            target=self._health_monitor,
            daemon=True,
            name="KernelHealthMonitor"
        )
        self._health_monitor_thread.start()
    
    def _background_cleanup(self):
        """Enhanced background cleanup with better logic"""
        cleanup_interval = 30  # seconds
        
        while not self._shutdown_event.is_set():
            try:
                current_time = time.time()
                
                # Clean up stale executions
                self._cleanup_stale_executions(current_time)
                
                # Clean up oversized buffers
                self._cleanup_oversized_buffers()
                
                # Clean up completed executions if over limit
                self._cleanup_excess_executions()
                
                # Force garbage collection periodically
                if current_time % 120 < cleanup_interval:  # Every 2 minutes
                    gc.collect()
                
                time.sleep(cleanup_interval)
                
            except Exception as e:
                logger.warning(f"Background cleanup error: {e}")
                time.sleep(5)
    
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
    
    def _remove_execution_unsafe(self, msg_id: str):
        """Remove execution without acquiring locks (caller must hold locks)"""
        exec_info = self.executions.pop(msg_id, None)
        buffer = self.output_buffers.pop(msg_id, None)
        
        if buffer:
            buffer.clear()
        
        if exec_info:
            logger.debug(f"Removed execution {msg_id} in state {exec_info.state}")
    
    def _health_monitor(self):
        """Monitor kernel health and trigger restart if needed"""
        while not self._shutdown_event.is_set():
            try:
                current_time = time.time()
                
                # Check kernel health periodically
                if current_time - self._last_kernel_check > self._kernel_check_interval:
                    self._check_kernel_health()
                    self._last_kernel_check = current_time
                
                time.sleep(5)  # Check every 5 seconds
                
            except Exception as e:
                logger.warning(f"Health monitor error: {e}")
                time.sleep(10)
    
    def _check_kernel_health(self):
        """Check if kernel is healthy and restart if needed"""
        try:
            # Use kernel lock to prevent race conditions
            with self.kernel_lock:
                if self._restart_in_progress.is_set():
                    return  # Restart already in progress
                
                # Check if kernel is alive
                is_alive = self.client.is_alive()
                
                if not is_alive and self._kernel_healthy:
                    logger.warning("Kernel appears to be dead, attempting restart")
                    self._trigger_kernel_restart()
                
                self._kernel_healthy = is_alive
                
        except Exception as e:
            logger.error(f"Kernel health check failed: {e}")
            self._kernel_healthy = False
    
    def _trigger_kernel_restart(self):
        """Trigger kernel restart in a separate thread to avoid blocking"""
        if self._restart_in_progress.is_set():
            logger.info("Kernel restart already in progress")
            return
        
        restart_thread = threading.Thread(
            target=self._restart_kernel_async,
            daemon=True,
            name="KernelRestart"
        )
        restart_thread.start()
    
    def _restart_kernel_async(self):
        """Async kernel restart with proper synchronization"""
        try:
            self._restart_in_progress.set()
            logger.info("Starting kernel restart...")
            
            # Clean up all executions first
            self.force_cleanup()
            
            with self.kernel_lock:
                try:
                    # Restart the kernel
                    self.kernel_manager.restart_kernel()
                    
                    # Wait for kernel to be ready
                    for _ in range(10):
                        if self.client.is_alive():
                            self._kernel_healthy = True
                            logger.info("Kernel restart successful")
                            return
                        time.sleep(1)
                    
                    logger.error("Kernel restart failed - not responding")
                    self._kernel_healthy = False
                    
                except Exception as e:
                    logger.error(f"Kernel restart failed: {e}")
                    self._kernel_healthy = False
                    
        finally:
            self._restart_in_progress.clear()
    
    def is_kernel_alive(self) -> bool:
        """Thread-safe kernel alive check with caching"""
        with self.kernel_lock:
            current_time = time.time()
            if current_time - self._last_kernel_check < 2:  # 2 second cache
                return self._kernel_healthy

            if self._restart_in_progress.is_set():
                return False
            
            try:
                alive = self.client.is_alive()
                self._kernel_healthy = alive
                self._last_kernel_check = current_time
                return alive
            except Exception as e:
                logger.warning(f"Kernel alive check failed: {e}")
                self._kernel_healthy = False
                return False

    
    def execute_code(self, code: str, timeout: int = 30):
        """Enhanced execute_code with better safety checks"""
        # Input validation
        if not code or not isinstance(code, str):
            raise ValueError("Invalid code input")

        # Check kernel health first
        if not self.is_kernel_alive():
            logger.error("Kernel is not alive")
            raise RuntimeError("Kernel is not running. Please restart the kernel.")
        
        # Skip completeness check for empty/whitespace code
        if not code.strip():
            raise ValueError("Empty code cannot be executed")

        # Thread-safe execution capacity check
        with self.execution_lock:
            active_count = sum(1 for exec_info in self.executions.values() 
                        if exec_info.state == ExecutionState.BUSY)
            
            if active_count >= 3:
                logger.warning(f"Too many concurrent executions: {active_count}")
                raise RuntimeError("Too many concurrent executions. Please wait.")
        
        # Modified completeness check - skip for simple statements
        if not self.is_long_running(code) and not self.is_simple_statement(code):
            try:
                with self.kernel_lock:
                    # Add timeout for is_complete check
                    self.client.shell_channel.send_control('is_complete_request', {
                        'code': code
                    })
                    reply = self.client.shell_channel.get_msg(timeout=2.0)
                
                if not reply or reply.get('content', {}).get('status') != 'complete':
                    indent = reply.get('content', {}).get('indent', '')
                    raise RuntimeError(f"Incomplete code: {indent or 'Missing closing brackets/parentheses'}")
            
            except queue.Empty:
                logger.warning("Timeout checking code completeness - proceeding with execution")
            except Exception as e:
                logger.warning(f"Code completeness check failed: {e}")
                # Instead of failing, we'll proceed with execution and let the kernel handle it
                pass
        
        try:
            # Execute the code
            with self.kernel_lock:
                msg_id = self.client.execute(code)
            logger.info(f"Code execution started with msg_id: {msg_id}")
            
            # Track execution with enhanced info
            with self.execution_lock:
                exec_info = ExecutionInfo(
                    msg_id=msg_id,
                    state=ExecutionState.BUSY,
                    start_time=time.time(),
                    last_activity=time.time()
                )
                self.executions[msg_id] = exec_info
                self.output_buffers[msg_id] = SafeOutputBuffer()
            
            # Return streaming response
            return StreamingResponse(
                self._stream_output(msg_id, timeout),
                media_type='text/plain',
                headers={
                    'X-Execution-ID': msg_id,
                    'Cache-Control': 'no-cache',
                    'Connection': 'keep-alive'
                }
            )
            
        except Exception as e:
            logger.error(f"Failed to execute code: {e}", exc_info=True)
            raise RuntimeError(f"Failed to execute code: {e}")

    def is_simple_statement(self, code: str) -> bool:
        """Check if code is a simple statement that doesn't need completeness check"""
        simple_patterns = [
            r'^\s*print\(.*\)\s*$',
            r'^\s*[\w]+\s*=\s*.+\s*$',
            r'^\s*[\w]+\s*$',
            r'^\s*import\s+.+\s*$',
            r'^\s*from\s+.+\s*import\s+.+\s*$'
        ]
        return any(re.match(pattern, code) for pattern in simple_patterns)

    async def _stream_output(self, msg_id: str, timeout: int) -> AsyncGenerator[str, None]:
        start_time = time.time()
        message_count = 0
        logger.info(f"Starting stream for execution {msg_id}")

        try:
            while not self._shutdown_event.is_set():
                if (time.time() - start_time) > timeout:
                    logger.warning(f"Execution timeout for {msg_id}")
                    yield f"\n[Execution timeout after {timeout} seconds]\n"
                    self._update_execution_state(msg_id, ExecutionState.TIMEOUT)
                    break

                # Check kernel health
                if not self.is_kernel_alive():
                    logger.error(f"Kernel died during execution {msg_id}")
                    yield f"\n[Error: Kernel connection lost]\n"
                    break

                try:
                    msg = await asyncio.to_thread(self.client.get_iopub_msg, timeout=1.0)
                    logger.debug(f"IOPub message: {msg}")
                    message_count += 1

                    # Update activity timestamp
                    with self.execution_lock:
                        if msg_id in self.executions:
                            self.executions[msg_id].update_activity()

                except queue.Empty:
                    await asyncio.sleep(0.1)
                    continue
                except Exception as e:
                    logger.warning(f"Message retrieval error: {repr(e)}")
                    await asyncio.sleep(0.1)
                    continue

                if msg["header"]["msg_type"] == "status":
                    state = msg["content"].get("execution_state")
                    if state == "idle":
                        logger.info(f"Execution finished for {msg_id}")
                        break

                # Process output
                output_text = self._process_message(msg, msg_id)
                if output_text:
                    with self.execution_lock:
                        buffer = self.output_buffers.get(msg_id)
                        if buffer:
                            if buffer.append(output_text):
                                yield output_text
                            else:
                                yield f"\n[Output buffer full. Stopping stream.]\n"
                                break
                        else:
                            break
                    await asyncio.sleep(0.01)

        except asyncio.CancelledError:
            logger.info(f"Stream cancelled for {msg_id}")
        except Exception as e:
            logger.error(f"Streaming error: {e}")
            yield f"\n[Streaming error: {e}]\n"
        finally:
            logger.info(f"Stream ended for {msg_id}. Processed {message_count} messages")
            self._finalize_execution(msg_id)

    
    def _process_message(self, msg: dict, msg_id: str) -> str:
        """Process individual message and return output text"""
        if not isinstance(msg, dict) or 'header' not in msg:
            return ""
        
        # Check if message belongs to our execution
        parent_header = msg.get('parent_header', {})
        if parent_header.get('msg_id') != msg_id:
            return ""
        
        msg_type = msg['header']['msg_type']
        content = msg.get('content', {})
        
        if msg_type == 'status':
            execution_state = content.get('execution_state', '')
            if execution_state == 'idle':
                self._update_execution_state(msg_id, ExecutionState.IDLE)
            elif execution_state == 'error':
                self._update_execution_state(msg_id, ExecutionState.ERROR)
            return ""
        
        elif msg_type == 'stream':
            return content.get('text', '')
        
        elif msg_type in ('execute_result', 'display_data'):
            data = content.get('data', {})
            return data.get('text/plain', '')
        
        elif msg_type == 'error':
            error_name = content.get('ename', 'Error')
            error_value = content.get('evalue', '')
            traceback = content.get('traceback', [])
            
            self._update_execution_state(msg_id, ExecutionState.ERROR)
            
            error_text = f"\n{error_name}: {error_value}\n"
            if traceback:
                error_text += '\n'.join(traceback)
            return error_text
        
        return ""
    
    def _update_execution_state(self, msg_id: str, state: ExecutionState):
        """Safely update execution state"""
        with self.execution_lock:
            if msg_id in self.executions:
                self.executions[msg_id].state = state
                self.executions[msg_id].update_activity()
    
    def _finalize_execution(self, msg_id: str):
        with self.execution_lock:
            if msg_id in self.executions:
                exec_info = self.executions[msg_id]
                if exec_info.state == ExecutionState.BUSY:
                    exec_info.state = ExecutionState.COMPLETED
                exec_info.update_activity()
                logger.info(f"Execution {msg_id} completed in {time.time() - exec_info.start_time:.2f}s")
    
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
    
    def get_stats(self) -> Dict[str, Any]:
        """Enhanced statistics"""
        with self.execution_lock:
            state_counts = defaultdict(int)
            total_buffer_size = 0
            
            for exec_info in self.executions.values():
                state_counts[exec_info.state.value] += 1
            
            for buffer in self.output_buffers.values():
                total_buffer_size += buffer.size()
            
            return {
                'total_executions': len(self.executions),
                'state_counts': dict(state_counts),
                'total_buffer_size': total_buffer_size,
                'kernel_alive': self.is_kernel_alive(),
                'kernel_healthy': self._kernel_healthy,
                'restart_in_progress': self._restart_in_progress.is_set(),
                'last_kernel_check': self._last_kernel_check
            }
    
    def restart_kernel(self):
        """Public method to trigger kernel restart"""
        if self._restart_in_progress.is_set():
            return {"status": "restart_in_progress"}
        
        self._trigger_kernel_restart()
        return {"status": "restart_triggered"}
    
    def shutdown_kernel(self):
        """Enhanced shutdown with proper cleanup"""
        logger.info("Shutting down kernel...")
        
        # Signal all background threads to stop
        self._shutdown_event.set()
        
        # Wait for threads to finish
        for thread in [self._cleanup_thread, self._health_monitor_thread]:
            if thread.is_alive():
                thread.join(timeout=5)
        
        # Clean up all tracking data
        self.force_cleanup()
        
        try:
            with self.kernel_lock:
                if hasattr(self.client, 'stop_channels'):
                    self.client.stop_channels()
                self.kernel_manager.shutdown_kernel()
            
            logger.info("Kernel shutdown completed")
            
        except Exception as e:
            logger.error(f"Error during kernel shutdown: {e}")
    
    # Keep existing methods for compatibility
    def is_long_running(self, code: str) -> bool:
        """Keep existing implementation"""
        try:
            tree = ast.parse(code)
            
            for node in ast.walk(tree):
                if isinstance(node, (ast.While, ast.For)):
                    return True
                    
                if isinstance(node, ast.Call):
                    if isinstance(node.func, ast.Name):
                        if node.func.id in ['sleep', 'wait', 'run', 'execute']:
                            return True
                    elif isinstance(node.func, ast.Attribute):
                        if node.func.attr in ['sleep', 'wait', 'run', 'execute']:
                            return True
                
                if isinstance(node, (ast.ListComp, ast.SetComp, ast.DictComp)):
                    return True
                    
        except Exception as e:
            logger.warning(f"Error analyzing code: {e}")
            return False
        
        long_running_keywords = [
            'time.sleep', 'asyncio.sleep', 'threading.', 'multiprocessing.',
            'requests.', 'urllib.', 'subprocess.', 'os.system'
        ]
        
        return any(keyword in code for keyword in long_running_keywords)
    
    def is_kernel_running(self):
        '''
        Checks if the Jupyter kernel is running.
        Returns:
            bool: True if the kernel is running, False otherwise.
        '''
        return self.client.is_alive()