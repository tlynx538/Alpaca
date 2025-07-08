# code_executor.py
import logging
import ast
import re
import queue
import asyncio
import time
import threading
from typing import Dict, Any, AsyncGenerator, Union, List, Optional
from fastapi.responses import StreamingResponse
from concurrent.futures import ThreadPoolExecutor
from kernel.kernel_manage import Kernel
from kernel.execution_tracker import ExecutionTracker, ExecutionState
from kernel.output_buffer import OutputBufferManager

# Setup logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CodeExecutor:
    def __init__(self, kernel_manager: Kernel, 
                 execution_tracker: ExecutionTracker, 
                 output_manager: OutputBufferManager,
                 max_concurrent_executions: int = 3):
        """
        Initialize CodeExecutor with proper async/thread handling.
        
        Args:
            kernel_manager: Manages kernel lifecycle and operations
            execution_tracker: Tracks execution states and metadata
            output_manager: Handles output processing and buffering
            max_concurrent_executions: Maximum concurrent executions allowed
        """
        self.kernel_manager = kernel_manager
        self.execution_tracker = execution_tracker
        self.output_manager = output_manager
        self.max_concurrent_executions = max_concurrent_executions
        self._shutdown_event = threading.Event()
        self._executor = ThreadPoolExecutor(max_workers=4)
        
    async def execute_code(self, code: str, timeout: int = 30) -> StreamingResponse:
        """Execute code and return streaming response"""
        # Input validation and kernel checks remain the same...
        
        # Execute code and get message ID
        msg_id = await self._execute_in_thread(code)
        self.execution_tracker.add_execution(msg_id)
        
        # Create and return StreamingResponse directly
        return StreamingResponse(
            self._generate_output(msg_id, timeout),
            media_type='text/plain',
            headers={'X-Execution-ID': msg_id}
        )

    async def _execute_in_thread(self, code: str) -> str:
        """Execute code in thread pool"""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            lambda: self._execute_code_sync(code)
        )

    def _execute_code_sync(self, code: str) -> str:
        """Synchronous code execution"""
        self.kernel_manager.ensure_channels_active()
        return self.kernel_manager.client.execute(code)

    async def _check_code_completeness(self, code: str):
        """Check if code is syntactically complete"""
        try:
            loop = asyncio.get_running_loop()
            reply = await loop.run_in_executor(
                self._executor,
                lambda: self._check_completeness_sync(code)
            )
            
            if not reply or reply.get('content', {}).get('status') != 'complete':
                indent = reply.get('content', {}).get('indent', '')
                raise RuntimeError(f"Incomplete code: {indent or 'Missing closing brackets'}")
        except queue.Empty:
            logger.warning("Timeout checking code completeness")
        except Exception as e:
            logger.warning(f"Code check failed: {e}")

    def _check_completeness_sync(self, code: str) -> dict:
        """Synchronous completeness check"""
        self.kernel_manager.client.shell_channel.send_control('is_complete_request', {'code': code})
        return self.kernel_manager.client.shell_channel.get_msg(timeout=2.0)

    async def _stream_output(self, msg_id: str, timeout: int) -> AsyncGenerator[str, None]:
        """Async generator for streaming output"""
        start_time = time.time()
        
        try:
            while not self._shutdown_event.is_set():
                if (time.time() - start_time) > timeout:
                    yield f"\n[Execution timed out after {timeout} seconds]\n"
                    self.execution_tracker._update_execution_state(msg_id, ExecutionState.TIMEOUT)
                    break

                msg = await self._get_iopub_message()
                if msg is None:
                    await asyncio.sleep(0.05)
                    continue

                # Process and yield output
                processed = self.output_manager.process_message(msg, msg_id)
                if processed:
                    yield processed
                    
        except Exception as e:
            logger.error(f"Streaming error: {e}")
            yield f"\n[Streaming error: {str(e)}]\n"
        finally:
            self.execution_tracker._finalize_execution(msg_id)

    async def _get_iopub_message(self) -> Optional[dict]:
        """Get message with minimal delay"""
        try:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(
                self._executor,
                lambda: self.kernel_manager.client.get_iopub_msg(timeout=0.1)  # Reduced timeout
            )
        except (queue.Empty, asyncio.TimeoutError):
            return None
        except Exception as e:
            logger.debug(f"Message error: {e}")
            return None

    async def _handle_timeout(self, msg_id: str, timeout: int):
        """Handle execution timeout properly"""
        logger.warning(f"Timeout for {msg_id} after {timeout}s")
        try:
            # Use kernel manager's interrupt method
            await asyncio.get_running_loop().run_in_executor(
                self._executor,
                self.kernel_manager.interrupt_kernel()
            )
        except Exception as e:
            logger.error(f"Interrupt failed: {e}")
        yield f"\n[Execution timed out after {timeout} seconds]\n"
        self.execution_tracker._update_execution_state(msg_id, ExecutionState.TIMEOUT)

    def _is_simple_statement(self, code: str) -> bool:
        """Check if code is simple"""
        patterns = [
            r'^\s*print\(.*\)\s*$',
            r'^\s*[\w]+\s*=\s*.+\s*$',
            r'^\s*import\s+.+\s*$',
            r'^\s*from\s+.+\s*import\s+.+\s*$'
        ]
        return any(re.match(p, code) for p in patterns)

    def _is_long_running(self, code: str) -> bool:
        """Check for long-running patterns"""
        try:
            tree = ast.parse(code)
            for node in ast.walk(tree):
                if isinstance(node, (ast.While, ast.For, ast.ListComp, ast.SetComp, ast.DictComp)):
                    return True
                if isinstance(node, ast.Call) and (
                    (isinstance(node.func, ast.Name) and node.func.id in ['sleep', 'wait']) or
                    (isinstance(node.func, ast.Attribute) and node.func.attr in ['sleep', 'wait'])
                ):
                    return True
        except Exception:
            return False
        
        keywords = ['time.sleep', 'asyncio.sleep', 'threading.', 'subprocess.']
        return any(k in code for k in keywords)

    async def _generate_output(self, msg_id: str, timeout: int) -> AsyncGenerator[str, None]:
        """Generate output chunks for streaming"""
        start_time = time.time()
        received_idle = False
        
        try:
            while not self._shutdown_event.is_set() and not received_idle:
                # Safety timeout check
                if time.time() - start_time > timeout:
                    yield f"\n[Execution timed out after {timeout} seconds]\n"
                    self.execution_tracker._update_execution_state(msg_id, ExecutionState.TIMEOUT)
                    break

                msg = await self._get_iopub_message()
                if msg is None:
                    await asyncio.sleep(0.05)
                    continue

                # Check for idle status message (execution complete)
                if (msg.get('header', {}).get('msg_type') == 'status' and 
                    msg.get('content', {}).get('execution_state') == 'idle'):
                    received_idle = True
                    continue

                # Process and yield output
                processed = self.output_manager.process_message(msg, msg_id)
                if processed:
                    yield processed
                    
        except Exception as e:
            logger.error(f"Streaming error: {e}")
            yield f"\n[Streaming error: {str(e)}]\n"
        finally:
            self.execution_tracker._finalize_execution(msg_id)

    async def _cleanup_executions(self, execution_tracker: ExecutionTracker = None):
        """Thread-safe execution cleanup"""
        try:
            logger.info("Starting execution cleanup")
            
            # 1. Interrupt kernel if alive
            if self.kernel_manager.is_kernel_alive():
                try:
                    self.kernel_manager.interrupt_kernel()
                except Exception as e:
                    logger.warning(f"Interrupt attempt failed: {str(e)}")

            # 2. Flush kernel channels
            self.kernel_manager.flush_channels()

            # 3. Clean execution tracker
            with self.execution_tracker._lock:
                self.execution_tracker._executions.clear()
                self.execution_tracker._output_buffers.clear()

            logger.info("Execution cleanup completed")
        except Exception as e:
            logger.error(f"Cleanup error: {str(e)}")
            raise
    
    async def _verify_kernel_ready(self, timeout: int = 10) -> bool:
        """Verify kernel is ready to accept commands"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                # Try a simple kernel info request
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: self.client.kernel_info()
                )
                return True
            except Exception:
                await asyncio.sleep(0.5)
        return False

    async def _start_channels_with_timeout(self, timeout: int):
        """Start channels with timeout handling"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    self.client.start_channels
                )
                
                # Verify channels are alive
                if all(channel.is_alive() for channel in [
                    self.client.shell_channel,
                    self.client.iopub_channel
                ]):
                    return
                    
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.warning(f"Channel start attempt failed: {e}")
                await asyncio.sleep(0.5)
        
        raise RuntimeError("Failed to start channels within timeout")