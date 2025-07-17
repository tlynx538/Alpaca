# kernel_wrapper.py
from kernel.kernel_manage import Kernel
from kernel.execution_tracker import ExecutionTracker, ExecutionState
from kernel.output_buffer import OutputBufferManager
from kernel.code_executor import CodeExecutor
from kernel.health_monitor import KernelHealthMonitor
from fastapi.responses import StreamingResponse
from typing import Dict, Any, Union, List, Optional
import threading
import logging
import gc
import time
import asyncio
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KernelWrapper:
    def __init__(self, kernel_name: str = 'python3', max_executions: int = 50, max_concurrent_executions: int = 3):
        """
        Improved Jupyter Kernel Manager with enhanced safety.
        
        Args:
            kernel_name (str): Name of the kernel to initialize.
            max_executions (int): Maximum number of executions to track.
            max_concurrent_executions (int): Maximum number of concurrent executions allowed.
        """
        self.max_executions = max_executions
        self._shutdown_event = threading.Event()
        
        # Initialize core components
        self.execution_tracker = ExecutionTracker(max_executions=max_executions)
        self.output_manager = OutputBufferManager(self.execution_tracker)

        try:
            self.kernel_manager = Kernel(kernel_name=kernel_name)
            logger.info(f"Kernel {kernel_name} initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize kernel: {e}")
            raise RuntimeError(f"Failed to initialize kernel: {e}")
        
        self.code_executor = CodeExecutor(
            kernel_manager=self.kernel_manager,
            execution_tracker=self.execution_tracker,
            output_manager=self.output_manager,
            max_concurrent_executions=max_concurrent_executions
        )
        
        self.health_monitor = KernelHealthMonitor(
            kernel_manager=self.kernel_manager,
            shutdown_event=self._shutdown_event
        )
        
        # Start background tasks
        self._start_background_tasks()

    @property
    def kernel_pid(self) -> Optional[int]:
        """Get the kernel process ID"""
        return self.kernel_manager.get_kernel_pid()

    def is_kernel_alive(self) -> bool:
        """Check if kernel is alive"""
        return self.kernel_manager.is_kernel_alive()

    async def cleanup(self, force=False):
        """Clean up kernel resources"""
        await asyncio.to_thread(self.kernel_manager.cleanup, force)

    def _start_background_tasks(self):
        """Start background cleanup and monitoring tasks"""
        self._cleanup_thread = threading.Thread(
            target=self._background_cleanup,
            daemon=True,
            name="KernelCleanup"
        )
        self._cleanup_thread.start()
        self.health_monitor.start_monitoring()

    def _background_cleanup(self):
        """Enhanced background cleanup with better logic"""
        cleanup_interval = 30  # seconds
        
        while not self._shutdown_event.is_set():
            try:
                current_time = time.time()
                self.execution_tracker._cleanup_stale_executions(current_time)
                self.output_manager.cleanup_oversized_buffers()
                self.execution_tracker._cleanup_excess_executions()
                
                if current_time % 120 < cleanup_interval:  # Every 2 minutes
                    gc.collect()
                
                time.sleep(cleanup_interval)
            except Exception as e:
                logger.warning(f"Background cleanup error: {e}")
                time.sleep(5)

    def __del__(self):
        """Destructor for cleanup"""
        if hasattr(self, '_shutdown_event') and not self._shutdown_event.is_set():
            self._shutdown_event.set()
            self.kernel_manager.shutdown_kernel()

    def execute_code(self, code: str, timeout: int = 30) -> StreamingResponse:
        """Execute user code using CodeExecutor."""
        return self.code_executor.execute_code(code, timeout)

    async def install_packages(self, packages: Union[str, List[str]], 
                         upgrade: bool = False, 
                         timeout: int = 300) -> Dict[str, str]:
        """
        Install packages using kernel execution.
        
        Args:
            packages: Either a single package name or list of packages.
            upgrade: Whether to upgrade if already installed.
            timeout: Maximum installation time in seconds per package.
        
        Returns:
            Dictionary with package names as keys and installation results as values.
        """
        if isinstance(packages, str):
            packages = [packages]
            
        results = {}
        for pkg in packages:
            cmd = f"!pip install {pkg} --upgrade" if upgrade else f"!pip install {pkg}"
            try:
                # Using code executor for consistent execution handling
                response = self.execute_code(cmd, timeout)
                results[pkg] = "Success"
            except Exception as e:
                results[pkg] = f"Failed: {str(e)}"
        
        return results

    def get_stats(self) -> Dict[str, Any]:
        """Enhanced statistics"""
        with self.execution_tracker.execution_lock:
            state_counts = defaultdict(int)
            total_buffer_size = 0
            
            for exec_info in self.execution_tracker.executions.values():
                state_counts[exec_info.state.value] += 1
            
            for buffer in self.execution_tracker.output_buffers.values():
                total_buffer_size += buffer.size()
            
            return {
                'total_executions': len(self.execution_tracker.executions),
                'state_counts': dict(state_counts),
                'total_buffer_size': total_buffer_size,
                'kernel_alive': self.kernel_manager.is_kernel_alive(),
                'kernel_healthy': self.health_monitor.get_health_status(),
                'restart_in_progress': self.health_monitor.is_restart_in_progress(),
                'last_kernel_check': self.health_monitor.get_last_check_time()
            }
        
    def perform_restart(self, lock: threading.Lock):
        """
        A synchronous, blocking method designed to be run in a background thread.
        This safely handles the entire kernel restart lifecycle.
        """
        logger.info("Background kernel restart process has started.")
        try:
            # 1. Gracefully stop background monitoring
            logger.info("Stopping health monitor and background threads for restart.")
            if hasattr(self, '_shutdown_event'):
                self._shutdown_event.set()
            if hasattr(self, 'health_monitor'):
                self.health_monitor.stop_monitoring()
            if hasattr(self, '_cleanup_thread') and self._cleanup_thread.is_alive():
                self._cleanup_thread.join(timeout=5)

            # 2. Shut down the old kernel manager
            logger.info("Shutting down the current kernel manager instance.")
            if self.kernel_manager and self.kernel_manager.is_kernel_alive():
                self.kernel_manager.shutdown_kernel()

            # 3. Create and start a new kernel instance
            logger.info("Creating and starting a new kernel manager instance.")
            new_kernel_manager = Kernel(kernel_name=self.kernel_manager.kernel_name)
            new_kernel_manager.start_kernel()

            if not new_kernel_manager.is_kernel_alive():
                raise RuntimeError("The new kernel failed to start during restart.")

            # 4. Re-initialize the wrapper with the new kernel
            logger.info("Re-initializing wrapper components with the new kernel.")
            self.kernel_manager = new_kernel_manager
            self.code_executor.kernel_manager = self.kernel_manager

            # 5. Restart background services
            logger.info("Restarting health monitor and background threads.")
            self._shutdown_event.clear()
            self.health_monitor = KernelHealthMonitor(
                kernel_manager=self.kernel_manager,
                shutdown_event=self._shutdown_event
            )
            self._start_background_tasks()

            logger.info("Kernel restart completed successfully.")

        except Exception as e:
            logger.critical(f"A critical error occurred during kernel restart: {e}", exc_info=True)
        finally:
            lock.release()
            logger.info("Restart lock has been released.")