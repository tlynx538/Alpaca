# kernel_wrapper.py
from kernel.kernel_manage import Kernel
from kernel.execution_tracker import ExecutionTracker, ExecutionState
from kernel.output_buffer import OutputBufferManager
from kernel.code_executor import CodeExecutor
from kernel.health_monitor import KernelHealthMonitor
from kernel.package_installer import PackageInstaller
from fastapi.responses import StreamingResponse
from typing import Dict, Any, Union, List, Callable
import threading
import logging
import gc
import time
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
        
        # Initialize ExecutionTracker for managing executions
        self.execution_tracker = ExecutionTracker(max_executions=max_executions)
        
        # Initialize OutputBufferManager for handling output processing
        self.output_manager = OutputBufferManager(self.execution_tracker)
        
        # Initialize KernelManager
        try:
            self.kernel_manager = Kernel(kernel_name=kernel_name)
            logger.info(f"Kernel {kernel_name} initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize kernel: {e}")
            raise RuntimeError(f"Failed to initialize kernel: {e}")
        
        # Initialize CodeExecutor for handling code execution
        self.code_executor = CodeExecutor(
            kernel_manager=self.kernel_manager,
            execution_tracker=self.execution_tracker,
            output_manager=self.output_manager,
            max_concurrent_executions=max_concurrent_executions
        )
        
        # Initialize KernelHealthMonitor for health monitoring
        self.health_monitor = KernelHealthMonitor(
            kernel_manager=self.kernel_manager,
            shutdown_event=self._shutdown_event
        )
        
        # Initialize PackageInstaller for package management
        self.package_installer = PackageInstaller(
            kernel_manager=self.kernel_manager
        )
        
        # Start background tasks
        self._start_background_tasks()

    def _start_background_tasks(self):
        """Start background cleanup and monitoring tasks"""
        self._cleanup_thread = threading.Thread(
            target=self._background_cleanup,
            daemon=True,
            name="KernelCleanup"
        )
        self._cleanup_thread.start()
        
        # Start health monitoring thread via KernelHealthMonitor
        self.health_monitor.start_monitoring()

    def _background_cleanup(self):
        """Enhanced background cleanup with better logic"""
        cleanup_interval = 30  # seconds
        
        while not self._shutdown_event.is_set():
            try:
                current_time = time.time()
                
                # Clean up stale executions
                self.execution_tracker._cleanup_stale_executions(current_time)
                
                # Clean up oversized buffers using OutputBufferManager
                self.output_manager.cleanup_oversized_buffers()
                
                # Clean up completed executions if over limit
                self.execution_tracker._cleanup_excess_executions()
                
                # Force garbage collection periodically
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
        """Execute user code using CodeExecutor.
        
        Args:
            code (str): The code to execute.
            timeout (int): Timeout for streaming output in seconds.
            
        Returns:
            StreamingResponse: A streaming response object for real-time output.
        """
        return self.code_executor.execute_code(code, timeout)

    def install_packages(self, packages: Union[str, List[str]], 
                         upgrade: bool = False, 
                         timeout: int = 300) -> Dict[str, str]:
        """Install packages using PackageInstaller.
        
        Args:
            packages: Either a single package name or list of packages.
            upgrade: Whether to upgrade if already installed.
            timeout: Maximum installation time in seconds per package.
        
        Returns:
            Dictionary with package names as keys and installation results as values.
        """
        return self.package_installer.install_packages(packages, upgrade, timeout)

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
