# health_monitor.py
import logging
import time
import threading
from kernel.kernel_manage import Kernel

# Setup logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KernelHealthMonitor:
    def __init__(self, kernel_manager: Kernel, check_interval: int = 10, shutdown_event: threading.Event = None):
        """Initialize KernelHealthMonitor with kernel manager and check interval.
        
        Args:
            kernel_manager (KernelManager): Manages kernel lifecycle operations.
            check_interval (int): Interval in seconds between health checks.
        """
        self.kernel_manager = kernel_manager
        self.check_interval = check_interval
        self._last_kernel_check = time.time()
        self._kernel_healthy = False
        self._shutdown_event = threading.Event()
        self._restart_in_progress = threading.Event()
        self._monitor_thread: Optional[threading.Thread] = None

    def start_monitoring(self):
        """Start the health monitoring thread."""
        monitor_thread = threading.Thread(
            target=self._health_monitor,
            daemon=True,
            name="KernelHealthMonitor"
        )
        monitor_thread.start()
        return monitor_thread

    def _health_monitor(self):
        """Monitor kernel health and trigger restart if needed."""
        while not self._shutdown_event.is_set():
            try:
                current_time = time.time()
                if current_time - self._last_kernel_check > self.check_interval:
                    self._check_kernel_health()
                    self._last_kernel_check = current_time
                time.sleep(5)  # Check every 5 seconds
            except Exception as e:
                logger.warning(f"Health monitor error: {e}")
                time.sleep(10)

    def _check_kernel_health(self):
        """Check if kernel is healthy and restart if needed."""
        try:
            if self._restart_in_progress.is_set():
                return  # Restart already in progress
            is_alive = self.kernel_manager.is_kernel_alive()
            if not is_alive and self._kernel_healthy:
                logger.warning("Kernel appears to be dead, attempting restart")
                self.kernel_manager._trigger_kernel_restart()
            self._kernel_healthy = is_alive
        except Exception as e:
            logger.error(f"Kernel health check failed: {e}")
            self._kernel_healthy = False

    def get_health_status(self) -> bool:
        """Return the current kernel health status."""
        return self._kernel_healthy

    def get_last_check_time(self) -> float:
        """Return the timestamp of the last kernel health check."""
        return self._last_kernel_check

    def shutdown(self):
        """Signal the health monitor to stop."""
        self._shutdown_event.set()
    
    def is_restart_in_progress(self) -> bool:
        """Check if a kernel restart is currently in progress."""
        return self._restart_in_progress.is_set()

    def _monitor_loop(self):
        """The main loop for the monitoring thread."""
        logger.info("Kernel health monitor thread started.")
        while not self._shutdown_event.is_set():
            try:
                status = self.kernel_manager.is_kernel_alive()
                with self._lock:
                    self._is_healthy = status
                    self._last_check_time = time.time()
                if not status:
                    logger.warning("Health check failed: Kernel is not alive.")
            except Exception as e:
                logger.error(f"Error during health check: {e}")
                with self._lock:
                    self._is_healthy = False
            
            self._shutdown_event.wait(self.check_interval)
        
        logger.info("Kernel health monitor thread has stopped.")
    
    def stop_monitoring(self):
        """
        Signals the monitoring thread to stop and waits for it to terminate.
        """
        if self._monitor_thread and self._monitor_thread.is_alive():
            logger.info("Stopping kernel health monitor...")
            
            # 1. Signal the thread to exit its loop
            self._shutdown_event.set()
            
            # 2. Wait for the thread to finish cleanly (with a timeout)
            self._monitor_thread.join(timeout=5)
            
            if self._monitor_thread.is_alive():
                logger.warning("Health monitor thread did not stop in time.")
            else:
                logger.info("Health monitor stopped successfully.")
        
        self._monitor_thread = None



    
