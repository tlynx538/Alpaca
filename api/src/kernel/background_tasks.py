# background_tasks.py
import logging
import time
import threading
import gc
from kernel.execution_tracker import ExecutionTracker
from kernel.output_buffer import OutputBufferManager

# Setup logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BackgroundTaskManager:
    def __init__(self, execution_tracker: ExecutionTracker, output_manager: OutputBufferManager, shutdown_event: threading.Event, cleanup_interval: int = 30):
        """Initialize BackgroundTaskManager with necessary dependencies.
        
        Args:
            execution_tracker (ExecutionTracker): Manages execution state cleanup.
            output_manager (OutputBufferManager): Manages output buffer cleanup.
            shutdown_event (threading.Event): Event to signal shutdown of background tasks.
            cleanup_interval (int): Interval in seconds between cleanup operations.
        """
        self.execution_tracker = execution_tracker
        self.output_manager = output_manager
        self.shutdown_event = shutdown_event
        self.cleanup_interval = cleanup_interval

    def start_tasks(self):
        """Start all background tasks."""
        cleanup_thread = threading.Thread(
            target=self._background_cleanup,
            daemon=True,
            name="KernelCleanup"
        )
        cleanup_thread.start()
        return cleanup_thread

    def _background_cleanup(self):
        """Enhanced background cleanup with better logic."""
        while not self.shutdown_event.is_set():
            try:
                current_time = time.time()
                
                # Clean up stale executions
                self.execution_tracker._cleanup_stale_executions(current_time)
                
                # Clean up oversized buffers using OutputBufferManager
                self.output_manager.cleanup_oversized_buffers()
                
                # Clean up completed executions if over limit
                self.execution_tracker._cleanup_excess_executions()
                
                # Force garbage collection periodically
                if current_time % 120 < self.cleanup_interval:  # Every 2 minutes
                    gc.collect()
                
                time.sleep(self.cleanup_interval)
            except Exception as e:
                logger.warning(f"Background cleanup error: {e}")
                time.sleep(5)
