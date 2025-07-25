import sys
import os
import asyncio
import logging

#Setup minimal logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from internal.kernel_wrapper import KernelWrapper

async def background_kernel_cleanup(kernel: KernelWrapper):
    """Background cleanup for kernel resources."""
    try:
        if hasattr(kernel, 'health_monitor'):
            kernel.health_monitor.pause_monitoring()
        await asyncio.to_thread(kernel.kernel_manager.shutdown_kernel)
        logger.info("Background kernel cleanup completed")
    except Exception as e:
        logger.error(f"Background cleanup failed: {e}")
    finally:
        if hasattr(kernel, 'health_monitor'):
            kernel.health_monitor.resume_monitoring()

async def async_main():
    kw = KernelWrapper(kernel_name='python3')
    try:
        # Start kernel
        await asyncio.to_thread(kw.kernel_manager.start_kernel, timeout=30)
        
        # Install packages
        result = await kw.install_packages(['numpy', 'matplotlib', 'seaborn'], upgrade=True, timeout=300)
        print(f"Installation result: {result}")

        # Schedule background cleanup
        asyncio.create_task(background_kernel_cleanup(kw))
    except Exception as e:
        print(f"Error: {e}")
        await asyncio.to_thread(kw.kernel_manager.shutdown_kernel)

if __name__ == "__main__":
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
        kw = KernelWrapper(kernel_name='python3')
        kw.kernel_manager.shutdown_kernel()

