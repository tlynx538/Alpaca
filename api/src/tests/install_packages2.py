import sys
import os
import asyncio
import logging

# Setup minimal logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from internal.kernel_wrapper import KernelWrapper

async def background_kernel_cleanup(kernel: KernelWrapper = None, force=False):
    if not kernel:
        logger.warning("No kernel provided for cleanup")
        return False

    logger.info(f"Starting {'forced ' if force else ''}background kernel cleanup")

    try:
        if hasattr(kernel, 'health_monitor'):
            kernel.health_monitor.pause_monitoring()

        # Simulate maintenance cleanup, not full shutdown
        await asyncio.sleep(0.5)  # Simulated cleanup work
        logger.info("Kernel maintenance cleanup completed")
        return True

    except Exception as e:
        logger.error(f"Cleanup error: {e}")
        return False
    finally:
        if hasattr(kernel, 'health_monitor'):
            kernel.health_monitor.resume_monitoring()
        if hasattr(kernel, 'kernel_manager'):
            logger.info("Refreshing kernel client after cleanup")
            await asyncio.to_thread(kernel.kernel_manager.reconnect_client)


async def install_package_set(kw: KernelWrapper, packages: list, upgrade: bool = True, timeout: int = 30):
    """Install a set of packages and return results."""
    try:
        print(f"Installing packages: {packages} with upgrade={upgrade} and timeout={timeout}")
        result = await kw.install_packages(packages, upgrade=upgrade, timeout=timeout)
        print(f"Installation result for {packages}: {result}")
    except Exception as e:
        print(f"Error installing {packages}: {e}")
        raise

async def async_main():
    kw = KernelWrapper(kernel_name='python3')
    try:
        logger.info("Starting kernel")
        await asyncio.to_thread(kw.kernel_manager.start_kernel, timeout=300)

        logger.info("Installing first package set")
        await install_package_set(kw, ['numpy', 'matplotlib', 'seaborn'])

        logger.info("Running first cleanup")
        await background_kernel_cleanup(kw)

        # NEW: Stop monitor completely to test if it blocks install
        if hasattr(kw, 'health_monitor'):
            kw.health_monitor.stop_monitoring()
        
        logger.info("Refreshing kernel client after cleanup")
        await asyncio.to_thread(kw.kernel_manager.reconnect_client)

        logger.info("Installing second package set")
        await install_package_set(kw, ['pandas', 'requests','scikit-learn'])

        logger.info("Running final cleanup")
        await background_kernel_cleanup(kw)

    except Exception as e:
        logger.exception(f"Fatal error in main: {e}")
        await asyncio.to_thread(kw.kernel_manager.shutdown_kernel)


if __name__ == "__main__":
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
        kw = KernelWrapper(kernel_name='python3')
        kw.kernel_manager.shutdown_kernel()
