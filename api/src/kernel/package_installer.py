import sys
import os
import asyncio
import logging
import queue
from typing import AsyncGenerator, Optional
from concurrent.futures import ThreadPoolExecutor

# Setup minimal logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from internal.kernel_wrapper import KernelWrapper

class PackageInstaller:
    def __init__(self, kernel: KernelWrapper, max_workers: int = 4):
        self.kernel = kernel
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._shutdown_event = asyncio.Event()
        
    async def install_packages(self, packages: list, upgrade: bool = False) -> AsyncGenerator[str, None]:
        """Install packages with streaming output"""
        try:
            # Start kernel if not already running
            if not self.kernel.kernel_manager.is_alive():
                await self._execute_in_thread(self.kernel.kernel_manager.start_kernel)
                self.kernel.kernel_manager.client().start_channels()
            
            # Install each package with streaming output
            for pkg in packages:
                install_cmd = f"!pip install {pkg} --upgrade" if upgrade else f"!pip install {pkg}"
                yield f"\nInstalling {pkg}...\n"
                
                # Execute and stream output
                msg_id = await self._execute_in_thread(
                    lambda: self.kernel.kernel_manager.client().execute(install_cmd)
                )
                
                async for output in self._stream_output(msg_id):
                    yield output
                    
            yield "\nInstallation complete\n"
            
        except Exception as e:
            yield f"\nError during installation: {str(e)}\n"
            raise
        finally:
            await self._cleanup()

    async def _execute_in_thread(self, func) -> str:
        """Execute function in thread pool and return message ID"""
        loop = asyncio.get_running_loop()
        if callable(func):
            return await loop.run_in_executor(self._executor, func)
        return await loop.run_in_executor(self._executor, lambda: func)

    async def _stream_output(self, msg_id: str) -> AsyncGenerator[str, None]:
        """Stream output from kernel execution"""
        client = self.kernel.kernel_manager.client()
        received_idle = False
        
        try:
            while not self._shutdown_event.is_set() and not received_idle:
                msg = await self._get_iopub_message(client)
                if msg is None:
                    await asyncio.sleep(0.05)
                    continue

                # Check for idle status
                if (msg.get('header', {}).get('msg_type') == 'status' and 
                    msg.get('content', {}).get('execution_state') == 'idle'):
                    received_idle = True
                    continue

                # Process and yield output
                output = self._process_message(msg)
                if output:
                    yield output
                    
        except Exception as e:
            logger.error(f"Streaming error: {e}")
            yield f"\n[Streaming error: {str(e)}]\n"

    async def _get_iopub_message(self, client, timeout: float = 0.1) -> Optional[dict]:
        """Get message with minimal delay"""
        try:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(
                self._executor,
                lambda: client.get_iopub_msg(timeout=timeout)
            )
        except (queue.Empty, asyncio.TimeoutError):
            return None
        except Exception as e:
            logger.debug(f"Message error: {e}")
            return None

    def _process_message(self, msg: dict) -> str:
        """Process kernel message into output string"""
        content = msg.get('content', {})
        
        if 'text' in content:
            return content['text']
        elif 'data' in content:  # For rich output
            return content['data'].get('text/plain', '')
        elif 'name' in content and content['name'] == 'stdout':
            return content.get('text', '')
        return ''

    async def _cleanup(self):
        """Cleanup resources"""
        try:
            if hasattr(self.kernel, 'health_monitor'):
                self.kernel.health_monitor.pause_monitoring()
            
            await self._execute_in_thread(self.kernel.kernel_manager.shutdown_kernel)
        except Exception as e:
            logger.error(f"Cleanup error: {e}")
        finally:
            if hasattr(self.kernel, 'health_monitor'):
                self.kernel.health_monitor.resume_monitoring()

async def async_main():
    kw = KernelWrapper(kernel_name='python3')
    installer = PackageInstaller(kw)
    
    try:
        # Stream installation output
        async for output in installer.install_packages(
            ['numpy', 'matplotlib', 'seaborn'], 
            upgrade=True
        ):
            print(output, end='', flush=True)
            
    except Exception as e:
        print(f"\nError: {e}")
    finally:
        await installer._cleanup()

if __name__ == "__main__":
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
        kw = KernelWrapper(kernel_name='python3')
        kw.kernel_manager.shutdown_kernel()