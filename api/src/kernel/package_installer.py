# package_installer.py
import logging
import time
import queue
from typing import Dict, Union, List, Callable
from kernel.kernel_manage import Kernel

# Setup logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PackageInstaller:
    def __init__(self, kernel_manager: Kernel):
        """Initialize PackageInstaller with kernel manager.
        
        Args:
            kernel_manager (KernelManager): Manages kernel operations for package installation.
        """
        self.kernel_manager = kernel_manager

    def install_packages(self, packages: Union[str, List[str]], upgrade: bool = False, timeout: int = 300) -> Dict[str, str]:
        """
        Install one or more Python packages in the kernel using pip.
        
        Args:
            packages: Either a single package name or list of packages
                      (supports pip syntax like "numpy>=1.20", "pandas[extra]")
            upgrade: Whether to upgrade if already installed
            timeout: Maximum installation time in seconds per package
        
        Returns:
            Dictionary with package names as keys and installation results as values
        """
        if not self.kernel_manager.is_kernel_alive():
            raise RuntimeError("Cannot install packages - kernel is not running")

        # Normalize input to list
        if isinstance(packages, str):
            packages = [packages]
        elif not isinstance(packages, list):
            raise ValueError("packages must be string or list of strings")

        results = {}
        pip_command_template = "pip install {package} {upgrade} --quiet"
        
        for package in packages:
            try:
                # Skip empty strings
                if not package or not isinstance(package, str):
                    results[package] = "Invalid package name"
                    continue

                # Check if already installed (basic check)
                base_pkg = package.split('[')[0].split('==')[0].split('>')[0].split('<')[0]
                check_code = f"import importlib.util; print(importlib.util.find_spec('{base_pkg}') is not None)"
                
                check_result = []
                def collect_check_output(msg):
                    if msg['header']['msg_type'] in ('stream', 'execute_result'):
                        check_result.append(msg['content'].get('text', ''))
                
                msg_id = self.kernel_manager.client.execute(check_code)
                self._capture_output(msg_id, collect_check_output, timeout=10)

                if any("True" in line for line in check_result) and not upgrade:
                    results[package] = "Already installed"
                    continue

                # Build install command
                cmd = pip_command_template.format(
                    package=package,
                    upgrade="--upgrade" if upgrade else ""
                )

                # Special handling for specific packages
                post_install = ""
                if "matplotlib" in package.lower():
                    post_install = " && python -c \"import matplotlib; matplotlib.use('Agg')\""

                # Execute installation
                install_output = []
                def collect_install_output(msg):
                    if msg['header']['msg_type'] in ('stream', 'execute_result'):
                        install_output.append(msg['content'].get('text', ''))
                    elif msg['header']['msg_type'] == 'error':
                        install_output.extend(msg['content']['traceback'])

                msg_id = self.kernel_manager.client.execute(cmd + post_install)
                success = self._capture_output(msg_id, collect_install_output, timeout)

                # Verify installation
                verify_result = []
                def collect_verify_output(msg):
                    if msg['header']['msg_type'] in ('stream', 'execute_result'):
                        verify_result.append(msg['content'].get('text', ''))

                msg_id = self.kernel_manager.client.execute(check_code)
                self._capture_output(msg_id, collect_verify_output, timeout=10)

                if any("True" in line for line in verify_result):
                    results[package] = "Success"
                else:
                    results[package] = f"Failed: {''.join(install_output)[:500]}..."

            except Exception as e:
                results[package] = f"Error: {str(e)}"

        return results

    def _capture_output(self, msg_id: str, callback: Callable, timeout: int) -> bool:
        """Helper method to capture execution output.
        
        Args:
            msg_id (str): The message ID of the execution.
            callback (Callable): Function to process the output message.
            timeout (int): Timeout for capturing output in seconds.
            
        Returns:
            bool: True if execution completed within timeout, False otherwise.
        """
        start_time = time.time()
        while (time.time() - start_time) < timeout:
            try:
                msg = self.kernel_manager.client.get_iopub_msg(timeout=1.0)
                if msg['parent_header']['msg_id'] == msg_id:
                    if msg['header']['msg_type'] == 'status':
                        if msg['content']['execution_state'] == 'idle':
                            return True
                    callback(msg)
            except queue.Empty:
                continue
        return False
