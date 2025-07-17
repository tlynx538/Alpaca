import sys
import os

# Add src directory to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from internal.kernel_wrapper import KernelWrapper  # Now works
# initialize Kernel 
kw = KernelWrapper(kernel_name='python3')
kw.kernel_manager.start_kernel()

# attempt to install a package 
try:
    kw.install_packages(['numpy','matplotlib','seaborn'], upgrade=True, timeout=60)
except Exception as e:
    print(f"Error during package installation: {e}")

kw.kernel_manager.shutdown_kernel()