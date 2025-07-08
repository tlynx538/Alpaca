import sys
import os

# Add src directory to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from internal.kernel_wrapper import KernelWrapper  # Now works
KernelWrapper(kernel_name='python3')