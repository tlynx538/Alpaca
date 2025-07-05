from jupyter_client.manager import KernelManager
from typing import Optional 
import threading
import queue

class KernelManagerInterface:
    def __init__(self, kernel_name: str = 'python3'):
        """
        According to the Jupyter Client documentation:
        "Manages a single kernel in a subprocess on this host."
        This is by design.

        Description:
        Initializes Jupyter Kernel Manager and exposes client for API
        to use.
        # starts python3 by default, specify for different kernel
        Usage: KernelManagerInterface(kernel_name='python3') 
        """
        try:
            self.kernel_manager = KernelManager(kernel_name=kernel_name)
            self.kernel_manager.start_kernel()
            self.client = self.kernel_manager.client()
            self.client.start_channels()

            self.running_executions = {} # Message ID -> busy /idle / error 
            self.output_buffers = {} # Message ID -> list of outputs
            self.output_queues = {} # Queue to collect outputs asynchronously



        except Exception as e:
            raise RuntimeError(f"Failed to start kernel: {e}")
    
    def execute_code(self, code: str):
        """
        Executes the provided code in the Jupyter kernel after checking for completeness.

        Args:
            code (str): The code to execute.

        Returns:
            list: [msg_id, output] if execution was successful,
                or [error_message] if kernel is not alive, code is incomplete, or an error occurred.
        """
        if not self.client.is_alive():
            return ["Kernel has not been started. Please start the kernel first."]

        # Step 1: Check if the code is complete
        try:
            completeness = self.client.is_complete(code, reply=True, timeout=2)
            if completeness['content']['status'] != 'complete':
                return [f"Incomplete code: {completeness['content'].get('indent', '')}"]
        except Exception as e:
            return [f"Error checking code completeness: {e}"]

        # Step 2: Proceed with code execution
        try:
            msg_id = self.client.execute(code)
            self.running_executions[msg_id] = 'busy'
            output = []

            while True:
                try:
                    msg = self.client.get_iopub_msg(timeout=2)
                    msg_type = msg['header']['msg_type']
                    content = msg['content']

                    if msg_type == 'status':
                        if content['execution_state'] == 'idle':
                            self.running_executions[msg_id] = 'idle'
                            break
                    elif msg_type == 'stream':
                        output.append(content.get('text', ''))
                    elif msg_type in ('execute_result', 'display_data'):
                        output.append(content.get('data', {}).get('text/plain', ''))
                    elif msg_type == 'error':
                        output.append('\n'.join(content.get('traceback', [])))
                except Exception as e:
                    output.append(f"[Error reading message: {e}]")
                    break

            return [msg_id, output]

        except Exception as e:
            return [f"Failed to execute code: {e}"]
        
    def get_execution_state(self, msg_id: str) -> Optional[str]:
        return self.running_executions.get(msg_id)

    
    def is_kernel_running(self):
        '''
        Checks if the Jupyter kernel is running.
        Returns:
            bool: True if the kernel is running, False otherwise.
        '''
        return self.client.is_alive()

    def get_kernel_info(self):
        '''
        Returns information about the current kernel.
        '''
        return self.client.kernel_info()

    def shutdown_kernel(self):
        '''
        Shuts down the Jupyter kernel.
        '''
        self.kernel_manager.shutdown_kernel()

    def restart_kernel(self):
        '''
        Restarts the Jupyter kernel.'''
        self.kernel_manager.restart_kernel()
    
