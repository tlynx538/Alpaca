from jupyter_client.manager import KernelManager
from fastapi.responses import StreamingResponse
from typing import Optional 
import asyncio
import ast 
import time 

class KernelWrapper:
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
    
    def is_long_running(self,code):
        """
        Determines if the code is long-running based on its structure.
        Args:
            code (str): The code to analyze.
        Returns:
            bool: True if the code is long-running, False otherwise."""
        try:
            tree = ast.parse(code)
            for node in ast.walk(tree):
                if isinstance(node, (ast.While, ast.For)):
                    return True
        except Exception:
            pass
        return any(keyword in code for keyword in ['time.sleep', 'asyncio'])

    def execute_code(self, code: str):
        """
        Executes code in the kernel and streams output live if it's long-running.

        Returns:
            StreamingResponse or list
        """
        if not self.client.is_alive():
            return ["Kernel has not been started. Please start the kernel first."]

        # Step 1: Skip completeness check if code is long-running
        if not self.is_long_running(code):
            try:
                reply = self.client.is_complete(code)
                if reply['status'] != 'complete':
                    return [f"Incomplete code: {reply.get('indent', '')}"]
            except Exception as e:
                return [f"Error checking code completeness: {e}"]

        # Step 2: Execute code and stream output
        try:
            msg_id = self.client.execute(code)
            self.running_executions[msg_id] = 'busy'

            async def stream():
                start_time = time.time()
                timeout_seconds = 30
                max_iterations = 300
                iterations = 0

                try:
                    while iterations < max_iterations and (time.time() - start_time) < timeout_seconds:
                        iterations += 1
                        try:
                            msg = self.client.get_iopub_msg(timeout=1)
                            msg_type = msg['header']['msg_type']
                            content = msg['content']

                            if msg_type == 'status' and content['execution_state'] == 'idle':
                                self.running_executions[msg_id] = 'idle'
                                break
                            elif msg_type == 'stream':
                                yield content.get('text', '')
                            elif msg_type in ('execute_result', 'display_data'):
                                yield content.get('data', {}).get('text/plain', '')
                            elif msg_type == 'error':
                                yield '\n'.join(content.get('traceback', []))
                        except Exception:
                            await asyncio.sleep(0.2)
                    else:
                        yield "\n[Warning: execution timeout or loop limit reached]\n"
                except Exception as e:
                    yield f"[Streaming error: {e}]"

            return StreamingResponse(stream(), media_type='text/plain')

        except Exception as e:
            return [f"Failed to execute code: {e}"]

        
    def get_execution_state(self, msg_id: str) -> Optional[str]:
        """
        Returns the state of a specific execution by its message ID.
        Args:
            msg_id (str): The message ID of the execution.
        Returns:
            Optional[str]: The state of the execution ('busy', 'idle', 'error') or None if not found.
        """
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
    
