from jupyter_client.manager import KernelManager

class KernelManagerInterface:
    def __init__(self, kernel_name: str = 'python3'):
        self.hasStarted = False
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
            self.hasStarted = True

        except Exception as e:
            raise RuntimeError(f"Failed to start kernel: {e}")
    
    def execute_code(self, code: str):
        '''
        Executes the provided code in the Jupyter kernel and returns the output.
        Args:
            code (str): The code to execute.
        Returns:
            list: A list of strings containing the output from the execution.'''
        if self.hasStarted:
            msg_id = self.client.execute(code)
            output = []

            while True:
                try:
                    msg = self.client.get_iopub_msg(timeout=2)
                    msg_type = msg['header']['msg_type']
                    if msg_type == 'stream':
                        output.append(msg['content']['text'])
                    elif msg_type in ('execute_result', 'display_data'):
                        output.append(msg['content']['data'].get('text/plain', ''))
                    elif msg_type == 'error':
                        output.append('\n'.join(msg['content']['traceback']))
                    elif msg_type == 'status' and msg['content']['execution_state'] == 'idle':
                        break
                except Exception:
                    break

            return output
        else:
            return ["Kernel has not been started. Please start the kernel first."]

    def get_kernel_info(self):
        '''
        Returns information about the current kernel.
        '''
        return self.kernel_manager.kernel_info()

    def shutdown_kernel(self):
        '''
        Shuts down the Jupyter kernel.
        '''
        self.kernel_manager.shutdown_kernel()

    def restart_kernel(self):
        '''
        Restarts the Jupyter kernel.'''
        self.kernel_manager.restart_kernel()

def getKernelManager(kernel_name: str = 'python3') -> KernelManagerInterface:
    '''
    Returns an instance of KernelManagerInterface.
    Args:
        kernel_name (str): The name of the kernel to start. Default is 'python3'.
    Returns:
        KernelManagerInterface: An instance of the kernel manager interface.
    '''
    return KernelManagerInterface(kernel_name=kernel_name)
