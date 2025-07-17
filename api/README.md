# Europa Jupyter REST API

This REST API can
- provision an ipython kernel
- execute code on the kernel 
- maintain state of kernel during execution
  
The Main Goal of this API is to run python code via REST API.

## Future Improvements
- Improve execution of python code in various scenarios (in case if the kernel is busy executing code)
- See through history of previously executed code
- Streaming Output of Code Blocks

## Other Notes
- In src/internal/kernel.py we are currently using `jupyter_client.manager.KernelManager` to invoke the instance, we might consider getting into `provisioning` in future.
- Involving Security of running unsafe code


## Existing Routes
| Method | Path                        | Description                                 |
|--------|-----------------------------|---------------------------------------------|
| GET    | /api/kernel/start               | Start a Jupyter kernel                      |
| GET    | /api/kernel/restart             | Restarts Jupyter kernel                     |
| POST   | /api/kernel/execute             | Execute code in the kernel                  |
| GET    | /api/kernel/info                | Get info about the current kernel           |
| POST   | /api/kernel/shutdown            | Shutdown the kernel                         |
| POST   | /api/kernel/execute/complete    | Check if code is complete (code completeness check) |
| POST   | /api/kernel/install-packages    | Install Python Packages to Jupyter Kernel             |

### Run the FastAPI server
`fastapi dev main.py`