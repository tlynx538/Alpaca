from fastapi import APIRouter
from internal.kernel import KernelWrapper
from fastapi.responses import StreamingResponse
from internal.models import CodeRequest, CodeCompleteRequest

router = APIRouter()

kernel_manager = None 


@router.get("/kernel/start")
async def start_kernel(kernel_name: str = 'python3'):
    """
    Starts a Jupyter kernel with the specified name.
    Default is 'python3'.
    """
    global kernel_manager
    try:
        kernel_manager = KernelWrapper()
        return {"status": "Kernel started successfully", "kernel_name": kernel_name}
    except Exception as e:
        return {"status": "Error", "message": str(e)}

@router.get("/kernel/restart")
async def restart_kernel():
    """
    Restarts the Jupyter kernel.
    """
    try:
        kernel_manager.restart_kernel()
        return {"status": "Kernel restarted successfully"}
    except Exception as e:
        return {"status": "Error", "message": str(e)}

@router.post("/kernel/execute")
async def execute_code(request: CodeRequest):
    result = kernel_manager.execute_code(request.code)
    if isinstance(result, StreamingResponse):
        return result
    return {"status": "error", "message": result[0]}  # plain error response

    
@router.get("/kernel/info")
async def get_kernel_info():
    """
    Returns information about the current Jupyter kernel.
    """
    try:
        info = kernel_manager.get_kernel_info()
        return {"status": "Kernel info retrieved successfully", "info": info}
    except Exception as e:
        return {"status": "Error", "message": str(e)}

@router.get("/kernel/status")
async def is_kernel_alive():
    """
    Checks if the Jupyter kernel is running.
    """
    try:
        is_alive = kernel_manager.is_kernel_running()
        return {"status": "Kernel status checked", "is_alive": is_alive}
    except Exception as e:
        return {"status": "Error", "message": str(e)}

@router.post("/kernel/execute/status")
async def get_execution_status(request: CodeCompleteRequest):
    """
    Checks the status of a code execution in the Jupyter kernel.
    """
    try:
        status = kernel_manager.get_execution_status(request.msg_id)
        return {"status": "Execution status checked", "execution_status": status}
    except Exception as e:
        return {"status": "Error", "message": str(e)}


@router.post("/kernel/shutdown")
async def shutdown_kernel():
    """
    Shuts down the Jupyter kernel.
    """
    try:
        kernel_manager.shutdown_kernel()
        return {"status": "Kernel shut down successfully"}
    except Exception as e:
        return {"status": "Error", "message": str(e)}