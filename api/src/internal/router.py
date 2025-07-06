from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from fastapi.responses import StreamingResponse, JSONResponse
from internal.kernel import KernelWrapper
from internal.models import CodeRequest, CodeCompleteRequest
from typing import Optional, Dict, Any
import logging
import asyncio
from contextlib import asynccontextmanager
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()

# Global kernel manager with proper lifecycle management
kernel_manager: Optional[KernelWrapper] = None
kernel_stats = {
    "started_at": None,
    "total_executions": 0,
    "failed_executions": 0,
    "restarts": 0
}

# Helper functions
def get_kernel_manager() -> KernelWrapper:
    """Dependency to ensure kernel is available"""
    global kernel_manager
    if kernel_manager is None:
        raise HTTPException(
            status_code=503, 
            detail="Kernel not started. Please start the kernel first."
        )
    if not kernel_manager.is_kernel_running():
        raise HTTPException(
            status_code=503,
            detail="Kernel is not running. Please restart the kernel."
        )
    return kernel_manager

def create_success_response(message: str, data: Optional[Dict[Any, Any]] = None) -> Dict[str, Any]:
    """Standardized success response format"""
    response = {
        "status": "success",
        "message": message,
        "timestamp": time.time()
    }
    if data:
        response["data"] = data
    return response

def create_error_response(message: str, error_code: Optional[str] = None) -> Dict[str, Any]:
    """Standardized error response format"""
    response = {
        "status": "error",
        "message": message,
        "timestamp": time.time()
    }
    if error_code:
        response["error_code"] = error_code
    return response

async def background_kernel_cleanup():
    """Background task for kernel cleanup"""
    global kernel_manager
    if kernel_manager:
        try:
            kernel_manager.force_cleanup()
            logger.info("Background cleanup completed")
        except Exception as e:
            logger.error(f"Background cleanup failed: {e}")

# Routes
@router.get("/kernel/start")
async def start_kernel(
    kernel_name: str = 'python3',
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """
    Starts a Jupyter kernel with the specified name.
    
    Args:
        kernel_name: Type of kernel to start (default: python3)
        
    Returns:
        Success/error response with kernel information
    """
    global kernel_manager, kernel_stats
    
    try:
        # Check if kernel is already running
        if kernel_manager is not None and kernel_manager.is_kernel_running():
            return create_success_response(
                "Kernel is already running",
                {
                    "kernel_name": kernel_name,
                    "stats": kernel_manager.get_stats()
                }
            )
        
        # Start new kernel
        kernel_manager = KernelWrapper(kernel_name=kernel_name)
        kernel_stats["started_at"] = time.time()
        kernel_stats["total_executions"] = 0
        kernel_stats["failed_executions"] = 0
        
        # Schedule background cleanup
        background_tasks.add_task(background_kernel_cleanup)
        
        logger.info(f"Kernel {kernel_name} started successfully")
        
        return create_success_response(
            f"Kernel {kernel_name} started successfully",
            {
                "kernel_name": kernel_name,
                "started_at": kernel_stats["started_at"],
                "stats": kernel_manager.get_stats()
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to start kernel: {e}")
        kernel_manager = None
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                f"Failed to start kernel: {str(e)}",
                "KERNEL_START_FAILED"
            )
        )

@router.get("/kernel/restart")
async def restart_kernel(
    background_tasks: BackgroundTasks = BackgroundTasks(),
    kernel: KernelWrapper = Depends(get_kernel_manager)
):
    """
    Restarts the Jupyter kernel.
    
    Returns:
        Success/error response with restart information
    """
    global kernel_stats
    
    try:
        # Get stats before restart
        old_stats = kernel.get_stats()
        
        # Restart kernel
        kernel.restart_kernel()
        kernel_stats["restarts"] += 1
        
        # Schedule cleanup after restart
        background_tasks.add_task(background_kernel_cleanup)
        
        logger.info("Kernel restarted successfully")
        
        return create_success_response(
            "Kernel restarted successfully",
            {
                "restart_count": kernel_stats["restarts"],
                "previous_uptime": old_stats.get("uptime", 0),
                "stats": kernel.get_stats()
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to restart kernel: {e}")
        kernel_stats["failed_executions"] += 1
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                f"Failed to restart kernel: {str(e)}",
                "KERNEL_RESTART_FAILED"
            )
        )

@router.post("/kernel/execute")
async def execute_code(
    request: CodeRequest,
    timeout: int = 30,
    kernel: KernelWrapper = Depends(get_kernel_manager)
):
    """
    Executes Python code in the Jupyter kernel.
    
    Args:
        request: Code execution request
        timeout: Maximum execution time in seconds
        
    Returns:
        StreamingResponse with execution output or error response
    """
    global kernel_stats
    
    try:
        # Validate request
        if not request.code or not request.code.strip():
            raise HTTPException(
                status_code=400,
                detail=create_error_response(
                    "Code cannot be empty",
                    "INVALID_CODE"
                )
            )
        
        # Check if code is too long (basic DOS protection)
        if len(request.code) > 100000:  # 100KB limit
            raise HTTPException(
                status_code=400,
                detail=create_error_response(
                    "Code too long. Maximum 100KB allowed.",
                    "CODE_TOO_LONG"
                )
            )
        
        # Execute code
        result = kernel.execute_code(request.code, timeout=timeout)
        kernel_stats["total_executions"] += 1
        
        if isinstance(result, StreamingResponse):
            logger.info(f"Code execution started (streaming)")
            return result
        else:
            # Non-streaming response (error case)
            kernel_stats["failed_executions"] += 1
            error_message = result[0] if result else "Unknown execution error"
            
            return JSONResponse(
                status_code=400,
                content=create_error_response(
                    error_message,
                    "EXECUTION_ERROR"
                )
            )
            
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Unexpected error during code execution: {e}")
        kernel_stats["failed_executions"] += 1
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                f"Unexpected execution error: {str(e)}",
                "EXECUTION_FAILED"
            )
        )

@router.get("/kernel/info")
async def get_kernel_info(kernel: KernelWrapper = Depends(get_kernel_manager)):
    """
    Returns information about the current Jupyter kernel.
    
    Returns:
        Kernel information and statistics
    """
    try:
        info = kernel.get_kernel_info()
        stats = kernel.get_stats()
        
        return create_success_response(
            "Kernel info retrieved successfully",
            {
                "kernel_info": info,
                "stats": stats,
                "session_stats": kernel_stats
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to get kernel info: {e}")
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                f"Failed to get kernel info: {str(e)}",
                "KERNEL_INFO_FAILED"
            )
        )

@router.get("/kernel/status")
async def get_kernel_status():
    """
    Checks if the Jupyter kernel is running and provides detailed status.
    
    Returns:
        Detailed kernel status information
    """
    global kernel_manager, kernel_stats
    
    try:
        if kernel_manager is None:
            return create_success_response(
                "Kernel not initialized",
                {
                    "is_alive": False,
                    "status": "not_started",
                    "session_stats": kernel_stats
                }
            )
        
        is_alive = kernel_manager.is_kernel_running()
        stats = kernel_manager.get_stats() if is_alive else {}
        active_executions = kernel_manager.get_active_executions() if is_alive else {}
        
        return create_success_response(
            f"Kernel status: {'running' if is_alive else 'stopped'}",
            {
                "is_alive": is_alive,
                "status": "running" if is_alive else "stopped",
                "stats": stats,
                "active_executions": active_executions,
                "session_stats": kernel_stats
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to check kernel status: {e}")
        return create_error_response(
            f"Failed to check kernel status: {str(e)}",
            "STATUS_CHECK_FAILED"
        )

@router.post("/kernel/execution/status")
async def get_execution_status(
    request: CodeCompleteRequest,
    kernel: KernelWrapper = Depends(get_kernel_manager)
):
    """
    Checks the status of a specific code execution.
    
    Args:
        request: Request containing message ID
        
    Returns:
        Execution status information
    """
    try:
        if not request.msg_id:
            raise HTTPException(
                status_code=400,
                detail=create_error_response(
                    "Message ID cannot be empty",
                    "INVALID_MSG_ID"
                )
            )
        
        status = kernel.get_execution_state(request.msg_id)
        
        return create_success_response(
            "Execution status retrieved",
            {
                "msg_id": request.msg_id,
                "execution_status": status,
                "is_active": status == "busy" if status else False
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get execution status: {e}")
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                f"Failed to get execution status: {str(e)}",
                "EXECUTION_STATUS_FAILED"
            )
        )

@router.post("/kernel/cleanup")
async def force_cleanup(
    background_tasks: BackgroundTasks = BackgroundTasks(),
    kernel: KernelWrapper = Depends(get_kernel_manager)
):
    """
    Forces cleanup of kernel resources and old executions.
    
    Returns:
        Cleanup status information
    """
    try:
        # Get stats before cleanup
        old_stats = kernel.get_stats()
        
        # Force cleanup
        kernel.force_cleanup()
        
        # Schedule background cleanup
        background_tasks.add_task(background_kernel_cleanup)
        
        # Get stats after cleanup
        new_stats = kernel.get_stats()
        
        logger.info("Force cleanup completed")
        
        return create_success_response(
            "Cleanup completed successfully",
            {
                "cleaned_executions": old_stats.get("total_executions", 0),
                "remaining_executions": new_stats.get("total_executions", 0),
                "stats": new_stats
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to cleanup: {e}")
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                f"Failed to cleanup: {str(e)}",
                "CLEANUP_FAILED"
            )
        )

@router.get("/kernel/executions")
async def get_active_executions(kernel: KernelWrapper = Depends(get_kernel_manager)):
    """
    Returns information about currently active executions.
    
    Returns:
        Active execution information
    """
    try:
        active_executions = kernel.get_active_executions()
        
        return create_success_response(
            f"Found {len(active_executions)} active executions",
            {
                "active_count": len(active_executions),
                "executions": active_executions
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to get active executions: {e}")
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                f"Failed to get active executions: {str(e)}",
                "ACTIVE_EXECUTIONS_FAILED"
            )
        )

@router.post("/kernel/shutdown")
async def shutdown_kernel():
    """
    Shuts down the Jupyter kernel and cleans up resources.
    
    Returns:
        Shutdown status information
    """
    global kernel_manager, kernel_stats
    
    try:
        if kernel_manager is None:
            return create_success_response(
                "Kernel was not running",
                {"was_running": False}
            )
        
        # Get final stats
        final_stats = kernel_manager.get_stats() if kernel_manager.is_kernel_running() else {}
        
        # Shutdown kernel
        kernel_manager.shutdown_kernel()
        
        # Reset global state
        uptime = time.time() - kernel_stats["started_at"] if kernel_stats["started_at"] else 0
        final_session_stats = {**kernel_stats, "uptime": uptime}
        
        kernel_manager = None
        kernel_stats = {
            "started_at": None,
            "total_executions": 0,
            "failed_executions": 0,
            "restarts": 0
        }
        
        logger.info("Kernel shutdown completed")
        
        return create_success_response(
            "Kernel shut down successfully",
            {
                "final_stats": final_stats,
                "session_stats": final_session_stats
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to shutdown kernel: {e}")
        # Still reset the global state even if shutdown failed
        kernel_manager = None
        
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                f"Failed to shutdown kernel: {str(e)}",
                "SHUTDOWN_FAILED"
            )
        )

# Health check endpoint
@router.get("/health")
async def health_check():
    """
    Health check endpoint for container orchestration.
    
    Returns:
        System health information
    """
    global kernel_manager, kernel_stats
    
    try:
        system_status = {
            "service": "healthy",
            "kernel_initialized": kernel_manager is not None,
            "kernel_running": kernel_manager.is_kernel_running() if kernel_manager else False,
            "session_stats": kernel_stats,
            "timestamp": time.time()
        }
        
        if kernel_manager:
            system_status["kernel_stats"] = kernel_manager.get_stats()
            system_status["active_executions"] = len(kernel_manager.get_active_executions())
        
        return create_success_response("System healthy", system_status)
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return create_error_response(
            f"Health check failed: {str(e)}",
            "HEALTH_CHECK_FAILED"
        )