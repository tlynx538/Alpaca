# src/internal/router.py
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from fastapi.responses import StreamingResponse, JSONResponse
from internal.kernel_wrapper import KernelWrapper
from internal.models import CodeRequest, CodeCompleteRequest, PackageInstallRequest
from typing import Optional, Dict, Any, Union, List
import logging
import time
import asyncio

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()

# Global kernel manager with proper lifecycle management
kernel_wrapper: Optional[KernelWrapper] = None
kernel_stats = {
    "started_at": None,
    "total_executions": 0,
    "failed_executions": 0,
    "restarts": 0
}

# Helper functions
def get_kernel_wrapper() -> KernelWrapper:
    """Dependency to ensure kernel is available"""
    global kernel_wrapper
    if kernel_wrapper is None:
        raise HTTPException(
            status_code=503, 
            detail="Kernel not started. Please start the kernel first."
        )
    if not kernel_wrapper.kernel_manager.is_kernel_alive():
        raise HTTPException(
            status_code=503,
            detail="Kernel is not running. Please restart the kernel."
        )
    return kernel_wrapper

def create_success_response(message: str, data: Optional[Dict[Any, Any]] = None) -> Dict[str, Any]:
    """Standardized success response format"""
    return {
        "status": "success",
        "message": message,
        "timestamp": time.time(),
        **({"data": data} if data else {})
    }

def create_error_response(message: str, error_code: Optional[str] = None) -> Dict[str, Any]:
    """Standardized error response format"""
    return {
        "status": "error",
        "message": message,
        "timestamp": time.time(),
        **({"error_code": error_code} if error_code else {})
    }

async def background_kernel_cleanup():
    """Background task for kernel cleanup"""
    global kernel_wrapper
    if kernel_wrapper:
        try:
            # Clean up kernel resources
            kernel_wrapper.kernel_manager.cleanup()
            
            # Clean up executions
            await kernel_wrapper.code_executor._cleanup_executions()
            
            logger.info("Background cleanup completed successfully")
        except Exception as e:
            logger.error(f"Background cleanup failed: {e}")

# Routes
@router.get("/kernel/start")
async def start_kernel(
    kernel_name: str = 'python3',
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """Start a new kernel instance"""
    global kernel_wrapper, kernel_stats
    
    try:
        # Check if kernel is already running
        if kernel_wrapper is not None and kernel_wrapper.kernel_manager.is_kernel_alive():
            return create_success_response(
                "Kernel is already running",
                {
                    "kernel_name": kernel_name,
                    "stats": kernel_wrapper.get_stats()
                }
            )
        
        # Start new kernel wrapper
        kernel_wrapper = KernelWrapper(kernel_name=kernel_name)
        kernel_stats.update({
            "started_at": time.time(),
            "total_executions": 0,
            "failed_executions": 0,
            "restarts": 0
        })
        
        # Schedule background cleanup
        background_tasks.add_task(background_kernel_cleanup)
        
        return create_success_response(
            f"Kernel {kernel_name} started successfully",
            {
                "kernel_name": kernel_name,
                "started_at": kernel_stats["started_at"],
                "stats": kernel_wrapper.get_stats()
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to start kernel: {e}")
        kernel_wrapper = None
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                f"Failed to start kernel: {str(e)}",
                "KERNEL_START_FAILED"
            )
        )


@router.post("/kernel/execute")
async def execute_code(
    request: CodeRequest,
    timeout: int = 30,
    kernel: KernelWrapper = Depends(get_kernel_wrapper)
):
    """Execute code in the kernel"""
    global kernel_stats
    
    try:
        # Validate request
        if not request.code or not request.code.strip():
            raise HTTPException(
                status_code=400,
                detail=create_error_response("Code cannot be empty", "INVALID_CODE")
            )
        
        response = await kernel.code_executor.execute_code(request.code, timeout=timeout)
        kernel_stats["total_executions"] += 1
        return response
            
    except HTTPException:
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

@router.post("/kernel/install-packages")
async def install_packages(
    request: PackageInstallRequest,
    kernel: KernelWrapper = Depends(get_kernel_wrapper),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """Install packages in the kernel environment"""
    try:
        # Convert single package to list if needed
        packages = [request.packages] if isinstance(request.packages, str) else request.packages
        
        if not packages or not all(isinstance(pkg, str) and pkg.strip() for pkg in packages):
            raise HTTPException(
                status_code=400,
                detail=create_error_response("Invalid package list", "INVALID_PACKAGE_LIST")
            )
        
        # Schedule cleanup
        background_tasks.add_task(background_kernel_cleanup)
        
        # Install packages
        results = kernel.install_packages(
            packages,
            upgrade=request.upgrade,
            timeout=request.timeout
        )
        
        # Calculate success/failure counts
        success_count = sum(1 for res in results.values() if "Success" in res or "Already installed" in res)
        failed_count = len(results) - success_count
        
        return create_success_response(
            f"Package installation completed ({success_count} success, {failed_count} failed)",
            {
                "results": results,
                "summary": {
                    "total": len(results),
                    "success": success_count,
                    "failed": failed_count
                }
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Package installation failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                f"Package installation failed: {str(e)}",
                "PACKAGE_INSTALL_FAILED"
            )
        )

@router.post("/kernel/restart")
async def restart_kernel(
    background_tasks: BackgroundTasks,
    kernel: KernelWrapper = Depends(get_kernel_wrapper)
):
    """Restart the kernel endpoint"""
    global kernel_stats
    
    try:
        # Get stats before restart
        old_stats = kernel.get_stats()
        
        # Trigger synchronous restart
        restart_result = kernel.kernel_manager.restart_kernel()
        
        if restart_result.get('status') != 'success':
            raise RuntimeError(restart_result.get('message', 'Restart failed'))
        
        # Update stats
        kernel_stats["restarts"] += 1
        
        # Schedule cleanup
        background_tasks.add_task(background_kernel_cleanup)
        
        return create_success_response(
            "Kernel restarted successfully",
            {
                "restart_count": kernel_stats["restarts"],
                "previous_uptime": old_stats.get('uptime', 0),
                "stats": kernel.get_stats(),
                "restart_details": restart_result
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

@router.post("/kernel/shutdown")
async def shutdown_kernel():
    """Shutdown the kernel"""
    global kernel_wrapper, kernel_stats
    
    try:
        if kernel_wrapper is None:
            return create_success_response("Kernel was not running", {"was_running": False})
        
        final_stats = kernel_wrapper.get_stats() if kernel_wrapper.kernel_manager.is_kernel_alive() else {}
        uptime = time.time() - kernel_stats["started_at"] if kernel_stats["started_at"] else 0
        
        # Trigger shutdown through KernelWrapper
        kernel_wrapper._shutdown_event.set()
        kernel_wrapper.kernel_manager.shutdown_kernel()
        
        # Reset state
        final_session_stats = {**kernel_stats, "uptime": uptime}
        kernel_wrapper = None
        kernel_stats = {
            "started_at": None,
            "total_executions": 0,
            "failed_executions": 0,
            "restarts": 0
        }
        
        return create_success_response(
            "Kernel shut down successfully",
            {
                "final_stats": final_stats,
                "session_stats": final_session_stats
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to shutdown kernel: {e}")
        kernel_wrapper = None
        raise HTTPException(
            status_code=500,
            detail=create_error_response(
                f"Failed to shutdown kernel: {str(e)}",
                "SHUTDOWN_FAILED"
            )
        )

@router.get("/health")
async def health_check():
    """Health check endpoint"""
    global kernel_wrapper, kernel_stats
    
    try:
        is_alive = kernel_wrapper.kernel_manager.is_kernel_alive() if kernel_wrapper else False
        return create_success_response(
            "System healthy",
            {
                "kernel_initialized": kernel_wrapper is not None,
                "kernel_running": is_alive,
                "session_stats": kernel_stats,
                **({"wrapper_stats": kernel_wrapper.get_stats()} if kernel_wrapper else {})
            }
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return create_error_response(
            f"Health check failed: {str(e)}",
            "HEALTH_CHECK_FAILED"
        )