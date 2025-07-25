# src/internal/router.py
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from fastapi.responses import StreamingResponse, JSONResponse
from internal.kernel_wrapper import KernelWrapper
from internal.models import CodeRequest, CodeCompleteRequest, PackageInstallRequest
from typing import Optional, Dict, Any, Union, List
import logging
import time
import asyncio
import threading 

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
restart_lock = threading.Lock()

# Helper functions
def get_kernel_wrapper() -> KernelWrapper:
    """Checks if kernel_wrapper is initialized and alive."""
    global kernel_wrapper
    if kernel_wrapper is None:
        raise HTTPException(
            status_code=503, 
            detail="Kernel not started. Please start the kernel first."
        )
    if not kernel_wrapper.is_kernel_alive():
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

async def background_kernel_cleanup(kernel: KernelWrapper = None, force=False):
    """Enhanced background cleanup"""
    if not kernel:
        logger.warning("No kernel provided for cleanup")
        return False

    try:
        await kernel.cleanup(force=force)
        return True
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        return False

@router.post("/kernel/start")
async def start_kernel(kernel_name: str = 'python3'):
    global kernel_wrapper, kernel_stats
    
    if kernel_wrapper is not None:
        if kernel_wrapper.is_kernel_alive():
            return create_success_response(
                "Kernel is already running",
                {"stats": kernel_wrapper.get_stats()}
            )
        else:
            try:
                await kernel_wrapper.cleanup()
            except Exception as e:
                logger.error(f"Cleanup of stale kernel failed: {e}")
            kernel_wrapper = None

    try:
        kernel_wrapper = KernelWrapper(kernel_name=kernel_name)
        await asyncio.to_thread(kernel_wrapper.kernel_manager.start_kernel)
        
        if not kernel_wrapper.is_kernel_alive():
            raise RuntimeError("Kernel failed to start")

        kernel_stats.update({
            "started_at": time.time(),
            "total_executions": 0,
            "failed_executions": 0,
            "restarts": 0
        })
        
        return create_success_response(
            f"Kernel {kernel_name} started successfully",
            {
                "kernel_name": kernel_name,
                "status": "running",
                "pid": kernel_wrapper.kernel_pid
            }
        )
            
    except Exception as e:
        logger.error(f"Failed to start kernel: {e}")
        if kernel_wrapper is not None:
            try:
                await kernel_wrapper.cleanup()
            except Exception as cleanup_error:
                logger.error(f"Cleanup failed: {cleanup_error}")
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
        if not request.code or not request.code.strip():
            raise HTTPException(
                status_code=400,
                detail=create_error_response("Code cannot be empty", "INVALID_CODE")
            )
        
        response = await kernel.execute_code(request.code, timeout=timeout)
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
    """Streaming package installation endpoint"""
    logger.info(f"Starting package installation for {request.packages}")
    
    # Convert single package to list if needed
    packages = [request.packages] if isinstance(request.packages, str) else request.packages

    # Validate package list
    if not packages or not all(isinstance(pkg, str) and pkg.strip() for pkg in packages):
        logger.error("Invalid package list provided")
        raise HTTPException(
            status_code=400,
            detail=create_error_response("Invalid package list", "INVALID_PACKAGE_LIST")
        )

    # Schedule cleanup
    background_tasks.add_task(background_kernel_cleanup, kernel)
    
    async def generate_stream():
        """Generator function for streaming output"""
        success_count = 0
        failed_count = 0
        
        try:
            for pkg in packages:
                install_cmd = f"!pip install {pkg} --upgrade" if request.upgrade else f"!pip install {pkg}"
                yield f"\nInstalling {pkg}...\n"
                
                # Get the streaming response
                response = await kernel.execute_code(install_cmd, timeout=request.timeout)
                
                # Handle both StreamingResponse and direct string responses
                if isinstance(response, StreamingResponse):
                    try:
                        async for chunk in response.body_iterator:
                            # Handle both bytes and string chunks
                            if isinstance(chunk, bytes):
                                chunk_str = chunk.decode('utf-8')
                            else:
                                chunk_str = str(chunk)
                            
                            yield chunk_str
                            
                            # Track installation status
                            if "Successfully installed" in chunk_str:
                                success_count += 1
                            elif "ERROR:" in chunk_str or "Failed" in chunk_str:
                                failed_count += 1
                    except Exception as e:
                        logger.error(f"Error streaming package {pkg}: {e}")
                        failed_count += 1
                        yield f"\n[ERROR] Failed to install {pkg}: {str(e)}\n"
                else:
                    # Handle non-streaming response
                    response_str = str(response)
                    yield response_str
                    if "Successfully installed" in response_str:
                        success_count += 1
                    elif "ERROR:" in response_str or "Failed" in response_str:
                        failed_count += 1
            
            yield f"\nInstallation complete. Success: {success_count}, Failed: {failed_count}\n"
            
        except Exception as e:
            logger.error(f"Streaming installation failed: {e}")
            yield f"\n[ERROR] Installation failed: {str(e)}\n"
            # Don't raise HTTPException here since we're in a streaming response
            # The error will be visible in the stream

    return StreamingResponse(
        generate_stream(),
        media_type="text/plain",
        headers={
            "X-Package-Install": "in-progress",
            "X-Packages": ",".join(packages)
        }
    )

@router.post("/kernel/restart")
async def restart_kernel(
    background_tasks: BackgroundTasks,
    kernel: KernelWrapper = Depends(get_kernel_wrapper)
):
    """Triggers a non-blocking kernel restart"""
    if not restart_lock.acquire(blocking=False):
        raise HTTPException(
            status_code=409,
            detail=create_error_response("Kernel restart already in progress.", "RESTART_IN_PROGRESS")
        )
    
    try:
        background_tasks.add_task(kernel.perform_restart, lock=restart_lock)
        return JSONResponse(
            status_code=202,
            content=create_success_response("Kernel restart has been initiated.")
        )
    except Exception as e:
        restart_lock.release()
        logger.error(f"Failed to initiate kernel restart: {e}")
        raise HTTPException(
            status_code=500,
            detail=create_error_response(f"Failed to initiate restart: {str(e)}", "RESTART_INITIATION_FAILED")
        )

@router.post("/kernel/shutdown")
async def shutdown_kernel(background_tasks: BackgroundTasks):
    """Shutdown the kernel"""
    global kernel_wrapper
    
    if kernel_wrapper is None:
        return create_success_response("No active kernel to shutdown")
    
    try:
        background_tasks.add_task(async_shutdown_sequence, kernel_wrapper)
        kernel_wrapper = None
        return create_success_response("Shutdown initiated")
        
    except Exception as e:
        logger.error(f"Shutdown initiation failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=create_error_response(f"Shutdown failed: {str(e)}", "SHUTDOWN_FAILED")
        )

async def async_shutdown_sequence(wrapper: KernelWrapper):
    """Orderly shutdown sequence"""
    try:
        await asyncio.wait_for(
            asyncio.to_thread(wrapper.kernel_manager.shutdown_kernel),
            timeout=30
        )
        await background_kernel_cleanup(wrapper)
    except asyncio.TimeoutError:
        logger.warning("Graceful shutdown timed out, forcing cleanup")
        await background_kernel_cleanup(wrapper, force=True)
    except Exception as e:
        logger.error(f"Shutdown error: {e}")
        await background_kernel_cleanup(wrapper, force=True)

@router.get("/health")
async def health_check():
    """Health check endpoint"""
    global kernel_wrapper, kernel_stats
    
    try:
        is_alive = kernel_wrapper.is_kernel_alive() if kernel_wrapper else False
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