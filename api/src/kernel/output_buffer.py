# output_buffer.py
import logging
from typing import Dict, AsyncGenerator
from kernel.execution_tracker import ExecutionTracker, ExecutionState

# Setup logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OutputBufferManager:
    def __init__(self, execution_tracker: ExecutionTracker):
        """Initialize OutputBufferManager with a reference to ExecutionTracker.
        
        Args:
            execution_tracker (ExecutionTracker): Reference to manage buffers and states.
        """
        self.execution_tracker = execution_tracker

    def process_message(self, msg: dict, msg_id: str) -> str:
        """Process individual message and return output text."""
        if not isinstance(msg, dict) or 'header' not in msg:
            return ""
        
        # Check if message belongs to our execution
        parent_header = msg.get('parent_header', {})
        if parent_header.get('msg_id') != msg_id:
            return ""
        
        msg_type = msg['header']['msg_type']
        content = msg.get('content', {})
        
        if msg_type == 'status':
            execution_state = content.get('execution_state', '')
            if execution_state == 'idle':
                self.execution_tracker._update_execution_state(msg_id, ExecutionState.IDLE)
            elif execution_state == 'error':
                self.execution_tracker._update_execution_state(msg_id, ExecutionState.ERROR)
            return ""
        
        elif msg_type == 'stream':
            return content.get('text', '')
        
        elif msg_type in ('execute_result', 'display_data'):
            data = content.get('data', {})
            return data.get('text/plain', '')
        
        elif msg_type == 'error':
            error_name = content.get('ename', 'Error')
            error_value = content.get('evalue', '')
            traceback = content.get('traceback', [])
            
            self.execution_tracker._update_execution_state(msg_id, ExecutionState.ERROR)
            
            error_text = f"\n{error_name}: {error_value}\n"
            if traceback:
                error_text += '\n'.join(traceback)
            return error_text
        
        return ""

    async def process_stream_message(self, msg: dict, msg_id: str) -> AsyncGenerator[str, None]:
        """Process a single stream message and yield output if available."""
        # Update activity timestamp using ExecutionTracker
        if msg_id in self.execution_tracker.executions:
            current_state = self.execution_tracker.executions[msg_id].state
            self.execution_tracker._update_execution_state(msg_id, current_state)
        else:
            self.execution_tracker._update_execution_state(msg_id, ExecutionState.ERROR)

        # Process status messages
        if msg["header"]["msg_type"] == "status":
            state = msg["content"].get("execution_state")
            if state == "idle":
                logger.info(f"Execution finished for {msg_id}")
                self.execution_tracker._update_execution_state(msg_id, ExecutionState.IDLE)
                return
            elif state == "error":
                self.execution_tracker._update_execution_state(msg_id, ExecutionState.ERROR)

        # Process output
        output_text = self.process_message(msg, msg_id)
        if output_text:
            with self.execution_tracker.execution_lock:
                buffer = self.execution_tracker.output_buffers.get(msg_id)
                if buffer:
                    if buffer.append(output_text):
                        yield output_text
                    else:
                        yield f"\n[Output buffer full. Stopping stream.]\n"

    def cleanup_oversized_buffers(self):
        """Clean up buffers that are too large."""
        with self.execution_tracker.execution_lock:
            oversized = []
            for msg_id, buffer in self.execution_tracker.output_buffers.items():
                if buffer.size() > buffer.max_size:
                    oversized.append(msg_id)
            for msg_id in oversized:
                logger.warning(f"Cleaning up oversized buffer for {msg_id}")
                self.execution_tracker._remove_execution_unsafe(msg_id)
