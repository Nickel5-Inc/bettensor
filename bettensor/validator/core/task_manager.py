"""
Task management for async operations in the validator.
"""

import asyncio
import time
import traceback
from typing import Dict, Callable, Coroutine, Any, Optional
import bittensor as bt

class TaskManager:
    """
    Manages async tasks with proper error handling and lifecycle management.
    
    This class provides utilities for:
    - Creating periodic tasks with proper cleanup
    - Running one-time tasks with timeout protection
    - Gracefully shutting down all managed tasks
    
    All tasks are properly tracked for cleanup during shutdown.
    """
    
    def __init__(self):
        """Initialize the task manager."""
        self.tasks: Dict[str, asyncio.Task] = {}
        self.running: bool = False
        self.stats: Dict[str, Dict[str, Any]] = {}  # Statistics for tasks
        
    async def start(self):
        """Start the task manager."""
        self.running = True
        bt.logging.info("TaskManager started")
        
    async def stop(self):
        """
        Stop all managed tasks gracefully.
        
        This will:
        1. Signal tasks to stop by setting running to False
        2. Cancel all tasks that don't exit on their own
        3. Wait for all tasks to complete or be cancelled
        """
        if not self.running:
            return
            
        bt.logging.info(f"Stopping {len(self.tasks)} managed tasks...")
        self.running = False
        
        # First, wait a short time for tasks to exit gracefully
        await asyncio.sleep(1)
        
        # Then cancel any remaining tasks
        pending_tasks = []
        for name, task in self.tasks.items():
            if not task.done():
                bt.logging.debug(f"Cancelling task: {name}")
                task.cancel()
                pending_tasks.append(task)
        
        # Wait for all cancellations to complete
        if pending_tasks:
            await asyncio.gather(*pending_tasks, return_exceptions=True)
            
        self.tasks = {}
        bt.logging.info("All tasks stopped")
        
    def create_periodic_task(self, 
                           func: Callable[..., Coroutine],
                           interval_seconds: float,
                           name: Optional[str] = None,
                           initial_delay: float = 0,
                           jitter: float = 0) -> asyncio.Task:
        """
        Create a task that runs periodically with proper cleanup.
        
        Args:
            func: Async function to run periodically
            interval_seconds: Seconds between runs
            name: Task name (defaults to function name)
            initial_delay: Seconds to delay first execution
            jitter: Random jitter added to interval (0-1, as percentage of interval)
            
        Returns:
            Task object representing the periodic execution
        """
        task_name = name or func.__name__
        
        async def wrapped_task():
            # Initial delay
            if initial_delay > 0:
                await asyncio.sleep(initial_delay)
                
            # Track statistics
            self.stats[task_name] = {
                "runs": 0,
                "errors": 0,
                "last_run": None,
                "last_error": None,
                "avg_duration": 0
            }
            
            total_duration = 0
            
            while self.running:
                start_time = time.time()
                try:
                    await func()
                    
                    # Update stats
                    self.stats[task_name]["runs"] += 1
                    self.stats[task_name]["last_run"] = time.time()
                    
                    duration = time.time() - start_time
                    total_duration += duration
                    self.stats[task_name]["avg_duration"] = total_duration / self.stats[task_name]["runs"]
                    
                except asyncio.CancelledError:
                    bt.logging.debug(f"Task {task_name} was cancelled")
                    break
                except Exception as e:
                    self.stats[task_name]["errors"] += 1
                    self.stats[task_name]["last_error"] = str(e)
                    
                    bt.logging.error(f"Error in task {task_name}: {str(e)}")
                    bt.logging.debug(traceback.format_exc())
                    
                # Calculate next run interval with jitter
                if jitter > 0:
                    jitter_amount = interval_seconds * jitter
                    sleep_time = interval_seconds + (asyncio.get_event_loop().time() % jitter_amount) - (jitter_amount / 2)
                else:
                    sleep_time = interval_seconds
                    
                await asyncio.sleep(max(0.1, sleep_time))  # Ensure minimum sleep
                    
        task = asyncio.create_task(wrapped_task())
        self.tasks[task_name] = task
        bt.logging.debug(f"Created periodic task: {task_name} (interval: {interval_seconds}s)")
        return task
        
    async def run_with_timeout(self, 
                             func: Callable[..., Coroutine], 
                             timeout: float, 
                             *args, 
                             **kwargs) -> Any:
        """
        Run a coroutine with a timeout.
        
        Args:
            func: Async function to run
            timeout: Seconds to wait before timing out
            *args, **kwargs: Arguments to pass to the function
            
        Returns:
            Result of the coroutine, or None if timed out
            
        Raises:
            asyncio.TimeoutError: If the task times out
        """
        func_name = getattr(func, '__name__', str(func))
        
        try:
            return await asyncio.wait_for(func(*args, **kwargs), timeout)
        except asyncio.TimeoutError:
            bt.logging.warning(f"Task {func_name} timed out after {timeout}s")
            raise
        except Exception as e:
            bt.logging.error(f"Error in task {func_name}: {str(e)}")
            bt.logging.debug(traceback.format_exc())
            raise
            
    def get_task_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all managed tasks."""
        return self.stats
            
    def cancel_task(self, name: str) -> bool:
        """
        Cancel a specific task.
        
        Args:
            name: Name of the task to cancel
            
        Returns:
            True if task was found and cancelled, False otherwise
        """
        if name in self.tasks:
            if not self.tasks[name].done():
                self.tasks[name].cancel()
            del self.tasks[name]
            return True
        return False 