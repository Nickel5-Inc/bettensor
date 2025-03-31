"""
Task Manager for the Bettensor Miner.

This module implements a task scheduling system that manages periodic tasks
and ensures they are executed reliably with proper error handling.
"""

import asyncio
import random
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import bittensor as bt


class Task:
    """A scheduled task with metadata about its execution."""
    
    def __init__(
        self,
        name: str,
        coro_func: Callable,
        interval: float,
        jitter: float = 0.1,
        initial_delay: float = 0.0,
        retry_on_error: bool = True,
        retry_delay: float = 1.0,
        max_retries: int = 3,
        callback_on_success: Optional[Callable] = None,
        callback_on_failure: Optional[Callable] = None,
    ):
        """
        Initialize a new task.
        
        Args:
            name: Name of the task for logging
            coro_func: Async function to execute
            interval: Seconds between executions
            jitter: Random fraction to add to interval (prevents tasks from bunching)
            initial_delay: Seconds to wait before first execution
            retry_on_error: Whether to retry on error
            retry_delay: Seconds to wait before retry
            max_retries: Maximum number of retries
            callback_on_success: Function to call after successful execution
            callback_on_failure: Function to call after failed execution
        """
        self.id = str(uuid.uuid4())
        self.name = name
        self.coro_func = coro_func
        self.interval = interval
        self.jitter = jitter
        self.initial_delay = initial_delay
        self.retry_on_error = retry_on_error
        self.retry_delay = retry_delay
        self.max_retries = max_retries
        self.callback_on_success = callback_on_success
        self.callback_on_failure = callback_on_failure
        
        # Runtime state
        self.last_execution_time: Optional[float] = None
        self.next_execution_time: float = time.time() + initial_delay
        self.running: bool = False
        self.failures: int = 0
        self.consecutive_failures: int = 0
        self.successes: int = 0
        self.retries: int = 0
        self.execution_times: List[float] = []
        self.max_execution_time: float = 0.0
        self.min_execution_time: Optional[float] = None
        self.total_execution_time: float = 0.0
        self.is_scheduled: bool = True
        self.task_object: Optional[asyncio.Task] = None
    
    def calculate_next_execution(self) -> float:
        """
        Calculate the next execution time with jitter.
        
        Returns:
            The next execution time in seconds since epoch
        """
        # Apply jitter to interval
        jitter_amount = self.interval * self.jitter * (2 * random.random() - 1)
        jittered_interval = max(0.1, self.interval + jitter_amount)
        
        # Calculate next execution time
        now = time.time()
        if self.last_execution_time is None:
            # First execution
            next_time = now + self.initial_delay
        else:
            # Schedule from previous execution time to maintain rhythm
            next_time = self.last_execution_time + jittered_interval
            
            # If we've fallen behind, schedule immediately with a small delay
            if next_time < now:
                next_time = now + 0.1
        
        self.next_execution_time = next_time
        return next_time
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about this task.
        
        Returns:
            A dictionary of task statistics
        """
        avg_execution_time = 0.0
        if self.execution_times:
            avg_execution_time = self.total_execution_time / len(self.execution_times)
        
        return {
            "id": self.id,
            "name": self.name,
            "interval": self.interval,
            "jitter": self.jitter,
            "running": self.running,
            "failures": self.failures,
            "consecutive_failures": self.consecutive_failures,
            "successes": self.successes,
            "retries": self.retries,
            "total_executions": self.failures + self.successes,
            "avg_execution_time": avg_execution_time,
            "max_execution_time": self.max_execution_time,
            "min_execution_time": self.min_execution_time if self.min_execution_time is not None else 0.0,
            "last_execution_time": self.last_execution_time,
            "next_execution_time": self.next_execution_time,
            "time_until_next_execution": self.next_execution_time - time.time() if self.next_execution_time else None,
            "is_scheduled": self.is_scheduled,
        }


class TaskManager:
    """
    Manages periodic tasks for the miner.
    
    This class schedules and executes tasks at specified intervals,
    with features like jitter, retries, and proper cleanup.
    """
    
    def __init__(self, event_manager=None):
        """
        Initialize the task manager.
        
        Args:
            event_manager: Optional event manager for publishing task events
        """
        self.tasks: Dict[str, Task] = {}
        self.running: bool = False
        self.main_task: Optional[asyncio.Task] = None
        self.event_manager = event_manager
        
        # For stats tracking
        self.start_time: Optional[float] = None
        self.total_executions: int = 0
        self.failed_executions: int = 0
        
        bt.logging.info("TaskManager initialized")
    
    def add_task(
        self,
        name: str,
        coro_func: Callable,
        interval: float,
        jitter: float = 0.1,
        initial_delay: float = 0.0,
        retry_on_error: bool = True,
        retry_delay: float = 1.0,
        max_retries: int = 3,
        callback_on_success: Optional[Callable] = None,
        callback_on_failure: Optional[Callable] = None,
    ) -> str:
        """
        Add a new task to be scheduled.
        
        Args:
            name: Name of the task for logging
            coro_func: Async function to execute
            interval: Seconds between executions
            jitter: Random fraction to add to interval
            initial_delay: Seconds to wait before first execution
            retry_on_error: Whether to retry on error
            retry_delay: Seconds to wait before retry
            max_retries: Maximum number of retries
            callback_on_success: Function to call after successful execution
            callback_on_failure: Function to call after failed execution
            
        Returns:
            The ID of the created task
        """
        task = Task(
            name=name,
            coro_func=coro_func,
            interval=interval,
            jitter=jitter,
            initial_delay=initial_delay,
            retry_on_error=retry_on_error,
            retry_delay=retry_delay,
            max_retries=max_retries,
            callback_on_success=callback_on_success,
            callback_on_failure=callback_on_failure,
        )
        
        self.tasks[task.id] = task
        bt.logging.info(f"Added task '{name}' with ID {task.id} and interval {interval}s")
        
        # If the task manager is already running, schedule this task
        if self.running:
            task.calculate_next_execution()
        
        return task.id
    
    def remove_task(self, task_id: str) -> bool:
        """
        Remove a task by ID.
        
        Args:
            task_id: The ID of the task to remove
            
        Returns:
            True if the task was removed, False if not found
        """
        if task_id in self.tasks:
            task = self.tasks[task_id]
            
            # Cancel the task if it's currently running
            if task.task_object and not task.task_object.done():
                task.task_object.cancel()
            
            # Remove the task
            del self.tasks[task_id]
            bt.logging.info(f"Removed task '{task.name}' with ID {task_id}")
            return True
        
        return False
    
    def get_task(self, task_id: str) -> Optional[Task]:
        """
        Get a task by ID.
        
        Args:
            task_id: The ID of the task to get
            
        Returns:
            The task if found, None otherwise
        """
        return self.tasks.get(task_id)
    
    def get_task_by_name(self, name: str) -> Optional[Task]:
        """
        Get a task by name.
        
        Args:
            name: The name of the task to get
            
        Returns:
            The first task with the given name if found, None otherwise
        """
        for task in self.tasks.values():
            if task.name == name:
                return task
        
        return None
    
    async def start(self) -> None:
        """Start the task manager and begin processing tasks."""
        if self.running:
            bt.logging.warning("TaskManager is already running")
            return
        
        self.running = True
        self.start_time = time.time()
        
        # Start the main task loop
        self.main_task = asyncio.create_task(self._task_loop())
        bt.logging.info("TaskManager started")
    
    async def stop(self) -> None:
        """Stop the task manager and cancel all tasks."""
        if not self.running:
            bt.logging.warning("TaskManager is not running")
            return
        
        self.running = False
        
        # Cancel all running tasks
        for task in self.tasks.values():
            if task.task_object and not task.task_object.done():
                task.task_object.cancel()
        
        # Cancel the main task loop
        if self.main_task and not self.main_task.done():
            self.main_task.cancel()
            
            try:
                await self.main_task
            except asyncio.CancelledError:
                pass
        
        bt.logging.info("TaskManager stopped")
    
    async def _task_loop(self) -> None:
        """Main task loop that schedules and executes tasks."""
        try:
            while self.running:
                now = time.time()
                
                # Find the next task to execute
                next_task = None
                next_execution_time = float('inf')
                
                for task in self.tasks.values():
                    if task.is_scheduled and not task.running and task.next_execution_time < next_execution_time:
                        next_task = task
                        next_execution_time = task.next_execution_time
                
                # If there's a task to execute now
                if next_task and next_execution_time <= now:
                    # Execute the task
                    await self._execute_task(next_task)
                    
                    # Calculate the next execution time for this task
                    next_task.calculate_next_execution()
                
                # Sleep for a short interval, or until the next task
                sleep_time = 0.1  # Default sleep time
                
                if next_task and now < next_task.next_execution_time:
                    sleep_time = min(next_task.next_execution_time - now, 1.0)
                
                await asyncio.sleep(sleep_time)
        
        except asyncio.CancelledError:
            bt.logging.info("Task loop cancelled")
            raise
        
        except Exception as e:
            bt.logging.error(f"Error in task loop: {str(e)}")
            if self.running:
                # Restart the task loop
                bt.logging.info("Restarting task loop")
                self.main_task = asyncio.create_task(self._task_loop())
    
    async def _execute_task(self, task: Task) -> None:
        """
        Execute a task and handle any errors.
        
        Args:
            task: The task to execute
        """
        # Mark the task as running
        task.running = True
        
        # Create a task object for this execution
        task.task_object = asyncio.create_task(self._run_task(task))
        
        # Wait for the task to complete
        try:
            await task.task_object
        except asyncio.CancelledError:
            bt.logging.info(f"Task '{task.name}' was cancelled")
        except Exception as e:
            bt.logging.error(f"Error executing task '{task.name}': {str(e)}")
    
    async def _run_task(self, task: Task) -> None:
        """
        Run a task and update its statistics.
        
        Args:
            task: The task to run
        """
        start_time = time.time()
        task.last_execution_time = start_time
        
        try:
            # Publish task started event
            if self.event_manager:
                await self.event_manager.publish("task_started", {
                    "task_id": task.id,
                    "task_name": task.name,
                    "start_time": start_time
                })
            
            # Execute the task
            await task.coro_func()
            
            # Update task statistics
            execution_time = time.time() - start_time
            task.execution_times.append(execution_time)
            
            # Keep only the last 100 execution times
            if len(task.execution_times) > 100:
                task.execution_times.pop(0)
            
            task.total_execution_time += execution_time
            task.max_execution_time = max(task.max_execution_time, execution_time)
            
            if task.min_execution_time is None:
                task.min_execution_time = execution_time
            else:
                task.min_execution_time = min(task.min_execution_time, execution_time)
            
            task.successes += 1
            task.consecutive_failures = 0
            task.retries = 0
            self.total_executions += 1
            
            # Call success callback
            if task.callback_on_success:
                task.callback_on_success(task)
            
            # Publish task completed event
            if self.event_manager:
                await self.event_manager.publish("task_completed", {
                    "task_id": task.id,
                    "task_name": task.name,
                    "execution_time": execution_time,
                    "success": True
                })
            
            bt.logging.debug(f"Task '{task.name}' completed in {execution_time:.4f}s")
        
        except Exception as e:
            execution_time = time.time() - start_time
            task.failures += 1
            task.consecutive_failures += 1
            self.failed_executions += 1
            self.total_executions += 1
            
            bt.logging.error(f"Task '{task.name}' failed: {str(e)}")
            
            # Call failure callback
            if task.callback_on_failure:
                task.callback_on_failure(task, e)
            
            # Publish task failed event
            if self.event_manager:
                await self.event_manager.publish("task_failed", {
                    "task_id": task.id,
                    "task_name": task.name,
                    "execution_time": execution_time,
                    "error": str(e)
                })
            
            # Handle retries
            if task.retry_on_error and task.retries < task.max_retries:
                task.retries += 1
                bt.logging.info(f"Retrying task '{task.name}' ({task.retries}/{task.max_retries}) in {task.retry_delay}s")
                
                # Wait for retry delay
                await asyncio.sleep(task.retry_delay)
                
                # Retry the task
                await self._run_task(task)
                return
        
        finally:
            # Mark the task as not running
            task.running = False
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the task manager.
        
        Returns:
            A dictionary of statistics
        """
        uptime = 0.0
        if self.start_time:
            uptime = time.time() - self.start_time
        
        running_tasks = sum(1 for task in self.tasks.values() if task.running)
        scheduled_tasks = sum(1 for task in self.tasks.values() if task.is_scheduled)
        
        return {
            "running": self.running,
            "uptime": uptime,
            "total_tasks": len(self.tasks),
            "running_tasks": running_tasks,
            "scheduled_tasks": scheduled_tasks,
            "total_executions": self.total_executions,
            "failed_executions": self.failed_executions,
            "success_rate": (self.total_executions - self.failed_executions) / max(1, self.total_executions),
            "tasks": {task_id: task.get_stats() for task_id, task in self.tasks.items()}
        }
    
    def pause_task(self, task_id: str) -> bool:
        """
        Pause a task by ID.
        
        Args:
            task_id: The ID of the task to pause
            
        Returns:
            True if the task was paused, False if not found
        """
        if task_id in self.tasks:
            task = self.tasks[task_id]
            task.is_scheduled = False
            bt.logging.info(f"Paused task '{task.name}' with ID {task_id}")
            return True
        
        return False
    
    def resume_task(self, task_id: str) -> bool:
        """
        Resume a paused task by ID.
        
        Args:
            task_id: The ID of the task to resume
            
        Returns:
            True if the task was resumed, False if not found
        """
        if task_id in self.tasks:
            task = self.tasks[task_id]
            task.is_scheduled = True
            task.calculate_next_execution()  # Recalculate next execution time
            bt.logging.info(f"Resumed task '{task.name}' with ID {task_id}")
            return True
        
        return False 