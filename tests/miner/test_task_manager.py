"""
Unit tests for the TaskManager class.
"""

import asyncio
import time
import unittest
from unittest.mock import MagicMock, patch

import bittensor as bt

from bettensor.miner.core.task_manager import TaskManager
from bettensor.miner.core.event_manager import EventManager


class TestTaskManager(unittest.TestCase):
    """Test cases for the TaskManager class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.event_manager = EventManager()
        self.task_manager = TaskManager(self.event_manager)
    
    def tearDown(self):
        """Tear down test fixtures."""
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(self.task_manager.stop())
        else:
            loop.run_until_complete(self.task_manager.stop())
        self.task_manager = None
        self.event_manager = None
    
    def test_initialization(self):
        """Test task manager initialization."""
        # Check default attributes are initialized
        self.assertFalse(self.task_manager.is_running)
        self.assertEqual(len(self.task_manager.tasks), 0)
        
        # Check that stats are initialized
        stats = self.task_manager.get_stats()
        self.assertEqual(stats["task_count"], 0)
        self.assertEqual(stats["running"], False)
    
    def test_add_task(self):
        """Test adding a task."""
        # Create a mock task function
        async def task_func():
            pass
        
        # Add task
        task = self.task_manager.add_task("test_task", task_func, 1.0)
        
        # Check that task was added
        self.assertEqual(len(self.task_manager.tasks), 1)
        self.assertEqual(self.task_manager.tasks["test_task"], task)
        
        # Check task attributes
        self.assertEqual(task.name, "test_task")
        self.assertEqual(task.interval, 1.0)
        self.assertEqual(task.jitter, 0.1)  # Default jitter
        self.assertEqual(task.max_retries, 3)  # Default max_retries
    
    def test_add_task_with_custom_attributes(self):
        """Test adding a task with custom attributes."""
        # Create a mock task function
        async def task_func():
            pass
        
        # Create success and failure callbacks
        async def success_callback(task_name, execution_time):
            pass
        
        async def failure_callback(task_name, error):
            pass
        
        # Add task with custom attributes
        task = self.task_manager.add_task(
            name="test_task",
            coro_func=task_func,
            interval=5.0,
            jitter=0.2,
            max_retries=5,
            backoff_factor=2.0,
            success_callback=success_callback,
            failure_callback=failure_callback
        )
        
        # Check task custom attributes
        self.assertEqual(task.interval, 5.0)
        self.assertEqual(task.jitter, 0.2)
        self.assertEqual(task.max_retries, 5)
        self.assertEqual(task.backoff_factor, 2.0)
        self.assertEqual(task.success_callback, success_callback)
        self.assertEqual(task.failure_callback, failure_callback)
    
    def test_remove_task(self):
        """Test removing a task."""
        # Create a mock task function
        async def task_func():
            pass
        
        # Add task
        self.task_manager.add_task("test_task", task_func, 1.0)
        
        # Check that task was added
        self.assertEqual(len(self.task_manager.tasks), 1)
        
        # Remove task
        self.task_manager.remove_task("test_task")
        
        # Check that task was removed
        self.assertEqual(len(self.task_manager.tasks), 0)
    
    def test_remove_nonexistent_task(self):
        """Test removing a task that doesn't exist."""
        # Remove task that doesn't exist
        self.task_manager.remove_task("nonexistent_task")
        
        # Should not raise an error
    
    @patch("bittensor.logging.info")
    async def test_start_stop(self, mock_logging):
        """Test starting and stopping the task manager."""
        # Start task manager
        await self.task_manager.start()
        
        # Check that task manager is running
        self.assertTrue(self.task_manager.is_running)
        
        # Stop task manager
        await self.task_manager.stop()
        
        # Check that task manager is stopped
        self.assertFalse(self.task_manager.is_running)
        
        # Check that logging was called
        mock_logging.assert_any_call("TaskManager started")
        mock_logging.assert_any_call("TaskManager stopped")
    
    @patch("bittensor.logging.warning")
    async def test_start_already_running(self, mock_logging):
        """Test starting the task manager when it's already running."""
        # Start task manager
        await self.task_manager.start()
        
        # Start task manager again
        await self.task_manager.start()
        
        # Check that warning was logged
        mock_logging.assert_called_once_with("TaskManager is already running")
        
        # Stop task manager
        await self.task_manager.stop()
    
    @patch("bittensor.logging.warning")
    async def test_stop_not_running(self, mock_logging):
        """Test stopping the task manager when it's not running."""
        # Stop task manager without starting it
        await self.task_manager.stop()
        
        # Check that warning was logged
        mock_logging.assert_called_once_with("TaskManager is not running")
    
    async def test_execute_task(self):
        """Test executing a task."""
        # Create a mock task function
        mock_func = MagicMock()
        async def task_func():
            mock_func()
            
        # Add task
        task = self.task_manager.add_task("test_task", task_func, 1.0)
        
        # Execute task
        await self.task_manager._execute_task(task)
        
        # Check that task function was called
        mock_func.assert_called_once()
        
        # Check that task was updated
        self.assertEqual(task.executed_count, 1)
        self.assertEqual(task.success_count, 1)
        self.assertEqual(task.failure_count, 0)
        self.assertGreater(task.last_execution_time, 0)
        self.assertGreater(task.last_success_time, 0)
    
    @patch("bittensor.logging.error")
    async def test_execute_task_error(self, mock_logging):
        """Test executing a task that raises an error."""
        # Create a task function that raises an error
        async def task_func():
            raise ValueError("Test error")
            
        # Add task
        task = self.task_manager.add_task("test_task", task_func, 1.0)
        
        # Execute task
        await self.task_manager._execute_task(task)
        
        # Check that error was logged
        mock_logging.assert_called_once()
        self.assertIn("Error executing task test_task", mock_logging.call_args[0][0])
        
        # Check that task was updated
        self.assertEqual(task.executed_count, 1)
        self.assertEqual(task.success_count, 0)
        self.assertEqual(task.failure_count, 1)
        self.assertGreater(task.last_execution_time, 0)
        self.assertEqual(task.last_success_time, 0)
        self.assertGreater(task.last_failure_time, 0)
    
    async def test_task_with_success_callback(self):
        """Test a task with a success callback."""
        # Create a mock task function and success callback
        mock_func = MagicMock()
        mock_callback = MagicMock()
        
        async def task_func():
            mock_func()
            
        async def success_callback(task_name, execution_time):
            mock_callback(task_name, execution_time)
            
        # Add task with success callback
        task = self.task_manager.add_task(
            name="test_task",
            coro_func=task_func,
            interval=1.0,
            success_callback=success_callback
        )
        
        # Execute task
        await self.task_manager._execute_task(task)
        
        # Check that task function and success callback were called
        mock_func.assert_called_once()
        mock_callback.assert_called_once()
        self.assertEqual(mock_callback.call_args[0][0], "test_task")
        self.assertGreater(mock_callback.call_args[0][1], 0)
    
    async def test_task_with_failure_callback(self):
        """Test a task with a failure callback."""
        # Create a task function that raises an error and a failure callback
        mock_callback = MagicMock()
        
        async def task_func():
            raise ValueError("Test error")
            
        async def failure_callback(task_name, error):
            mock_callback(task_name, error)
            
        # Add task with failure callback
        task = self.task_manager.add_task(
            name="test_task",
            coro_func=task_func,
            interval=1.0,
            failure_callback=failure_callback
        )
        
        # Execute task
        await self.task_manager._execute_task(task)
        
        # Check that failure callback was called
        mock_callback.assert_called_once()
        self.assertEqual(mock_callback.call_args[0][0], "test_task")
        self.assertIsInstance(mock_callback.call_args[0][1], ValueError)
        self.assertEqual(str(mock_callback.call_args[0][1]), "Test error")
    
    @patch("asyncio.sleep")
    async def test_task_loop(self, mock_sleep):
        """Test the task loop execution."""
        # Mock sleep to speed up test
        mock_sleep.return_value = None
        
        # Create a mock task function
        mock_func = MagicMock()
        async def task_func():
            mock_func()
            
        # Add task
        self.task_manager.add_task("test_task", task_func, 1.0)
        
        # Start task manager
        await self.task_manager.start()
        
        # Run task loop for a short time
        await asyncio.sleep(0.1)
        
        # Stop task manager
        await self.task_manager.stop()
        
        # Check that task function was called at least once
        self.assertGreater(mock_func.call_count, 0)
    
    async def test_get_task(self):
        """Test getting a task by name."""
        # Create a mock task function
        async def task_func():
            pass
            
        # Add task
        task = self.task_manager.add_task("test_task", task_func, 1.0)
        
        # Get task
        retrieved_task = self.task_manager.get_task("test_task")
        
        # Check that task was retrieved
        self.assertEqual(retrieved_task, task)
    
    async def test_get_nonexistent_task(self):
        """Test getting a task that doesn't exist."""
        # Get task that doesn't exist
        task = self.task_manager.get_task("nonexistent_task")
        
        # Check that None was returned
        self.assertIsNone(task)
    
    async def test_get_stats(self):
        """Test getting task manager statistics."""
        # Create mock task functions
        async def task_func1():
            pass
            
        async def task_func2():
            pass
            
        # Add tasks
        self.task_manager.add_task("task1", task_func1, 1.0)
        self.task_manager.add_task("task2", task_func2, 2.0)
        
        # Get stats
        stats = self.task_manager.get_stats()
        
        # Check stats
        self.assertEqual(stats["task_count"], 2)
        self.assertEqual(stats["running"], False)
        self.assertIn("tasks", stats)
        self.assertEqual(len(stats["tasks"]), 2)
        self.assertIn("task1", stats["tasks"])
        self.assertIn("task2", stats["tasks"])


# Run the tests
if __name__ == "__main__":
    unittest.main() 