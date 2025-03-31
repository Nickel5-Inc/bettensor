"""
Unit tests for the EventManager class.
"""

import asyncio
import unittest
from unittest.mock import MagicMock, patch

import bittensor as bt

from bettensor.miner.core.event_manager import EventManager


class TestEventManager(unittest.TestCase):
    """Test cases for the EventManager class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.event_manager = EventManager()
    
    def tearDown(self):
        """Tear down test fixtures."""
        self.event_manager = None
    
    def test_initialization(self):
        """Test event manager initialization."""
        # Check default attributes are initialized
        self.assertIsNotNone(self.event_manager.subscribers)
        
        # Check that stats are initialized
        stats = self.event_manager.get_stats()
        self.assertEqual(stats["events_published"], 0)
        self.assertEqual(stats["events_processed"], 0)
        self.assertIn("average_processing_time", stats)
    
    def test_subscribe_unsubscribe(self):
        """Test subscribing and unsubscribing to events."""
        # Create a mock handler
        handler = MagicMock()
        
        # Subscribe handler to event type
        self.event_manager.subscribe("test_event", handler)
        
        # Check that handler is subscribed
        self.assertIn(handler, self.event_manager.subscribers["test_event"])
        
        # Unsubscribe handler
        self.event_manager.unsubscribe("test_event", handler)
        
        # Check that handler is unsubscribed
        self.assertNotIn(handler, self.event_manager.subscribers["test_event"])
    
    def test_double_subscribe(self):
        """Test that subscribing the same handler twice doesn't add it twice."""
        # Create a mock handler
        handler = MagicMock()
        
        # Subscribe handler to event type twice
        self.event_manager.subscribe("test_event", handler)
        self.event_manager.subscribe("test_event", handler)
        
        # Check that handler is only subscribed once
        self.assertEqual(len(self.event_manager.subscribers["test_event"]), 1)
    
    def test_unsubscribe_nonexistent(self):
        """Test unsubscribing a handler that isn't subscribed."""
        # Create a mock handler
        handler = MagicMock()
        
        # Unsubscribe handler that isn't subscribed
        self.event_manager.unsubscribe("test_event", handler)
        
        # Should not raise an error
        
    @patch("bittensor.logging.error")
    async def test_publish_no_subscribers(self, mock_logging):
        """Test publishing an event with no subscribers."""
        # Publish event with no subscribers
        await self.event_manager.publish("test_event", {"test": "data"})
        
        # Check that stats were updated
        stats = self.event_manager.get_stats()
        self.assertEqual(stats["events_published"], 1)
        self.assertEqual(stats["events_by_type"]["test_event"], 1)
    
    async def test_publish_with_subscribers(self):
        """Test publishing an event with subscribers."""
        # Create a mock handler
        handler = MagicMock()
        
        # Make the handler an async function
        async def async_handler(event_type, data):
            handler(event_type, data)
        
        # Subscribe handler to event type
        self.event_manager.subscribe("test_event", async_handler)
        
        # Publish event
        await self.event_manager.publish("test_event", {"test": "data"})
        
        # Check that handler was called
        handler.assert_called_once_with("test_event", {"test": "data"})
        
        # Check that stats were updated
        stats = self.event_manager.get_stats()
        self.assertEqual(stats["events_published"], 1)
        self.assertEqual(stats["events_processed"], 1)
    
    @patch("bittensor.logging.error")
    async def test_handler_exception(self, mock_logging):
        """Test that exceptions in handlers are caught and logged."""
        # Create a handler that raises an exception
        async def error_handler(event_type, data):
            raise ValueError("Test error")
        
        # Subscribe handler to event type
        self.event_manager.subscribe("test_event", error_handler)
        
        # Publish event
        await self.event_manager.publish("test_event", {"test": "data"})
        
        # Check that exception was caught and logged
        mock_logging.assert_called_once()
        self.assertIn("Error in event handler", mock_logging.call_args[0][0])
    
    async def test_multiple_handlers(self):
        """Test publishing an event with multiple subscribers."""
        # Create mock handlers
        handler1 = MagicMock()
        handler2 = MagicMock()
        
        # Make the handlers async functions
        async def async_handler1(event_type, data):
            handler1(event_type, data)
        
        async def async_handler2(event_type, data):
            handler2(event_type, data)
        
        # Subscribe handlers to event type
        self.event_manager.subscribe("test_event", async_handler1)
        self.event_manager.subscribe("test_event", async_handler2)
        
        # Publish event
        await self.event_manager.publish("test_event", {"test": "data"})
        
        # Check that both handlers were called
        handler1.assert_called_once_with("test_event", {"test": "data"})
        handler2.assert_called_once_with("test_event", {"test": "data"})
    
    async def test_stats_tracking(self):
        """Test that stats are tracked correctly."""
        # Create a mock handler
        handler = MagicMock()
        
        # Make the handler an async function
        async def async_handler(event_type, data):
            handler(event_type, data)
            await asyncio.sleep(0.1)  # Add a small delay to test execution time tracking
        
        # Subscribe handler to event type
        self.event_manager.subscribe("test_event", async_handler)
        
        # Publish event multiple times
        for i in range(5):
            await self.event_manager.publish("test_event", {"test": "data"})
        
        # Check that stats were updated
        stats = self.event_manager.get_stats()
        self.assertEqual(stats["events_published"], 5)
        self.assertEqual(stats["events_processed"], 5)
        self.assertEqual(stats["events_by_type"]["test_event"], 5)
        self.assertTrue(stats["average_processing_time"] > 0)
        
        # Check execution time tracking
        self.assertTrue(len(stats["average_execution_times"]) > 0)
        self.assertTrue(stats["average_execution_times"]["test_event"] > 0)
    
    async def test_sync_handler(self):
        """Test that synchronous handlers work correctly."""
        # Create a synchronous handler
        handler = MagicMock()
        
        # Subscribe handler to event type
        self.event_manager.subscribe("test_event", handler)
        
        # Publish event
        await self.event_manager.publish("test_event", {"test": "data"})
        
        # Check that handler was called
        handler.assert_called_once_with("test_event", {"test": "data"})
    
    @patch("bittensor.logging.debug")
    async def test_publish_logs(self, mock_logging):
        """Test that publishing an event logs appropriately."""
        # Create a mock handler
        handler = MagicMock()
        
        # Subscribe handler to event type
        self.event_manager.subscribe("test_event", handler)
        
        # Publish event
        await self.event_manager.publish("test_event", {"test": "data"})
        
        # Check that event was logged
        mock_logging.assert_any_call("Publishing event test_event to 1 subscribers")


# Run the tests
if __name__ == "__main__":
    unittest.main() 