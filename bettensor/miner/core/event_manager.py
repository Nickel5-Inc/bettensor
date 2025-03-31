"""
Event Manager for the Bettensor Miner.

This module provides an event system that allows components to communicate
through events without direct dependencies.
"""

import asyncio
import time
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Set

import bittensor as bt


class EventManager:
    """
    Manages the event system for the miner.
    
    Allows components to publish events and subscribe to event types,
    facilitating decoupled communication between different parts of the system.
    """
    
    def __init__(self):
        """Initialize the event manager."""
        # Dictionary mapping event types to lists of handler functions
        self.subscribers = defaultdict(list)
        
        # Event statistics
        self.stats = {
            "events_published": 0,
            "events_processed": 0,
            "events_by_type": defaultdict(int),
            "handler_execution_times": defaultdict(list),
            "average_processing_time": 0.0,
        }
        
        bt.logging.info("EventManager initialized")
    
    def subscribe(self, event_type: str, handler: Callable[[str, Any], None]):
        """
        Subscribe a handler function to an event type.
        
        Args:
            event_type: The type of event to subscribe to
            handler: The function to call when the event occurs
        """
        if handler not in self.subscribers[event_type]:
            self.subscribers[event_type].append(handler)
            bt.logging.debug(f"Handler subscribed to event type: {event_type}")
    
    def unsubscribe(self, event_type: str, handler: Callable[[str, Any], None]):
        """
        Unsubscribe a handler function from an event type.
        
        Args:
            event_type: The type of event to unsubscribe from
            handler: The function to unsubscribe
        """
        if handler in self.subscribers[event_type]:
            self.subscribers[event_type].remove(handler)
            bt.logging.debug(f"Handler unsubscribed from event type: {event_type}")
    
    async def publish(self, event_type: str, data: Any = None):
        """
        Publish an event to all subscribers.
        
        Args:
            event_type: The type of event being published
            data: The data associated with the event
        """
        start_time = time.time()
        handlers = self.subscribers.get(event_type, [])
        
        # Update statistics
        self.stats["events_published"] += 1
        self.stats["events_by_type"][event_type] += 1
        
        if not handlers:
            bt.logging.debug(f"Event published with no subscribers: {event_type}")
            return
        
        bt.logging.debug(f"Publishing event {event_type} to {len(handlers)} subscribers")
        
        # Execute all handlers asynchronously
        tasks = []
        for handler in handlers:
            tasks.append(self._execute_handler(handler, event_type, data))
        
        # Wait for all handlers to complete
        if tasks:
            await asyncio.gather(*tasks)
        
        # Update average processing time
        processing_time = time.time() - start_time
        if self.stats["average_processing_time"] == 0:
            self.stats["average_processing_time"] = processing_time
        else:
            self.stats["average_processing_time"] = (
                0.9 * self.stats["average_processing_time"] + 0.1 * processing_time
            )
    
    async def _execute_handler(self, handler: Callable, event_type: str, data: Any):
        """
        Execute a single event handler and track its performance.
        
        Args:
            handler: The handler function to execute
            event_type: The type of event
            data: The event data
        """
        handler_start_time = time.time()
        try:
            # Call the handler with the event type and data
            if asyncio.iscoroutinefunction(handler):
                await handler(event_type, data)
            else:
                handler(event_type, data)
            
            # Update statistics
            self.stats["events_processed"] += 1
            
        except Exception as e:
            bt.logging.error(f"Error in event handler for {event_type}: {str(e)}")
        
        # Track handler execution time
        execution_time = time.time() - handler_start_time
        self.stats["handler_execution_times"][event_type].append(execution_time)
        
        # Keep only the last 100 execution times
        if len(self.stats["handler_execution_times"][event_type]) > 100:
            self.stats["handler_execution_times"][event_type] = self.stats["handler_execution_times"][event_type][-100:]
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the event manager.
        
        Returns:
            A dictionary of statistics
        """
        # Calculate average execution time for each event type
        average_times = {}
        for event_type, times in self.stats["handler_execution_times"].items():
            if times:
                average_times[event_type] = sum(times) / len(times)
            else:
                average_times[event_type] = 0
        
        # Create a copy of the stats with the defaultdict converted to a regular dict
        stats = {
            "events_published": self.stats["events_published"],
            "events_processed": self.stats["events_processed"],
            "events_by_type": dict(self.stats["events_by_type"]),
            "average_execution_times": average_times,
            "average_processing_time": self.stats["average_processing_time"],
            "subscriber_count": {event: len(handlers) for event, handlers in self.subscribers.items()},
        }
        
        return stats 