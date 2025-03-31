"""
Event management system based on the pub-sub pattern.

This module provides the event-driven architecture for the validator,
allowing components to publish events and subscribe to events from other parts
of the system.
"""

import asyncio
from typing import Dict, List, Callable, Any, Coroutine, Set, Optional
import time
import traceback
import bittensor as bt

class Event:
    """Represents an event in the system with data payload."""
    
    def __init__(self, 
                event_type: str, 
                data: Any = None, 
                source: str = None,
                timestamp: float = None):
        self.event_type = event_type
        self.data = data
        self.source = source
        self.timestamp = timestamp or time.time()
        
    def __str__(self):
        return f"Event({self.event_type}, source={self.source}, timestamp={self.timestamp})"


class EventManager:
    """
    Manages the event-driven communication between system components.
    
    This class implements a pub-sub (publisher-subscriber) pattern where:
    - Components can publish events to the system
    - Components can subscribe to specific event types
    - Events are processed asynchronously
    - Event handling errors are isolated and logged
    
    Events are processed in the order they are received.
    """
    
    def __init__(self):
        """Initialize the event manager."""
        self.subscribers: Dict[str, List[Callable[[Event], Coroutine]]] = {}
        self.running: bool = False
        self.event_queue: asyncio.Queue = asyncio.Queue()
        self.event_processor_task: Optional[asyncio.Task] = None
        self.processed_events: Dict[str, int] = {}  # Counts by event type
        self.processing_errors: Dict[str, int] = {}  # Error counts by event type
        self.recent_errors: List[str] = []  # Last 10 errors
        self.MAX_RECENT_ERRORS = 10
        
    async def start(self):
        """Start the event manager and event processing task."""
        if self.running:
            return
            
        self.running = True
        self.event_processor_task = asyncio.create_task(self._event_processor())
        bt.logging.info("EventManager started")
        
    async def stop(self):
        """Stop the event manager and cleanup resources."""
        if not self.running:
            return
            
        self.running = False
        if self.event_processor_task:
            # Give it a moment to finish gracefully
            await asyncio.sleep(0.1)
            if not self.event_processor_task.done():
                self.event_processor_task.cancel()
                try:
                    await self.event_processor_task
                except asyncio.CancelledError:
                    pass
                
        bt.logging.info("EventManager stopped")
        
    def subscribe(self, 
                event_type: str, 
                callback: Callable[[Event], Coroutine]) -> None:
        """
        Subscribe to events of a specific type.
        
        Args:
            event_type: Type of event to subscribe to
            callback: Async function that will be called when event occurs
        """
        if event_type not in self.subscribers:
            self.subscribers[event_type] = []
            
        if callback not in self.subscribers[event_type]:
            self.subscribers[event_type].append(callback)
            bt.logging.debug(f"Added subscriber for {event_type} events: {callback.__name__}")
            
    def unsubscribe(self, 
                   event_type: str, 
                   callback: Callable[[Event], Coroutine]) -> bool:
        """
        Unsubscribe from events of a specific type.
        
        Args:
            event_type: Type of event to unsubscribe from
            callback: Callback function to remove
            
        Returns:
            True if successfully unsubscribed, False otherwise
        """
        if event_type in self.subscribers and callback in self.subscribers[event_type]:
            self.subscribers[event_type].remove(callback)
            bt.logging.debug(f"Removed subscriber for {event_type} events: {callback.__name__}")
            return True
        return False
        
    async def publish(self, 
                    event_type: str, 
                    data: Any = None, 
                    source: str = None) -> None:
        """
        Publish an event to all subscribers.
        
        Args:
            event_type: Type of event to publish
            data: Data payload for the event
            source: Source component that published the event
        """
        event = Event(event_type, data, source)
        await self.event_queue.put(event)
        bt.logging.debug(f"Published event: {event}")
        
    async def _event_processor(self) -> None:
        """Background task that processes events from the queue."""
        while self.running:
            try:
                # Get event from queue with timeout
                try:
                    event = await asyncio.wait_for(self.event_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                    
                # Process event
                await self._process_event(event)
                self.event_queue.task_done()
                
            except asyncio.CancelledError:
                bt.logging.debug("Event processor task cancelled")
                break
            except Exception as e:
                bt.logging.error(f"Unexpected error in event processor: {str(e)}")
                bt.logging.debug(traceback.format_exc())
                await asyncio.sleep(1)  # Prevent tight loop on persistent errors
                
    async def _process_event(self, event: Event) -> None:
        """Process a single event by calling all subscribers."""
        # Update event count
        self.processed_events[event.event_type] = self.processed_events.get(event.event_type, 0) + 1
        
        if event.event_type not in self.subscribers:
            return  # No subscribers for this event type
            
        # Call all subscribers
        tasks = []
        for callback in self.subscribers[event.event_type]:
            task = asyncio.create_task(self._safe_call_subscriber(callback, event))
            tasks.append(task)
            
        if tasks:
            # Wait for all subscriber callbacks to complete
            await asyncio.gather(*tasks, return_exceptions=True)
            
    async def _safe_call_subscriber(self, 
                                  callback: Callable[[Event], Coroutine], 
                                  event: Event) -> None:
        """Call a subscriber callback with error handling."""
        try:
            await callback(event)
        except Exception as e:
            # Update error counts
            self.processing_errors[event.event_type] = self.processing_errors.get(event.event_type, 0) + 1
            
            # Store recent error details
            error_details = f"{time.time()}: Error in {callback.__name__} handling {event.event_type}: {str(e)}"
            self.recent_errors.append(error_details)
            if len(self.recent_errors) > self.MAX_RECENT_ERRORS:
                self.recent_errors.pop(0)  # Remove oldest error
                
            bt.logging.error(f"Error in event handler {callback.__name__} for {event.event_type}: {str(e)}")
            bt.logging.debug(traceback.format_exc())
            
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about event processing."""
        return {
            "processed_events": self.processed_events.copy(),
            "processing_errors": self.processing_errors.copy(),
            "recent_errors": self.recent_errors.copy(),
            "subscriber_count": {event_type: len(callbacks) for event_type, callbacks in self.subscribers.items()},
            "queue_size": self.event_queue.qsize() if hasattr(self.event_queue, "qsize") else -1
        }
        
    def get_event_types(self) -> Set[str]:
        """Get all event types that have subscribers."""
        return set(self.subscribers.keys()) 