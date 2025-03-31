import functools
import os
import subprocess
import sys
import time
import copy
import traceback
import signal
import torch
import bittensor as bt
import websocket
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
import asyncio
from bettensor.protocol import GameData
from bettensor.validator.bettensor_validator import BettensorValidator
from bettensor.validator.core.event_driven_validator import EventDrivenValidator
from bettensor.validator.scoring.watchdog import Watchdog
from bettensor.validator.io.auto_updater import check_and_install_dependencies, perform_update
from functools import partial
from typing import Optional, Any
import async_timeout
from bettensor.validator.utils.state_sync import StateSync
import signal

# Constants for timeouts (in seconds)
UPDATE_TIMEOUT = 300  # 5 minutes
GAME_DATA_TIMEOUT = 1200  # 20 minutes (for deep updates)
METAGRAPH_TIMEOUT = 120  # 2 minutes
QUERY_TIMEOUT = 600  # 10 minutes
WEBSITE_TIMEOUT = 60  # 1 minute
SCORING_TIMEOUT = 300  # 5 minutes
WEIGHTS_TIMEOUT = 300  # 5 minutes
DATABASE_MIN_TIMEOUT = 30  # Minimum timeout (30 seconds)
DATABASE_MAX_TIMEOUT = 120  # Maximum timeout (2 minutes)
DATABASE_TIMEOUT_BACKOFF = 1.5  # Multiply timeout by this factor on failure
DATABASE_TIMEOUT_REDUCTION = 0.8  # Multiply by this on success
DATABASE_TIMEOUT = 30  # 30 seconds timeout for database operations

# At the top of the file, define the timeouts
TASK_TIMEOUTS = {
    'sync_metagraph': 300,  # 5 minutes
    'query_and_process_axons': 600,  # 10 minutes
    'send_data_to_website': 300,  # 5 minutes
    'scoring_run': 600,  # 10 minutes
    'set_weights': 300,  # 5 minutes
    'perform_update': 300,  # 5 minutes
    'check_hotkeys': 60,  # 1 minute
    'state_sync': 300,  # 5 minutes
}

# At the top with other globals
_validator = None  # Global validator instance

def cancellable_task(func):
    """
    Decorator that wraps a coroutine in a cancellable task.
    Cancels the task if any error occurs.
    """
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        task = asyncio.create_task(func(*args, **kwargs))
        try:
            return await task
        except Exception as e:
            if not task.done():
                bt.logging.warning(f"Cancelling task {func.__name__} due to error")
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    bt.logging.debug(f"Task {func.__name__} cancelled successfully")
                except Exception as cancel_error:
                    bt.logging.error(f"Error while cancelling task {func.__name__}: {str(cancel_error)}")
            bt.logging.error(f"Error in {func.__name__}: {str(e)}")
            bt.logging.debug(traceback.format_exc())
            raise  # Re-raise the exception to maintain error handling flow
    return wrapper



def time_task(task_name):
    """Decorator to time tasks and log their duration."""
    def decorator(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                # Await the coroutine if it's async
                if asyncio.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)
                return result
            except Exception as e:
                duration = time.time() - start_time
                bt.logging.error(f"{task_name} failed after {duration:.2f} seconds with error: {str(e)}")
                raise
            finally:
                duration = time.time() - start_time
                bt.logging.debug(f"{task_name} completed in {duration:.2f} seconds")
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                duration = time.time() - start_time
                bt.logging.error(f"{task_name} failed after {duration:.2f} seconds with error: {str(e)}")
                raise
            finally:
                duration = time.time() - start_time
                bt.logging.debug(f"{task_name} completed in {duration:.2f} seconds")
        
        # Return appropriate wrapper based on whether the function is async
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper
    
    return decorator

async def log_status(validator):
    while True:
        current_time = datetime.now(timezone.utc)
        current_block = validator.subtensor.block
        blocks_until_query_axons = max(0, validator.query_axons_interval - (current_block - validator.last_queried_block))
        blocks_until_send_data = max(0, validator.send_data_to_website_interval - (current_block - validator.last_sent_data_to_website))
        blocks_until_scoring = max(0, validator.scoring_interval - (current_block - validator.last_scoring_block))
        blocks_until_set_weights = max(0, validator.set_weights_interval - (current_block - validator.last_set_weights_block))
        blocks_until_state_sync = max(0, validator.state_sync_interval - (current_block - validator.last_state_sync))

        status_message = (
            "\n"
            "================================ VALIDATOR STATUS ================================\n"
            f"Current time: {current_time}\n"
            f"Scoring System, Current Day: {validator.scoring_system.current_day}\n"
            f"Current block: {current_block}\n"
            f"Last updated block: {validator.last_updated_block}\n"
            f"Blocks until next query_and_process_axons: {blocks_until_query_axons}\n"
            f"Blocks until send_data_to_website: {blocks_until_send_data}\n"
            f"Blocks until scoring_run: {blocks_until_scoring}\n"
            f"Blocks until set_weights: {blocks_until_set_weights}\n"
            f"Blocks until state sync: {blocks_until_state_sync}\n"
            "================================================================================\n"
        )
        
        debug_message = (
            f"Scoring System, Current Day: {validator.scoring_system.current_day}\n"
            f"Scoring System, Current Day Tiers: {validator.scoring_system.tiers[:, validator.scoring_system.current_day]}\n"
            f"Scoring System, Current Day Tiers Length: {len(validator.scoring_system.tiers[:, validator.scoring_system.current_day])}\n"
            f"Scoring System, Current Day Scores: {validator.scoring_system.composite_scores[:, validator.scoring_system.current_day, 0]}\n"
            f"Scoring System, Amount Wagered Last 5 Days: {validator.scoring_system.amount_wagered[:, validator.scoring_system.current_day]}\n"
        )

        bt.logging.info(status_message)
        # bt.logging.debug(debug_message)
        await asyncio.sleep(30)

async def game_data_update_loop(validator):
    """Background thread function for updating game data"""
    # Initialize to trigger deep update on first run
    last_deep_update = 0
    DEEP_UPDATE_INTERVAL = 21600  # 6 hours in seconds
    first_run = True
    
    # Dynamic timeout settings
    base_timeout = 300  # 5 minutes base timeout
    max_timeout = 1800  # 30 minutes max timeout
    timeout_backoff = 1.5  # Multiply timeout by this factor on failure
    current_timeout = base_timeout
    
    while True:
        try:
            current_time = datetime.now(timezone.utc)
            current_time_secs = int(time.time())
            
            # Try to acquire the lock with a timeout
            try:
                # Shield the critical game update operation from cancellation
                async with async_timeout.timeout(30):  # 30 second timeout for lock acquisition
                    async with validator.operation_lock:
                        # Deep update on startup or every 6 hours
                        if first_run or (current_time_secs - last_deep_update >= DEEP_UPDATE_INTERVAL):
                            bt.logging.info("Performing deep game data update...")
                            deep_update_time = current_time - timedelta(days=7)
                            async with async_timeout.timeout(max(current_timeout * 2, max_timeout)):  # Double timeout for deep updates
                                await asyncio.shield(update_game_data(validator, deep_update_time))
                            last_deep_update = current_time_secs
                            validator.last_api_call = deep_update_time
                            await validator.save_state()
                            first_run = False
                            bt.logging.info("Deep update completed")
                            await asyncio.sleep(5)  # Brief pause after deep update
                        
                        # Regular update
                        async with async_timeout.timeout(current_timeout):
                            await asyncio.shield(update_game_data(validator, current_time))
                            validator.last_api_call = current_time
                            await validator.save_state()
                            
                        # Successful update - reduce timeout back towards base
                        current_timeout = max(base_timeout, current_timeout / timeout_backoff)
                        bt.logging.debug(f"Game update successful, current timeout: {current_timeout}s")
                        
            except asyncio.TimeoutError:
                # Increase timeout on failure, up to max_timeout
                current_timeout = min(current_timeout * timeout_backoff, max_timeout)
                bt.logging.warning(f"Game data update timed out, increased timeout to {current_timeout}s")
                await asyncio.sleep(30)
                continue
            
            # Successful update - wait before next attempt
            await asyncio.sleep(300)  # Wait 5 minutes between updates
            
        except Exception as e:
            bt.logging.error(f"Error in game data update loop: {str(e)}")
            bt.logging.error(traceback.format_exc())
            # Increase timeout on error
            current_timeout = min(current_timeout * timeout_backoff, max_timeout)
            bt.logging.warning(f"Increased timeout to {current_timeout}s after error")
            await asyncio.sleep(30)  # Shorter sleep on error

async def update_game_data_with_lock(validator, current_time):
    """Wrapper to handle the async lock for update_game_data"""
    try:
        async with async_timeout.timeout(GAME_DATA_TIMEOUT):
            async with validator.operation_lock:
                await asyncio.to_thread(update_game_data, validator, current_time)
    except asyncio.TimeoutError:
        bt.logging.error("Game data update timed out while waiting for lock")
        
async def state_sync_task(validator):
        """
        Periodically checks and performs state synchronization (for primary validator nodes)
        """
        while True:
            try:
                current_block = validator.subtensor.block
                blocks_since_sync = current_block - validator.last_state_sync
                if validator.is_primary and blocks_since_sync >= validator.state_sync_interval:
                    bt.logging.info("Initiating state synchronization...")
                    async with async_timeout.timeout(300):  # Adjust timeout as needed
                        if await validator.state_sync.push_state():
                            validator.last_state_sync = current_block
                            bt.logging.info("State synchronization successful.")
                        else:
                            bt.logging.error("State synchronization failed.")
            except asyncio.TimeoutError:
                bt.logging.error("State synchronization timed out.")
            except Exception as e:
                bt.logging.error(f"State synchronization error: {e}")
            finally:
                await asyncio.sleep(60)

async def run(validator: BettensorValidator):
    """Main async run loop for the validator"""
    # Load environment variables
    load_dotenv()
    await initialize(validator)
    validator.watchdog = Watchdog(validator=validator, timeout=1200)  # 20 minutes timeout

    # Initialize background tasks
    validator.background_tasks = {
        'game_data': asyncio.create_task(game_data_update_loop(validator)),
        'status_log': asyncio.create_task(log_status(validator))  # Using existing method
    }

    last_state_push = 0
    last_state_check = 0
    STATE_PUSH_INTERVAL = 3600  # 1 hour
    STATE_CHECK_INTERVAL = 300  # 5 minutes
    MAX_PUSH_DURATION = 120  # 2 minutes
    
    # Track WebSocket health
    last_websocket_health_check = time.time()
    WEBSOCKET_HEALTH_CHECK_INTERVAL = 300  # 5 minutes

    try:
        # Ensure neuron is initialized
        if not validator.is_initialized:
            await validator.initialize_neuron()

        while True:
            current_time = datetime.now(timezone.utc)
            current_block = validator.subtensor.block
            current_time_secs = int(time.time())
            bt.logging.info(f"Current block: {current_block}")

            if (current_time_secs - last_state_check) >= STATE_CHECK_INTERVAL:
                async with validator.operation_lock:
                    if await validator.state_sync.should_pull_state():
                        bt.logging.info("State divergence detected, pulling latest state")
                        if await validator.state_sync.pull_state():
                            bt.logging.info("Successfully pulled latest state")
                            await validator.db_manager.initialize(force=True)
                            continue
                        else:
                            bt.logging.error("Failed to pull latest state")
                last_state_check = current_time_secs

            # Create tasks with timeouts
            tasks = []
            
            # Sync metagraph
            tasks.append((
                sync_metagraph_with_retry(validator),
                TASK_TIMEOUTS['sync_metagraph']
            ))
            
            await asyncio.sleep(2)

            # Check hotkeys
            tasks.append((
                validator.check_hotkeys(),
                TASK_TIMEOUTS['check_hotkeys']
            ))

            await asyncio.sleep(2)

            # Perform update (if needed)
            tasks.append((
                perform_update(validator),
                TASK_TIMEOUTS['perform_update']
            ))

            await asyncio.sleep(2)
            
            # Check WebSocket health and connection status if enabled
            if (time.time() - last_websocket_health_check >= WEBSOCKET_HEALTH_CHECK_INTERVAL and 
                hasattr(validator, 'websocket_manager') and 
                validator.websocket_manager is not None):
                    
                # Log WebSocket status
                active_connections = validator.websocket_manager.get_active_connection_count()
                total_miners = len(validator.metagraph.hotkeys) if validator.metagraph else 0
                
                if total_miners > 0:
                    connection_percentage = (active_connections / total_miners) * 100
                    bt.logging.info(f"WebSocket status: {active_connections}/{total_miners} miners connected ({connection_percentage:.1f}%)")
                    
                    # If connection percentage is too low, check if WebSocket server is healthy
                    if connection_percentage < 25:  # Arbitrary threshold
                        bt.logging.warning(f"Low WebSocket connection rate ({connection_percentage:.1f}%). Checking WebSocket server health...")
                        try:
                            is_healthy = validator.websocket_manager.check_health()
                            if not is_healthy:
                                bt.logging.error("WebSocket server is unhealthy. Attempting restart...")
                                await validator.websocket_manager.restart()
                        except Exception as e:
                            bt.logging.error(f"Error checking WebSocket health: {str(e)}")
                
                last_websocket_health_check = time.time()

            # Query and process axons - only if needed as fallback when WebSocket connections are insufficient
            if (current_block - validator.last_queried_block) > validator.query_axons_interval:
                # Call this method but it will internally decide whether to actually run or skip
                tasks.append((
                    query_and_process_axons(validator),
                    TASK_TIMEOUTS['query_and_process_axons']
                ))

            await asyncio.sleep(2)

            # Send data to website
            if (current_block - validator.last_sent_data_to_website) > validator.send_data_to_website_interval:
                tasks.append((
                    send_data_to_website_server(validator),
                    TASK_TIMEOUTS['send_data_to_website']
                ))

            await asyncio.sleep(2)

            # Recalculate scores
            if (current_block - validator.last_scoring_block) > validator.scoring_interval:
                tasks.append((
                    scoring_run(validator, current_time),
                    TASK_TIMEOUTS['scoring_run']
                ))

            await asyncio.sleep(2)

            # Set weights
            if (current_block - validator.last_set_weights_block) > validator.set_weights_interval:
                tasks.append((
                    set_weights(validator, validator.scores),
                    TASK_TIMEOUTS['set_weights']
                ))

            # Create a wrapper for each task that includes timeout and error handling
            async def execute_task_safely(task_coroutine, timeout_seconds):
                try:
                    async with async_timeout.timeout(timeout_seconds):
                        result = await task_coroutine
                        validator.watchdog.reset()  # Reset after successful task
                        return result
                except asyncio.TimeoutError:
                    bt.logging.error(f"Task {task_coroutine} timed out after {timeout_seconds} seconds")
                    return None
                except Exception as e:
                    bt.logging.error(f"Task {task_coroutine} failed with error: {str(e)}")
                    return None

            # Execute regular tasks first
            if tasks:
                # Split tasks into sequential and parallel groups
                sequential_tasks = []
                parallel_tasks = []
                
                for task_coroutine, timeout_seconds in tasks:
                    # Check if this is one of our sequential tasks
                    if any(task_name in str(task_coroutine) for task_name in ['query_and_process_axons', 'scoring_run', 'set_weights']):
                        sequential_tasks.append((task_coroutine, timeout_seconds))
                    else:
                        parallel_tasks.append((task_coroutine, timeout_seconds))

                # Execute parallel tasks first
                if parallel_tasks:
                    results = await asyncio.gather(*[
                        execute_task_safely(task_coroutine, timeout_seconds)
                        for task_coroutine, timeout_seconds in parallel_tasks
                    ], return_exceptions=True)

                # Then execute sequential tasks one by one in order
                for task_coroutine, timeout_seconds in sequential_tasks:
                    await execute_task_safely(task_coroutine, timeout_seconds)

            # State push with timeout protection
            current_block = validator.subtensor.block
            blocks_since_sync = current_block - validator.last_state_sync
            if validator.is_primary and blocks_since_sync >= validator.state_sync_interval:
                bt.logging.info("Initiating state synchronization...")
                try:
                    async with async_timeout.timeout(300):
                        if await validator.state_sync.push_state():
                            validator.last_state_sync = current_block
                            bt.logging.info("State synchronization successful.")
                        else:
                            bt.logging.error("State synchronization failed.")
                except asyncio.TimeoutError:
                    bt.logging.error("State synchronization timed out.")
                except Exception as e:
                    bt.logging.error(f"State synchronization error: {e}")


    except Exception as e:
        bt.logging.error(f"Error in main loop: {e}")
        bt.logging.error(traceback.format_exc())
    except KeyboardInterrupt:
        bt.logging.info("Keyboard interrupt received. Shutting down gracefully...")
    finally:
        # Cancel all background tasks
        for task in validator.background_tasks.values():
            task.cancel()
        await asyncio.gather(*validator.background_tasks.values(), return_exceptions=True)

@cancellable_task
async def run_with_timeout(func, timeout: int, *args, **kwargs) -> Optional[Any]:
    """
    Enhanced run_with_timeout that ensures proper task cleanup
    """
    try:
        # Convert sync function to async if needed
        if not asyncio.iscoroutinefunction(func):
            async_func = partial(asyncio.to_thread, func)
        else:
            async_func = func
        
        async with async_timeout.timeout(timeout):
            task = asyncio.create_task(async_func(*args, **kwargs))
            try:
                return await task
            except asyncio.CancelledError:
                bt.logging.warning(f"{func.__name__} was cancelled")
                raise
            finally:
                # Ensure task is properly cleaned up
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    
    except asyncio.TimeoutError:
        func_name = getattr(func, '__name__', str(func))
        bt.logging.error(f"{func_name} timed out after {timeout} seconds")
        return None
    except Exception as e:
        func_name = getattr(func, '__name__', str(func))
        bt.logging.error(f"Error in {func_name}: {str(e)}")
        bt.logging.error(traceback.format_exc())
        return None

async def initialize(validator):
    load_dotenv()
    validator.is_primary = os.environ.get("VALIDATOR_IS_PRIMARY") == "True"
    should_pull_state = os.environ.get("VALIDATOR_PULL_STATE", "True").lower() == "true"
    bt.logging.info(f"Validator is primary: {validator.is_primary}")
    bt.logging.info(f"Should pull state: {should_pull_state}")

    # Add state sync initialization
    branch = subprocess.check_output(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"]
    ).decode().strip()
    
    # Set branch to main if it is none or not main or test
    if branch is None or branch not in ["main", "test"]:
        bt.logging.warning(f"Invalid branch. Setting to main by default for data sync")
        branch = "main"

    validator.state_sync = StateSync(
        state_dir="./bettensor/validator/state",
        db_manager=validator.db_manager,
        validator=validator
    )
    
    # Pull latest state before starting if configured to do so
    if should_pull_state:
        bt.logging.info("Pulling latest state from Azure blob storage...")
        async with async_timeout.timeout(300):  # 5 minute timeout
            if await validator.state_sync.pull_state():
                bt.logging.info("Successfully pulled latest state")
                # Check if this is a new validator or one that has gone out of sync
                is_new_validator = not os.path.exists(validator.db_manager.database_path) or \
                                 os.path.getsize(validator.db_manager.database_path) < 1024 * 1024  # Less than 1MB
                await validator.db_manager.initialize(force=True)
                if is_new_validator:
                    bt.logging.info("New validator detected, rebuilding historical scores...")
                    await validator.scoring_system.rebuild_historical_scores()
            else:
                bt.logging.warning("Failed to pull latest state, continuing with local state")
    else:
        bt.logging.info("Skipping state pull due to VALIDATOR_PULL_STATE configuration")
    
    validator.serve_axon()
    validator.initialize_connection()

    if not validator.last_updated_block:
        bt.logging.info("Updating last updated block; will set weights this iteration")
        validator.last_updated_block = validator.subtensor.block - 301
        validator.last_queried_block = validator.subtensor.block - 11
        validator.last_sent_data_to_website = validator.subtensor.block - 16
        validator.last_scoring_block = validator.subtensor.block - 51
        validator.last_set_weights_block = validator.subtensor.block - 301
    validator.last_api_call = datetime.now(timezone.utc) - timedelta(days=1)
    
    # Define default intervals if they don't exist
    if not hasattr(validator, 'update_game_data_interval'):
        validator.update_game_data_interval = 10  # Default value, adjust as needed

    if not hasattr(validator, 'query_axons_interval'):
        validator.query_axons_interval = 40  # Default value, adjust as needed

    if not hasattr(validator, 'send_data_to_website_interval'):
        validator.send_data_to_website_interval = 15  # Default value, adjust as needed

    if not hasattr(validator, 'scoring_interval'):
        validator.scoring_interval = 60  # Default value, adjust as needed

    if not hasattr(validator, 'set_weights_interval'):
        validator.set_weights_interval = 300  # Default value, adjust as needed

    # Define last operation block numbers if they don't exist
    if not hasattr(validator, 'last_queried_block'):
        validator.last_queried_block = validator.subtensor.block - 10

    if not hasattr(validator, 'last_sent_data_to_website'):
        validator.last_sent_data_to_website = validator.subtensor.block - 15

    if not hasattr(validator, 'last_scoring_block'):
        validator.last_scoring_block = validator.subtensor.block - 50

    if not hasattr(validator, 'last_set_weights_block'):
        validator.last_set_weights_block = validator.subtensor.block - 300
    validator.operation_lock = asyncio.Lock()

    # Define state sync intervals
    if not hasattr(validator, 'state_sync_interval'):
        validator.state_sync_interval = 200  # 15 minutes
    if not hasattr(validator, 'last_state_sync'):
        validator.last_state_sync = validator.subtensor.block - 200


@cancellable_task
async def log_status_with_watchdog(validator):
        while True:
            try:
                await log_status(validator)
                validator.watchdog.reset()
            except Exception as e:
                bt.logging.error(f"Error in status log: {str(e)}")
            await asyncio.sleep(30)



@time_task("update_game_data")
@cancellable_task
async def update_game_data(validator, current_time, deep_query=False):
    """
    Calls SportsData to update game data in the database with dynamic timeouts
    """
    bt.logging.info("\n--------------------------------Updating game data--------------------------------\n")
    
    try:
        async with async_timeout.timeout(GAME_DATA_TIMEOUT):
            current_timeout = DATABASE_MIN_TIMEOUT
            max_retries = 3
            
            for attempt in range(max_retries):
                try:
                    bt.logging.debug(f"Database operation attempt {attempt + 1} with timeout {current_timeout}s")
                    async with async_timeout.timeout(current_timeout):
                        if not await validator.db_manager.ensure_connection():
                            bt.logging.error("Failed to establish database connection")
                            return
                        
                        api_call_time = current_time
                        query_time = current_time - timedelta(days=2) if deep_query else validator.last_api_call
                        
                        bt.logging.info(f"{'Deep query' if deep_query else 'Regular query'} from {query_time} to {current_time}")
                        all_games = await validator.sports_data.fetch_and_update_game_data(query_time)
                        
                        # Success - reduce timeout for next time
                        current_timeout = max(DATABASE_MIN_TIMEOUT, current_timeout * DATABASE_TIMEOUT_REDUCTION)
                        break
                        
                except asyncio.TimeoutError:
                    bt.logging.warning(f"Database operation timed out after {current_timeout}s")
                    current_timeout = min(DATABASE_MAX_TIMEOUT, current_timeout * DATABASE_TIMEOUT_BACKOFF)
                    if attempt == max_retries - 1:
                        raise
                    continue
            
            # Only update last_api_call for regular queries
            if not deep_query:
                validator.last_api_call = api_call_time
                await validator.save_state()
            
            return all_games
            
    except asyncio.TimeoutError:
        bt.logging.error("Game data update timed out")
        raise
    except ConnectionError as e:
        bt.logging.error(f"Database connection error: {str(e)}")
        raise
    except Exception as e:
        bt.logging.error(f"Error in game data update: {str(e)}")
        raise

# Add a new method to handle database cleanup with timeout
async def cleanup_database_connections(validator):
    """Cleanup database connections with timeout"""
    try:
        async with async_timeout.timeout(DATABASE_TIMEOUT):
            await validator.db_manager.cleanup()
    except asyncio.TimeoutError:
        bt.logging.error("Database cleanup timed out")
    except Exception as e:
        bt.logging.error(f"Error during database cleanup: {str(e)}")

# Modify the signal handler to use the new cleanup method
def signal_handler(signum, frame):
    global _validator
    signal_name = signal.strsignal(signum) if hasattr(signal, 'strsignal') else str(signum)
    bt.logging.info(f"Received signal {signum} ({signal_name}). Shutting down gracefully...")
    
    if _validator:
        try:
            # Check if we can get the running loop
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                # If there's no running loop, create a new one
                bt.logging.warning("No running event loop, creating a new one for cleanup")
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            # Use the loop for cleanup
            if loop.is_running():
                bt.logging.info("Using existing event loop for cleanup")
                
                # Create a future with a timeout
                cleanup_task = asyncio.ensure_future(_validator.cleanup())
                
                # Add a timeout to prevent hanging
                def timeout_cleanup():
                    if not cleanup_task.done():
                        bt.logging.warning("Cleanup timed out after 10 seconds, forcing exit")
                        cleanup_task.cancel()
                
                # Schedule the timeout
                loop.call_later(10, timeout_cleanup)
                
                # Don't use run_until_complete inside a callback as it could cause issues
                # Instead, just schedule the exit
                def finalize_cleanup(_):
                    bt.logging.info("Cleanup completed, preparing to exit")
                    # Give a short delay to allow logs to be written before exit
                    loop.call_later(0.5, sys.exit, 0)
                
                # Add callback for when cleanup is done
                cleanup_task.add_done_callback(finalize_cleanup)
            else:
                bt.logging.info("Running cleanup in new event loop")
                try:
                    # Run cleanup in the new loop
                    loop.run_until_complete(asyncio.wait_for(_validator.cleanup(), timeout=10))
                    bt.logging.info("Cleanup completed successfully")
                except asyncio.TimeoutError:
                    bt.logging.warning("Cleanup timed out after 10 seconds")
                except Exception as e:
                    bt.logging.error(f"Error during cleanup: {e}")
                finally:
                    loop.close()
                    # Exit after cleanup
                    sys.exit(0)
        except Exception as e:
            bt.logging.error(f"Error during {signal_name} shutdown: {e}")
            bt.logging.error(traceback.format_exc())
            # Make sure we exit even if there's an error
            sys.exit(1)
    else:
        bt.logging.info("No validator instance to clean up, exiting directly")
        sys.exit(0)

@time_task("sync_metagraph")
@cancellable_task
async def sync_metagraph_with_retry(validator):
    max_retries = 3
    retry_delay = 60
    for attempt in range(max_retries):
        try:
            validator.metagraph = validator.sync_metagraph()
            bt.logging.info("Metagraph synced successfully.")
            return
        except websocket.WebSocketConnectionClosedException:
            if attempt < max_retries - 1:
                bt.logging.warning(f"WebSocket connection closed. Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
            else:
                raise
        except Exception as e:
            bt.logging.error(f"Error syncing metagraph: {str(e)}")
            bt.logging.error(f"Traceback: {traceback.format_exc()}")
            raise

@time_task("filter_and_update_axons")
@cancellable_task
async def filter_and_update_axons(validator):
    all_axons = validator.metagraph.axons
    bt.logging.trace(f"All axons: {all_axons}")

    if validator.scores is None:
        bt.logging.warning("Scores were None. Reinitializing...")
        validator.init_default_scores()

    if validator.scores is None:
        bt.logging.error("Failed to initialize scores. Exiting.")
        return None, None, None, None

    num_uids = len(validator.metagraph.uids.tolist())
    current_scores_len = len(validator.scores)

    if num_uids > current_scores_len:
        bt.logging.info(f"Discovered new Axons, current scores: {validator.scores}")
        validator.scores = torch.cat(
            (
                validator.scores,
                torch.zeros(
                    (num_uids - current_scores_len),
                    dtype=torch.float32,
                ),
            )
        )
        bt.logging.info(f"Updated scores, new scores: {validator.scores}")

    # Run get_uids_to_query in a thread since it might be CPU-bound
    result = await asyncio.to_thread(
        validator.get_uids_to_query,
        all_axons=all_axons
    )
    
    # Make sure we're returning a tuple of values, not a coroutine
    return result

@time_task("query_and_process_axons")
@cancellable_task
async def query_and_process_axons(validator):
    """
    Queries axons and processes the responses in batches with non-blocking response handling.
    This method is now only used as a fallback when WebSocket connections are not available or have failed.
    """
    try:
        bt.logging.info("\n--------------------------------Querying and processing axons (HTTP Fallback)--------------------------------\n")
        
        # First check if WebSocket is enabled and has active connections
        if (hasattr(validator, 'websocket_manager') and 
            validator.websocket_manager is not None and 
            validator.websocket_manager.is_running() and
            validator.websocket_manager.get_active_connection_count() > 0):
            
            # Count how many miners we're connected to via WebSocket
            active_connections = validator.websocket_manager.get_active_connection_count()
            total_miners = len(validator.metagraph.hotkeys)
            connection_percentage = (active_connections / total_miners) * 100 if total_miners > 0 else 0
            
            # If we have a significant portion of miners connected via WebSocket, skip HTTP polling
            if connection_percentage >= 50:  # Threshold: 50% of miners connected
                bt.logging.info(f"Skipping HTTP polling as {active_connections}/{total_miners} miners ({connection_percentage:.1f}%) are connected via WebSocket")
                validator.last_queried_block = validator.subtensor.block
                return None
            
            bt.logging.info(f"Only {connection_percentage:.1f}% of miners connected via WebSocket, proceeding with HTTP fallback")
        
        async with validator.operation_lock:
            validator.last_queried_block = validator.subtensor.block
            current_time = datetime.now(timezone.utc).isoformat()
            
            gamedata_dict = await validator.fetch_local_game_data(current_time)
            bt.logging.info(f"Number of games: {len(gamedata_dict)}")
            if gamedata_dict is None:
                bt.logging.error("No game data found")
                return None

            synapse = GameData.create(
                db_path=validator.db_path,
                wallet=validator.wallet,
                subnet_version=validator.subnet_version,
                neuron_uid=validator.uid,
                synapse_type="game_data",
                gamedata_dict=gamedata_dict,
            )
            
            if synapse is None:
                bt.logging.error("Synapse is None")
                return None

            # Get filtered axons
            filtered_axons = await filter_and_update_axons(validator)
            if filtered_axons is None:
                bt.logging.error("Failed to filter and update axons")
                return None

            uids_to_query, list_of_uids, blacklisted_uids, uids_not_to_query = filtered_axons
            
            # Check which miners are already connected via WebSocket
            connected_miners = set()
            if hasattr(validator, 'websocket_manager') and validator.websocket_manager is not None:
                connected_miners = set(validator.websocket_manager.get_connected_hotkeys())
            
            # Only query miners that aren't connected via WebSocket
            uids_to_query_filtered = []
            list_of_uids_filtered = []
            
            for i, axon in enumerate(uids_to_query):
                if axon.hotkey not in connected_miners:
                    uids_to_query_filtered.append(axon)
                    list_of_uids_filtered.append(list_of_uids[i])
            
            bt.logging.info(f"Querying {len(uids_to_query_filtered)} miners via HTTP (skipping {len(connected_miners)} WebSocket-connected miners)")
            
            # If no miners to query, return early
            if not uids_to_query_filtered:
                bt.logging.info("No miners to query via HTTP, all required miners are connected via WebSocket")
                return None
            
            # Batch size configuration
            BATCH_SIZE = 16
            MAX_CONCURRENT_REQUESTS = 3  # Limit concurrent outgoing requests
            
            # Split axons into batches
            axon_batches = [uids_to_query_filtered[i:i + BATCH_SIZE] for i in range(0, len(uids_to_query_filtered), BATCH_SIZE)]
            
            # Semaphore to limit outgoing requests
            request_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
            # Queue to store pending responses
            response_queue = asyncio.Queue()
            
            async def process_batch(batch):
                """Process a single batch with prioritized response handling"""
                try:
                    # Wait for semaphore before sending new requests
                    async with request_semaphore:
                        responses = await validator.dendrite.forward(
                            axons=batch,
                            synapse=synapse,
                            timeout=validator.timeout,
                            deserialize=False,
                        )
                        
                        if isinstance(responses, list):
                            # Filter valid responses
                            valid_responses = [
                                response for response in responses
                                if isinstance(response, GameData) and 
                                response.metadata.synapse_type == "prediction" and
                                response.prediction_dict
                            ]
                            # Put responses in queue for processing
                            if valid_responses:
                                await response_queue.put(valid_responses)
                            return len(valid_responses)
                        return 0
                        
                except Exception as e:
                    bt.logging.error(f"Error processing batch: {str(e)}")
                    return 0

            # Create tasks for all batches
            batch_tasks = [
                asyncio.create_task(process_batch(batch))
                for batch in axon_batches
            ]
            
            # Process responses as they come in
            total_responses = []
            processed_count = 0
            
            while batch_tasks or not response_queue.empty():
                # Prioritize processing responses if available
                try:
                    responses = await asyncio.wait_for(
                        response_queue.get(),
                        timeout=1.0  # Short timeout to check batch_tasks
                    )
                    total_responses.extend(responses)
                    response_queue.task_done()
                    processed_count += len(responses)
                except asyncio.TimeoutError:
                    # No responses available, check if any batch tasks completed
                    if batch_tasks:
                        done, batch_tasks = await asyncio.wait(
                            batch_tasks,
                            timeout=1.0,
                            return_when=asyncio.FIRST_COMPLETED
                        )
                        for task in done:
                            try:
                                processed_count += await task
                            except Exception as e:
                                bt.logging.error(f"Batch task error: {str(e)}")
                
            # Process all collected responses
            if total_responses:
                await validator.process_prediction(list_of_uids_filtered, total_responses)
            
            bt.logging.info(f"Processed {processed_count} valid responses from {len(uids_to_query_filtered)} total axons via HTTP")
            return total_responses

    except Exception as e:
        bt.logging.error(f"Error querying and processing axons: {str(e)}")
        bt.logging.error(traceback.format_exc())
        return None

@time_task("send_data_to_website_server")
@cancellable_task
async def send_data_to_website_server(validator):
    """Sends data to the website server"""
    bt.logging.info("\n--------------------------------Sending data to website server--------------------------------\n")
    validator.last_sent_data_to_website = validator.subtensor.block
    #bt.logging.info(f"Last sent data to website: {validator.last_sent_data_to_website}")

    try:
        result = await validator.website_handler.fetch_and_send_predictions()
        if result is True:
            bt.logging.info("Predictions fetched and sent successfully")
        elif result is False:
            bt.logging.warning("Failed to send predictions")
        else:
            bt.logging.info("No new predictions were sent this round")
    except Exception as e:
        bt.logging.error(f"Error in send_data_to_website_server: {str(e)}")
        bt.logging.error(traceback.format_exc())

@time_task("scoring_run")
@cancellable_task
async def scoring_run(validator, current_time):
    """
    calls the scoring system to update miner scores before setting weights
    """
    bt.logging.trace("\n--------------------------------Scoring run--------------------------------\n")
    validator.last_scoring_block = validator.subtensor.block
    
    try:
        # Get UIDs to query and invalid UIDs
        (
            _,
            list_of_uids,
            blacklisted_uids,
            uids_not_to_query,
        ) = validator.get_uids_to_query(validator.metagraph.axons)

        valid_uids = set(list_of_uids)
        # Combine blacklisted_uids and uids_not_to_query
        invalid_uids = set(blacklisted_uids + uids_not_to_query)
        bt.logging.info(f"Invalid UIDs: {invalid_uids}")
        validator.scores = await validator.scoring_system.scoring_run(
            current_time, invalid_uids, valid_uids
        )
        bt.logging.trace(f"Scoring run completed")

        for uid in blacklisted_uids:
            if uid is not None:
                bt.logging.debug(
                    f"Setting score for blacklisted UID: {uid}. Old score: {validator.scores[uid]}"
                )
                validator.scores[uid] = (
                    validator.neuron_config.alpha * validator.scores[uid]
                    + (1 - validator.neuron_config.alpha) * 0.0
                )
                bt.logging.debug(
                    f"Set score for blacklisted UID: {uid}. New score: {validator.scores[uid]}"
                )

        for uid in uids_not_to_query:
            if uid is not None:
                bt.logging.trace(
                    f"Setting score for not queried UID: {uid}. Old score: {validator.scores[uid]}"
                )
                validator_alpha_type = type(validator.neuron_config.alpha)
                validator_scores_type = type(validator.scores[uid])
                bt.logging.debug(
                    f"validator_alpha_type: {validator_alpha_type}, validator_scores_type: {validator_scores_type}"
                )
                validator.scores[uid] = (
                    validator.neuron_config.alpha * validator.scores[uid]
                    + (1 - validator.neuron_config.alpha) * 0.0
                )
                bt.logging.trace(
                    f"Set score for not queried UID: {uid}. New score: {validator.scores[uid]}"
                )

    except Exception as e:
        bt.logging.error(f"Error in scoring_run: {str(e)}")
        bt.logging.error(f"Traceback: {traceback.format_exc()}")
        raise


@time_task("set_weights")
@cancellable_task
async def set_weights(validator, weights_to_set):
    """Wrapper for weight setting that handles the multiprocessing timeout gracefully"""
    try:
        # Run the weight setter in a thread to not block the event loop
        result = await asyncio.to_thread(
            validator.weight_setter.set_weights,
            weights_to_set
        )
        bt.logging.info(f"Set weights result: {result}")
        if result is True:
            validator.last_set_weights_block = validator.subtensor.block
        else:
            validator.last_set_weights_block = validator.subtensor.block - 250 # Set weights block to 250 blocks ago, to prevent spamming the network with failed weight sets
        return result
    except Exception as e:
        validator.last_set_weights_block = validator.subtensor.block - 250 # Set weights block to 250 blocks ago, to prevent spamming the network with failed weight sets
        bt.logging.error(f"Error in set_weights wrapper: {str(e)}")
        bt.logging.error(traceback.format_exc())
        return False


@time_task("check_state_sync")
@cancellable_task
async def check_state_sync(validator):
    """Periodically check and sync state if needed"""
    while True:
        try:
            if not validator.is_primary:
                if await validator.state_sync.should_pull_state():
                    bt.logging.info("State divergence detected, pulling latest state")
                    async with async_timeout.timeout(300):  # 5 minute timeout
                        if await validator.state_sync.pull_state():
                            bt.logging.info("Successfully pulled latest state")
                        else:
                            bt.logging.error("Failed to pull latest state")
            await asyncio.sleep(3600)  # Check every hour
        except asyncio.TimeoutError:
            bt.logging.error("State sync check timed out")
        except Exception as e:
            bt.logging.error(f"Error in state sync check: {e}")
            await asyncio.sleep(300)  # On error, retry after 5 minutes
            
def cleanup_pycache():
    """Remove all __pycache__ directories and .pyc files"""
    bt.logging.info("Cleaning up __pycache__ directories and .pyc files")
    try:
        # Get the root directory (where the script is running)
        root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        
        # Walk through all directories
        for dirpath, dirnames, filenames in os.walk(root_dir):
            # Remove __pycache__ directories
            if '__pycache__' in dirnames:
                cache_path = os.path.join(dirpath, '__pycache__')
                try:
                    bt.logging.debug(f"Removing {cache_path}")
                    import shutil
                    shutil.rmtree(cache_path)
                except Exception as e:
                    bt.logging.warning(f"Failed to remove {cache_path}: {str(e)}")
            
            # Remove .pyc files
            for filename in filenames:
                if filename.endswith('.pyc'):
                    pyc_path = os.path.join(dirpath, filename)
                    try:
                        bt.logging.debug(f"Removing {pyc_path}")
                        os.remove(pyc_path)
                    except Exception as e:
                        bt.logging.warning(f"Failed to remove {pyc_path}: {str(e)}")
                        
    except Exception as e:
        bt.logging.error(f"Error during pycache cleanup: {str(e)}")

# The main function parses the configuration and runs the validator.
async def main():
    """Main function for running the validator."""
    global _validator
    
    # Configure signal handlers for graceful shutdown
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, signal_handler)
    
    # Clean up __pycache__ folders to avoid loading stale bytecode
    try:
        cleanup_pycache()
    except Exception as e:
        bt.logging.error(f"Error during pycache cleanup: {str(e)}")
    
    try:
        # Check if we should use the new event-driven validator
        # This can be controlled via environment variable or config parameter
        use_event_driven = os.environ.get("USE_EVENT_DRIVEN", "true").lower() == "true"
        
        if use_event_driven:
            bt.logging.info("Using new event-driven validator architecture")
            
            # Create and initialize the event-driven validator
            _validator = EventDrivenValidator.config()
            await _validator.initialize()
            
            # Start the validator (which schedules all periodic tasks)
            success = await _validator.start()
            
            if not success:
                bt.logging.error("Failed to start event-driven validator")
                return
                
            # Just wait until interrupted (the validator runs tasks in the background)
            bt.logging.info("Event-driven validator running, press Ctrl+C to exit")
            while True:
                await asyncio.sleep(1)
                
        else:
            bt.logging.info("Using legacy validator architecture")
            
            # Create and use the legacy validator
            _validator = await BettensorValidator.create()
            
            bt.logging.debug("Starting main validator process loop")
            await run(_validator)
            
    except asyncio.CancelledError:
        bt.logging.info("Validator was cancelled, performing graceful shutdown")
        # Let the signal handler or finally block handle cleanup
    except Exception as e:
        bt.logging.error(f"Error in validator: {str(e)}")
        bt.logging.error(traceback.format_exc())
        # Let the finally block handle cleanup
        raise
    finally:
        # Final cleanup in case it wasn't triggered by the signal handler
        if _validator:
            try:
                bt.logging.info("Performing final cleanup...")
                if isinstance(_validator, EventDrivenValidator):
                    await asyncio.wait_for(_validator.stop(), timeout=10)
                else:
                    await asyncio.wait_for(_validator.cleanup(), timeout=10)
                bt.logging.info("Final cleanup completed successfully")
            except (asyncio.TimeoutError, RuntimeError, Exception) as e:
                bt.logging.warning(f"Final cleanup attempt failed: {str(e)}")
        
        # Ensure validator reference is cleared
        _validator = None

if __name__ == "__main__":
    try:
        # Create a single event loop for both operations
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Run dependency check
        loop.run_until_complete(check_and_install_dependencies())
        
        # Run main function using the same loop
        loop.run_until_complete(main())
        
    except Exception as e:
        bt.logging.error(f"Startup error: {e}")
        bt.logging.error(traceback.format_exc())
        sys.exit(1)
    finally:
        try:
            # Clean up the loop
            loop.close()
        except Exception as e:
            bt.logging.error(f"Error closing event loop: {e}")
