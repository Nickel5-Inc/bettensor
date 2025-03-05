"""
Retry and transaction management utilities for the vesting system.
"""

import logging
import asyncio
from functools import wraps
from typing import Any, Callable, List, Optional, Type, Union
from datetime import datetime, timedelta

from sqlalchemy.exc import SQLAlchemyError, OperationalError, IntegrityError
from bettensor.validator.utils.database import DatabaseManager

logger = logging.getLogger(__name__)


class TransactionError(Exception):
    """Base class for transaction-related errors."""
    pass


class RetryableError(Exception):
    """Error that can be retried."""
    pass


def with_retries(
    max_attempts: int = 3,
    initial_delay: float = 0.1,
    max_delay: float = 2.0,
    backoff_factor: float = 2.0,
    retryable_errors: Optional[List[Type[Exception]]] = None
):
    """
    Decorator for retrying operations with exponential backoff.
    
    Args:
        max_attempts: Maximum number of retry attempts
        initial_delay: Initial delay between retries in seconds
        max_delay: Maximum delay between retries in seconds
        backoff_factor: Factor to increase delay by after each attempt
        retryable_errors: List of error types that should trigger a retry
    """
    if retryable_errors is None:
        retryable_errors = [
            OperationalError,
            RetryableError,
            asyncio.TimeoutError
        ]
    
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_error = None
            delay = initial_delay
            
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                    
                except tuple(retryable_errors) as e:
                    last_error = e
                    if attempt < max_attempts - 1:
                        logger.warning(
                            f"Attempt {attempt + 1}/{max_attempts} failed: {str(e)}. "
                            f"Retrying in {delay:.2f}s..."
                        )
                        await asyncio.sleep(delay)
                        delay = min(delay * backoff_factor, max_delay)
                    continue
                    
                except Exception as e:
                    # Non-retryable error
                    logger.error(f"Non-retryable error occurred: {str(e)}")
                    raise
            
            # If we get here, all retries failed
            logger.error(
                f"Operation failed after {max_attempts} attempts. "
                f"Last error: {str(last_error)}"
            )
            raise last_error
            
        return wrapper
    return decorator


class TransactionManager:
    """
    Manages database transactions with retry and rollback capabilities.
    """
    
    def __init__(
        self,
        db_manager: DatabaseManager,
        max_attempts: int = 3,
        initial_delay: float = 0.1
    ):
        self.db_manager = db_manager
        self.max_attempts = max_attempts
        self.initial_delay = initial_delay
    
    @with_retries()
    async def execute_in_transaction(
        self,
        operations: List[Callable],
        rollback_operations: Optional[List[Callable]] = None
    ):
        """
        Execute a list of operations in a transaction with rollback support.
        
        Args:
            operations: List of async callables to execute
            rollback_operations: Optional list of async callables to execute on rollback
        """
        async with self.db_manager.async_session() as session:
            try:
                # Start transaction
                transaction = await session.begin()
                
                try:
                    # Execute all operations
                    results = []
                    for op in operations:
                        result = await op(session)
                        results.append(result)
                    
                    # Commit transaction
                    await transaction.commit()
                    return results
                    
                except Exception as e:
                    # Rollback transaction
                    await transaction.rollback()
                    
                    # Execute rollback operations if provided
                    if rollback_operations:
                        try:
                            for op in rollback_operations:
                                await op(session)
                        except Exception as rollback_error:
                            logger.error(
                                f"Error during rollback: {str(rollback_error)}"
                            )
                    
                    raise TransactionError(f"Transaction failed: {str(e)}")
                    
            except Exception as e:
                logger.error(f"Database error: {str(e)}")
                raise
    
    @with_retries()
    async def execute_batch_insert(
        self,
        table_name: str,
        columns: List[str],
        values: List[dict],
        conflict_resolution: Optional[str] = None
    ):
        """
        Execute a batch insert operation with retry support.
        
        Args:
            table_name: Name of the table to insert into
            columns: List of column names
            values: List of dictionaries containing values to insert
            conflict_resolution: Optional ON CONFLICT clause
        """
        if not values:
            return
            
        async with self.db_manager.async_session() as session:
            try:
                # Construct base query
                query = f"""
                    INSERT INTO {table_name} 
                        ({', '.join(columns)})
                    SELECT 
                        {', '.join(f'v.{col}' for col in columns)}
                    FROM UNNEST(:values) AS v({
                        ', '.join(f'{col} {self._get_pg_type(values[0][col])}' 
                                for col in columns)
                    })
                """
                
                # Add conflict resolution if provided
                if conflict_resolution:
                    query += f"\n{conflict_resolution}"
                
                # Execute query
                await session.execute(
                    query,
                    {"values": values}
                )
                
                await session.commit()
                
            except IntegrityError as e:
                await session.rollback()
                logger.error(f"Integrity error during batch insert: {str(e)}")
                raise
                
            except Exception as e:
                await session.rollback()
                logger.error(f"Error during batch insert: {str(e)}")
                raise
    
    @staticmethod
    def _get_pg_type(value: Any) -> str:
        """Get PostgreSQL type for a Python value."""
        if isinstance(value, bool):
            return "boolean"
        elif isinstance(value, int):
            return "bigint"
        elif isinstance(value, float):
            return "float"
        elif isinstance(value, datetime):
            return "timestamp"
        else:
            return "text" 