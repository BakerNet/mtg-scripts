"""Retry logic for MTG data processing operations.

This module provides decorators and utilities for retrying operations that
may fail due to temporary issues like database locks, network timeouts, etc.

Requires Python 3.10+
"""

import functools
import logging
import random
import sqlite3
import time
from typing import Any, Callable, Type

from .exceptions import (
    DatabaseConnectionError,
    DatabaseError,
    RetryableError,
    handle_sqlite_error,
)

logger = logging.getLogger(__name__)


def retry_on_exception(
    exceptions: tuple[Type[Exception], ...] = (RetryableError,),
    max_retries: int = 3,
    base_delay: float = 1.0,
    exponential_backoff: bool = True,
    jitter: bool = True,
) -> Callable:
    """Decorator to retry functions that may fail with specific exceptions.

    Args:
        exceptions: Tuple of exception types to retry on
        max_retries: Maximum number of retry attempts
        base_delay: Base delay in seconds between retries
        exponential_backoff: Whether to use exponential backoff
        jitter: Whether to add random jitter to delay

    Returns:
        Decorated function with retry logic
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e

                    if attempt >= max_retries:
                        logger.error(
                            f"Function {func.__name__} failed after {max_retries} retries"
                        )
                        raise e

                    # Calculate delay
                    delay = base_delay
                    if exponential_backoff:
                        delay *= 2**attempt
                    if jitter:
                        delay *= 0.5 + random.random()

                    logger.warning(
                        f"Function {func.__name__} failed (attempt {attempt + 1}/{max_retries + 1}): "
                        f"{e}. Retrying in {delay:.2f}s..."
                    )
                    time.sleep(delay)
                except Exception as e:
                    # Don't retry on non-retryable exceptions
                    logger.error(
                        f"Function {func.__name__} failed with non-retryable error: {e}"
                    )
                    raise e

            # Should never reach here, but just in case
            if last_exception:
                raise last_exception

        return wrapper

    return decorator


def retry_database_operation(max_retries: int = 3, base_delay: float = 0.5) -> Callable:
    """Decorator specifically for database operations with SQLite-specific handling.

    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Base delay in seconds between retries

    Returns:
        Decorated function with database retry logic
    """
    return retry_on_exception(
        exceptions=(
            sqlite3.OperationalError,  # Database locks, busy, etc.
            DatabaseConnectionError,
            RetryableError,
        ),
        max_retries=max_retries,
        base_delay=base_delay,
        exponential_backoff=True,
        jitter=True,
    )


class RetryableOperation:
    """Context manager for retryable operations with custom logic."""

    def __init__(
        self, operation_name: str, max_retries: int = 3, base_delay: float = 1.0
    ):
        self.operation_name = operation_name
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.current_attempt = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            return False  # No exception, don't suppress

        if not issubclass(exc_type, RetryableError):
            return False  # Not retryable, don't suppress

        self.current_attempt += 1

        if self.current_attempt > self.max_retries:
            logger.error(
                f"Operation {self.operation_name} failed after {self.max_retries} retries"
            )
            return False  # Don't suppress, let it raise

        # Calculate delay and sleep
        delay = self.base_delay * (2 ** (self.current_attempt - 1))
        delay *= 0.5 + random.random()  # Add jitter

        logger.warning(
            f"Operation {self.operation_name} failed "
            f"(attempt {self.current_attempt}/{self.max_retries}): {exc_val}. "
            f"Retrying in {delay:.2f}s..."
        )
        time.sleep(delay)

        return True  # Suppress the exception to retry

    def should_retry(self) -> bool:
        """Check if we should continue retrying."""
        return self.current_attempt <= self.max_retries


def handle_database_errors(func: Callable) -> Callable:
    """Decorator to convert SQLite errors to our custom exceptions.

    Args:
        func: Function to decorate

    Returns:
        Decorated function with error handling
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        try:
            return func(*args, **kwargs)
        except sqlite3.Error as e:
            # Convert to our custom exception
            raise handle_sqlite_error(e)
        except Exception:
            # Re-raise other exceptions unchanged
            raise

    return wrapper


@retry_database_operation(max_retries=5, base_delay=0.1)
def execute_with_retry(
    cursor: sqlite3.Cursor, query: str, params: tuple = ()
) -> sqlite3.Cursor:
    """Execute a database query with automatic retry on transient failures.

    Args:
        cursor: Database cursor
        query: SQL query to execute
        params: Query parameters

    Returns:
        Cursor after execution

    Raises:
        DatabaseError: If query fails after all retries
    """
    try:
        return cursor.execute(query, params)
    except sqlite3.OperationalError as e:
        if "database is locked" in str(e).lower():
            # This is retryable
            raise RetryableError(f"Database is locked: {e}")
        else:
            # Convert to our exception
            raise handle_sqlite_error(e, query=query)
    except sqlite3.Error as e:
        raise handle_sqlite_error(e, query=query)


@retry_database_operation(max_retries=3, base_delay=0.2)
def commit_with_retry(conn: sqlite3.Connection) -> None:
    """Commit a database transaction with automatic retry.

    Args:
        conn: Database connection

    Raises:
        DatabaseError: If commit fails after all retries
    """
    try:
        conn.commit()
    except sqlite3.OperationalError as e:
        if "database is locked" in str(e).lower():
            raise RetryableError(f"Database commit failed - locked: {e}")
        else:
            raise handle_sqlite_error(e)
    except sqlite3.Error as e:
        raise handle_sqlite_error(e)


class DatabaseTransaction:
    """Context manager for database transactions with retry logic."""

    def __init__(self, conn: sqlite3.Connection, max_retries: int = 3):
        self.conn = conn
        self.max_retries = max_retries
        self._in_transaction = False

    def __enter__(self):
        try:
            self.conn.execute("BEGIN")
            self._in_transaction = True
            return self
        except sqlite3.Error as e:
            raise handle_sqlite_error(e)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self._in_transaction:
            return False

        try:
            if exc_type is None:
                # Success - commit
                commit_with_retry(self.conn)
            else:
                # Error - rollback
                self.conn.rollback()
        except sqlite3.Error as e:
            logger.error(f"Error during transaction cleanup: {e}")
            try:
                self.conn.rollback()
            except sqlite3.Error:
                pass  # Ignore rollback errors
        finally:
            self._in_transaction = False

        return False  # Don't suppress exceptions

    def execute(self, query: str, params: tuple = ()) -> sqlite3.Cursor:
        """Execute a query within this transaction."""
        if not self._in_transaction:
            raise DatabaseError("Cannot execute query - not in transaction")

        cursor = self.conn.cursor()
        return execute_with_retry(cursor, query, params)
