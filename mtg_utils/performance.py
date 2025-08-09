"""Performance optimizations and retry logic for MTG data processing.

This module provides utilities for improving processing performance including
connection pooling, parallel processing, batch optimization, and retry logic
for handling transient failures.

Requires Python 3.10+
"""

import functools
import logging
import multiprocessing
import random
import sqlite3
import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from queue import Empty, Queue
from typing import Any, Callable, Generator, Iterator, Type

from .exceptions import (
    DatabaseConnectionError,
    DatabaseError,
    RetryableError,
    handle_sqlite_error,
)

logger = logging.getLogger(__name__)


@dataclass
class ProcessingStats:
    """Statistics for processing operations."""

    total_items: int = 0
    processed_items: int = 0
    failed_items: int = 0
    start_time: float = 0.0
    end_time: float = 0.0

    @property
    def duration(self) -> float:
        """Total processing duration in seconds."""
        return self.end_time - self.start_time if self.end_time > 0 else 0.0

    @property
    def items_per_second(self) -> float:
        """Processing rate in items per second."""
        return self.processed_items / self.duration if self.duration > 0 else 0.0

    @property
    def success_rate(self) -> float:
        """Success rate as a percentage."""
        return (
            (self.processed_items / self.total_items * 100)
            if self.total_items > 0
            else 0.0
        )


class ConnectionPool:
    """Thread-safe SQLite connection pool."""

    def __init__(self, db_path: Path, max_connections: int = 10):
        self.db_path = db_path
        self.max_connections = max_connections
        self._pool = Queue(maxsize=max_connections)
        self._created_connections = 0
        self._lock = threading.Lock()

    def _create_connection(self) -> sqlite3.Connection:
        """Create a new database connection."""
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        # Enable WAL mode for better concurrency
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=10000")
        conn.execute("PRAGMA temp_store=memory")
        return conn

    @contextmanager
    def get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        """Get a connection from the pool."""
        conn = None
        try:
            # Try to get existing connection
            try:
                conn = self._pool.get_nowait()
            except Empty:
                # Create new connection if pool is empty and under limit
                with self._lock:
                    if self._created_connections < self.max_connections:
                        conn = self._create_connection()
                        self._created_connections += 1
                    else:
                        # Wait for a connection to become available
                        conn = self._pool.get(timeout=30)

            yield conn

        except Exception as e:
            # Connection might be corrupted, don't return it to pool
            logger.warning(f"Connection error, discarding connection: {e}")
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
            raise
        else:
            # Return healthy connection to pool
            if conn:
                try:
                    self._pool.put_nowait(conn)
                except Exception:
                    # Pool is full, close the connection
                    conn.close()

    def close_all(self):
        """Close all connections in the pool."""
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except Empty:
                break
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")


class BatchProcessor:
    """Optimized batch processor for database operations."""

    def __init__(
        self,
        connection_pool: ConnectionPool,
        batch_size: int = 1000,
        max_workers: int = 4,
    ):
        self.pool = connection_pool
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.stats = ProcessingStats()

    def process_batches(
        self,
        data: list[Any],
        process_func: Callable[[sqlite3.Connection, list[Any]], tuple[int, int, int]],
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> ProcessingStats:
        """Process data in parallel batches.

        Args:
            data: List of items to process
            process_func: Function to process each batch (conn, batch_data) -> (new, updated, skipped)
            progress_callback: Optional callback for progress updates (current, total)

        Returns:
            Processing statistics
        """
        self.stats = ProcessingStats(total_items=len(data), start_time=time.time())

        # Split data into batches
        batches = [
            data[i : i + self.batch_size] for i in range(0, len(data), self.batch_size)
        ]

        total_new = 0
        total_updated = 0
        total_skipped = 0

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all batches
            future_to_batch = {
                executor.submit(self._process_single_batch, batch, process_func): batch
                for batch in batches
            }

            # Process completed batches
            for future in as_completed(future_to_batch):
                try:
                    new, updated, skipped = future.result()
                    total_new += new
                    total_updated += updated
                    total_skipped += skipped

                    self.stats.processed_items += new + updated
                    self.stats.failed_items += skipped

                    if progress_callback:
                        progress_callback(
                            self.stats.processed_items, self.stats.total_items
                        )

                except Exception as e:
                    batch = future_to_batch[future]
                    logger.error(f"Batch processing failed: {e}")
                    self.stats.failed_items += len(batch)

        self.stats.end_time = time.time()

        logger.info(
            f"Batch processing completed: {total_new} new, {total_updated} updated, "
            f"{total_skipped} skipped in {self.stats.duration:.2f}s "
            f"({self.stats.items_per_second:.1f} items/sec)"
        )

        return self.stats

    def _process_single_batch(
        self,
        batch: list[Any],
        process_func: Callable[[sqlite3.Connection, list[Any]], tuple[int, int, int]],
    ) -> tuple[int, int, int]:
        """Process a single batch of data."""
        with self.pool.get_connection() as conn:
            return process_func(conn, batch)


class ParallelFileProcessor:
    """Parallel processor for multiple files."""

    def __init__(self, max_workers: int | None = None):
        self.max_workers = max_workers or multiprocessing.cpu_count()

    def process_files(
        self,
        files: list[Path],
        process_func: Callable[[Path], Any],
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> list[Any]:
        """Process multiple files in parallel.

        Args:
            files: List of file paths to process
            process_func: Function to process each file
            progress_callback: Optional progress callback

        Returns:
            List of processing results
        """
        results = []

        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all files
            future_to_file = {
                executor.submit(process_func, file_path): file_path
                for file_path in files
            }

            completed = 0

            # Process completed files
            for future in as_completed(future_to_file):
                try:
                    result = future.result()
                    results.append(result)
                    completed += 1

                    if progress_callback:
                        progress_callback(completed, len(files))

                except Exception as e:
                    file_path = future_to_file[future]
                    logger.error(f"File processing failed for {file_path}: {e}")
                    results.append(None)

        return results


def chunked(iterable: list[Any], chunk_size: int) -> Iterator[list[Any]]:
    """Split an iterable into chunks of specified size.

    Args:
        iterable: Iterable to chunk
        chunk_size: Size of each chunk

    Yields:
        Chunks of the iterable
    """
    for i in range(0, len(iterable), chunk_size):
        yield iterable[i : i + chunk_size]


def optimize_sqlite_connection(conn: sqlite3.Connection) -> None:
    """Apply SQLite performance optimizations to a connection.

    Args:
        conn: Database connection to optimize
    """
    try:
        # Enable WAL mode for better concurrency
        conn.execute("PRAGMA journal_mode=WAL")

        # Reduce synchronous writes for better performance
        conn.execute("PRAGMA synchronous=NORMAL")

        # Increase cache size
        conn.execute("PRAGMA cache_size=10000")

        # Store temporary tables in memory
        conn.execute("PRAGMA temp_store=memory")

        # Increase mmap size for better I/O
        conn.execute("PRAGMA mmap_size=268435456")  # 256MB

        conn.commit()

    except sqlite3.Error as e:
        logger.warning(f"Could not apply all SQLite optimizations: {e}")


class ProgressTracker:
    """Thread-safe progress tracker."""

    def __init__(self, total: int):
        self.total = total
        self.current = 0
        self.lock = threading.Lock()
        self.start_time = time.time()

    def update(self, increment: int = 1) -> tuple[int, float]:
        """Update progress and return current count and percentage.

        Args:
            increment: Amount to increment by

        Returns:
            Tuple of (current_count, percentage_complete)
        """
        with self.lock:
            self.current += increment
            percentage = (self.current / self.total) * 100 if self.total > 0 else 0
            return self.current, percentage

    def get_eta(self) -> float:
        """Estimate time remaining in seconds."""
        with self.lock:
            if self.current == 0:
                return 0.0

            elapsed = time.time() - self.start_time
            rate = self.current / elapsed
            remaining_items = self.total - self.current

            return remaining_items / rate if rate > 0 else 0.0

    def get_rate(self) -> float:
        """Get current processing rate (items per second)."""
        with self.lock:
            elapsed = time.time() - self.start_time
            return self.current / elapsed if elapsed > 0 else 0.0


def memory_efficient_batch_generator(
    data: list[Any], batch_size: int
) -> Generator[list[Any], None, None]:
    """Memory-efficient batch generator that doesn't load all data at once.

    Args:
        data: Data to batch
        batch_size: Size of each batch

    Yields:
        Batches of data
    """
    for i in range(0, len(data), batch_size):
        yield data[i : i + batch_size]


class AsyncDatabaseWriter:
    """Asynchronous database writer for high-throughput scenarios."""

    def __init__(self, connection_pool: ConnectionPool, buffer_size: int = 10000):
        self.pool = connection_pool
        self.buffer_size = buffer_size
        self.write_queue = Queue(maxsize=buffer_size)
        self.writer_thread = None
        self.stop_event = threading.Event()
        self.stats = ProcessingStats()

    def start(self):
        """Start the async writer thread."""
        self.stop_event.clear()
        self.writer_thread = threading.Thread(target=self._writer_loop)
        self.writer_thread.daemon = True
        self.writer_thread.start()
        self.stats.start_time = time.time()

    def stop(self, timeout: float = 30.0):
        """Stop the async writer and wait for completion."""
        self.stop_event.set()
        if self.writer_thread:
            self.writer_thread.join(timeout=timeout)
        self.stats.end_time = time.time()

    def write_batch(self, query: str, data: list[tuple]):
        """Queue a batch for writing."""
        self.write_queue.put((query, data), timeout=10)

    def _writer_loop(self):
        """Main writer loop running in background thread."""
        while not self.stop_event.is_set():
            try:
                try:
                    query, data = self.write_queue.get(timeout=1.0)
                except Empty:
                    continue

                with self.pool.get_connection() as conn:
                    rows_affected = bulk_insert_with_transaction(conn, query, data)
                    self.stats.processed_items += rows_affected

                self.write_queue.task_done()

            except Exception as e:
                logger.error(f"Async writer error: {e}")
                self.stats.failed_items += len(data) if "data" in locals() else 0

        # Process remaining items in queue
        while not self.write_queue.empty():
            try:
                query, data = self.write_queue.get_nowait()
                with self.pool.get_connection() as conn:
                    bulk_insert_with_transaction(conn, query, data)
                self.write_queue.task_done()
            except Empty:
                break
            except Exception as e:
                logger.error(f"Final cleanup error: {e}")


# =============================================================================
# RETRY LOGIC (from retry.py)
# =============================================================================


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


@retry_database_operation(max_retries=3, base_delay=0.1)
def bulk_insert_with_transaction(
    conn: sqlite3.Connection, query: str, data: list[tuple]
) -> int:
    """Perform bulk insert with transaction and retry logic.

    Args:
        conn: Database connection
        query: SQL insert query
        data: List of data tuples

    Returns:
        Number of rows affected
    """
    try:
        cursor = conn.cursor()
        cursor.execute("BEGIN IMMEDIATE")

        cursor.executemany(query, data)
        rows_affected = cursor.rowcount

        conn.commit()
        return rows_affected

    except sqlite3.Error as e:
        try:
            conn.rollback()
        except sqlite3.Error:
            pass

        if "database is locked" in str(e).lower():
            raise RetryableError(f"Database locked during bulk insert: {e}")
        else:
            raise DatabaseError(f"Bulk insert failed: {e}", query=query)
