"""Custom exceptions for MTG data processing.

This module defines custom exception types that provide better error handling
and debugging capabilities throughout the MTG processing pipeline.

Requires Python 3.10+
"""

import sqlite3
from pathlib import Path
from typing import Any


class MTGProcessingError(Exception):
    """Base exception for all MTG processing errors."""

    def __init__(self, message: str, context: dict[str, Any] | None = None):
        self.message = message
        self.context = context or {}
        super().__init__(self.message)

    def __str__(self) -> str:
        if self.context:
            context_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            return f"{self.message} (Context: {context_str})"
        return self.message


class FileOperationError(MTGProcessingError):
    """Raised when file operations fail."""

    def __init__(
        self, message: str, file_path: Path | None = None, operation: str | None = None
    ):
        context = {}
        if file_path:
            context["file_path"] = str(file_path)
        if operation:
            context["operation"] = operation
        super().__init__(message, context)


class DatabaseError(MTGProcessingError):
    """Raised when database operations fail."""

    def __init__(
        self,
        message: str,
        query: str | None = None,
        table: str | None = None,
        original_error: Exception | None = None,
    ):
        context = {}
        if query:
            # Sanitize query in error messages to prevent information disclosure
            sanitized_query = self._sanitize_query(query)
            context["query"] = (
                sanitized_query[:100] + "..."
                if len(sanitized_query) > 100
                else sanitized_query
            )
        if table:
            context["table"] = table
        if original_error:
            context["original_error"] = str(original_error)
        super().__init__(message, context)

    def _sanitize_query(self, query: str) -> str:
        """Sanitize SQL query for error logging.

        Args:
            query: SQL query to sanitize

        Returns:
            Sanitized query string
        """
        # Remove potential sensitive data patterns
        import re

        sanitized = query

        # Replace string literals with placeholders
        sanitized = re.sub(r"'[^']*'", "'***'", sanitized)
        sanitized = re.sub(r'"[^"]*"', '"***"', sanitized)

        # Replace file paths
        sanitized = re.sub(r"/[\w/.-]+", "/***", sanitized)

        return sanitized


class CardProcessingError(MTGProcessingError):
    """Raised when card data processing fails."""

    def __init__(
        self,
        message: str,
        card_name: str | None = None,
        card_uuid: str | None = None,
        set_code: str | None = None,
    ):
        context = {}
        if card_name:
            context["card_name"] = card_name
        if card_uuid:
            context["card_uuid"] = card_uuid
        if set_code:
            context["set_code"] = set_code
        super().__init__(message, context)


class PriceProcessingError(MTGProcessingError):
    """Raised when price data processing fails."""

    def __init__(
        self,
        message: str,
        card_uuid: str | None = None,
        price_data: dict[str, Any] | None = None,
    ):
        context = {}
        if card_uuid:
            context["card_uuid"] = card_uuid
        if price_data:
            context["price_keys"] = list(price_data.keys())
        super().__init__(message, context)


class ConfigurationError(MTGProcessingError):
    """Raised when configuration is invalid or missing."""

    def __init__(
        self, message: str, config_key: str | None = None, config_value: Any = None
    ):
        context = {}
        if config_key:
            context["config_key"] = config_key
        if config_value is not None:
            context["config_value"] = str(config_value)
        super().__init__(message, context)


class RetryableError(MTGProcessingError):
    """Base class for errors that can be retried."""

    def __init__(
        self, message: str, max_retries: int = 3, context: dict[str, Any] | None = None
    ):
        self.max_retries = max_retries
        super().__init__(message, context)


class DatabaseConnectionError(RetryableError):
    """Raised when database connection fails but can be retried."""

    def __init__(self, message: str, db_path: Path | None = None):
        context = {"db_path": str(db_path)} if db_path else None
        super().__init__(message, context=context)


class FileCorruptionError(FileOperationError):
    """Raised when a file is corrupted or unreadable."""

    def __init__(self, file_path: Path, file_type: str = "unknown"):
        message = f"File appears to be corrupted: {file_path.name}"
        super().__init__(message, file_path, f"read_{file_type}")


class ValidationError(MTGProcessingError):
    """Raised when data validation fails."""

    def __init__(
        self,
        message: str,
        field_name: str | None = None,
        field_value: Any = None,
        expected_type: str | None = None,
    ):
        context = {}
        if field_name:
            context["field_name"] = field_name
        if field_value is not None:
            context["field_value"] = str(field_value)
        if expected_type:
            context["expected_type"] = expected_type
        super().__init__(message, context)


def handle_sqlite_error(
    error: sqlite3.Error, query: str | None = None, table: str | None = None
) -> DatabaseError:
    """Convert SQLite errors to our custom database errors.

    Args:
        error: The original SQLite error
        query: SQL query that caused the error (optional)
        table: Table name involved in the error (optional)

    Returns:
        Appropriate DatabaseError subclass
    """
    if isinstance(error, sqlite3.IntegrityError):
        return DatabaseError(
            "Database integrity constraint violated",
            query=query,
            table=table,
            original_error=error,
        )
    elif isinstance(error, sqlite3.OperationalError):
        return DatabaseError(
            "Database operation failed", query=query, table=table, original_error=error
        )
    elif isinstance(error, sqlite3.DatabaseError):
        return DatabaseError(
            "General database error", query=query, table=table, original_error=error
        )
    else:
        return DatabaseError(
            f"Unknown SQLite error: {type(error).__name__}",
            query=query,
            table=table,
            original_error=error,
        )


def reraise_with_context(original_error: Exception, context: dict[str, Any]) -> None:
    """Re-raise an exception with additional context.

    Args:
        original_error: The original exception
        context: Additional context to add

    Raises:
        MTGProcessingError with enhanced context
    """
    if isinstance(original_error, MTGProcessingError):
        # Add to existing context
        original_error.context.update(context)
        raise original_error
    else:
        # Create new exception with context
        raise MTGProcessingError(
            f"{type(original_error).__name__}: {str(original_error)}", context=context
        ) from original_error
