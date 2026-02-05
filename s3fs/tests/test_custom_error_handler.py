"""Tests for custom error handler functionality."""

import asyncio
import pytest
from botocore.exceptions import ClientError

import s3fs.core
from s3fs.core import (
    S3FileSystem,
    _error_wrapper,
    set_custom_error_handler,
    add_retryable_error,
)


# Custom exception types for testing
class CustomRetryableError(Exception):
    """A custom exception that should be retried."""

    pass


class CustomNonRetryableError(Exception):
    """A custom exception that should not be retried."""

    pass


@pytest.fixture(autouse=True)
def reset_error_handler():
    """Reset the custom error handler and retryable errors after each test."""
    original_errors = s3fs.core.S3_RETRYABLE_ERRORS
    yield
    # Reset to default handler
    s3fs.core.CUSTOM_ERROR_HANDLER = lambda e: False
    # Reset retryable errors tuple
    s3fs.core.S3_RETRYABLE_ERRORS = original_errors


def test_handler_retry_on_custom_exception():
    """Test that custom error handler allows retrying on custom exceptions."""
    call_count = 0

    async def failing_func():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise CustomRetryableError("Custom error that should retry")
        return "success"

    # Set up custom handler to retry CustomRetryableError
    def custom_handler(e):
        return isinstance(e, CustomRetryableError)

    set_custom_error_handler(custom_handler)

    # Should retry and eventually succeed
    async def run_test():
        result = await _error_wrapper(failing_func, retries=5)
        assert result == "success"
        assert call_count == 3  # Failed twice, succeeded on third attempt

    asyncio.run(run_test())


def test_handler_no_retry_on_other_exception():
    """Test that custom error handler does not retry exceptions it doesn't handle."""
    call_count = 0

    async def failing_func():
        nonlocal call_count
        call_count += 1
        raise CustomNonRetryableError("Custom error that should not retry")

    # Set up custom handler that only retries CustomRetryableError
    def custom_handler(e):
        return isinstance(e, CustomRetryableError)

    set_custom_error_handler(custom_handler)

    # Should not retry and fail immediately
    async def run_test():
        with pytest.raises(CustomNonRetryableError):
            await _error_wrapper(failing_func, retries=5)

        assert call_count == 1  # Should only be called once

    asyncio.run(run_test())


def test_handler_with_client_error():
    """Test that custom handler can make ClientError retryable."""
    call_count = 0

    async def failing_func():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            # Create a ClientError that doesn't match the built-in retry patterns
            error_response = {
                "Error": {
                    "Code": "CustomThrottlingError",
                    "Message": "Custom throttling message",
                }
            }
            raise ClientError(error_response, "operation_name")
        return "success"

    # Set up custom handler to retry on specific ClientError codes
    def custom_handler(e):
        if isinstance(e, ClientError):
            return e.response.get("Error", {}).get("Code") == "CustomThrottlingError"
        return False

    set_custom_error_handler(custom_handler)

    # Should retry and eventually succeed
    async def run_test():
        result = await _error_wrapper(failing_func, retries=5)
        assert result == "success"
        assert call_count == 3

    asyncio.run(run_test())


def test_handler_preserves_builtin_retry_pattern():
    """Test that custom handler doesn't interfere with built-in retry logic."""
    call_count = 0

    async def failing_func():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            # SlowDown is a built-in retryable pattern
            error_response = {
                "Error": {
                    "Code": "SlowDown",
                    "Message": "Please reduce your request rate",
                }
            }
            raise ClientError(error_response, "operation_name")
        return "success"

    # Set up a custom handler that handles something else
    def custom_handler(e):
        return isinstance(e, CustomRetryableError)

    set_custom_error_handler(custom_handler)

    # Should still retry SlowDown errors due to built-in logic
    async def run_test():
        result = await _error_wrapper(failing_func, retries=5)
        assert result == "success"
        assert call_count == 3

    asyncio.run(run_test())


def test_handler_max_retries():
    """Test that custom handler respects max retries."""
    call_count = 0

    async def always_failing_func():
        nonlocal call_count
        call_count += 1
        raise CustomRetryableError("Always fails")

    def custom_handler(e):
        return isinstance(e, CustomRetryableError)

    set_custom_error_handler(custom_handler)

    # Should retry up to retries limit then raise
    async def run_test():
        with pytest.raises(CustomRetryableError):
            await _error_wrapper(always_failing_func, retries=3)

        assert call_count == 3

    asyncio.run(run_test())


def test_handler_sleep_behavior():
    """Test that retries due to custom handler also wait between attempts."""
    call_times = []

    async def failing_func():
        call_times.append(asyncio.get_event_loop().time())
        raise CustomRetryableError("Retry me")

    def custom_handler(e):
        return isinstance(e, CustomRetryableError)

    set_custom_error_handler(custom_handler)

    async def run_test():
        with pytest.raises(CustomRetryableError):
            await _error_wrapper(failing_func, retries=3)

        # Should have made 3 attempts
        assert len(call_times) == 3

        # Check that there was a delay between attempts
        # The wait time formula is min(1.7**i * 0.1, 15)
        # For i=0: min(0.1, 15) = 0.1
        # For i=1: min(0.17, 15) = 0.17
        if len(call_times) >= 2:
            time_between_first_and_second = call_times[1] - call_times[0]
            # Should be roughly 0.1 seconds (with some tolerance)
            assert time_between_first_and_second >= 0.05

    asyncio.run(run_test())


def test_default_handler():
    """Test behavior when custom handler is not set explicitly."""
    call_count = 0

    async def failing_func():
        nonlocal call_count
        call_count += 1
        raise ValueError("Regular exception")

    # Don't set a custom handler, use default (returns False)
    # Should not retry regular exceptions
    async def run_test():
        with pytest.raises(ValueError):
            await _error_wrapper(failing_func, retries=5)

        assert call_count == 1

    asyncio.run(run_test())


def test_add_retryable_error():
    """Test adding a custom exception to the retryable errors tuple."""
    call_count = 0

    async def failing_func():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise CustomRetryableError("Custom error")
        return "success"

    # Add CustomRetryableError to the retryable errors
    add_retryable_error(CustomRetryableError)

    # Should now be retried automatically without custom handler
    async def run_test():
        result = await _error_wrapper(failing_func, retries=5)
        assert result == "success"
        assert call_count == 3

    asyncio.run(run_test())
