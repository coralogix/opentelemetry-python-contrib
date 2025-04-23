"""
This module is used to log messages and exceptions to the console.
It is used to debug the application and to log errors.
"""
import os
import sys
import traceback
from typing import Any, Optional

CX_DEBUG = os.getenv("CX_DEBUG", "").lower() == "true"


def cx_debug(message: Any, *args: Any) -> None:
    """
    Logs a debug message if the CX_DEBUG environment variable is set to 'true'.
    
    print method is used to avoid any possible issues with instrumentation in logging library.

    Args:
        message: The message to be logged. Can be any type that can be converted to string.
    """
    if CX_DEBUG:
        print(f"[DEBUG] {message}", *args)


def cx_exception(exception: Exception, message: Optional[str] = None) -> None:
    """
    Logs detailed information about an exception including file, line number, and traceback.
    Then re-raises the exception.
    
    print method is used to avoid any possible issues with instrumentation in logging library.

    Args:
        exception: The exception that was caught
        message: Optional additional message to include in the log
    """
    if not CX_DEBUG:
        return

    _, _, exc_traceback = sys.exc_info()
    if exc_traceback is not None:
        frame = exc_traceback.tb_frame
        filename = frame.f_code.co_filename
        line_number = frame.f_lineno
        function_name = frame.f_code.co_name

        error_message = f"Exception in {filename}:{line_number} ({function_name})"
        if message:
            error_message += f" - {message}"
        error_message += f"\nException: {str(exception)}"
        error_message += f"\nTraceback:\n{''.join(traceback.format_tb(exc_traceback))}"

        print(f"[ERROR] {error_message}")

    raise exception
