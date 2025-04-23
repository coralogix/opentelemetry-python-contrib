from typing import Any, Callable, Optional

from opentelemetry.context import Context
from opentelemetry.propagate import get_global_textmap

from opentelemetry.instrumentation.aws_lambda.coralogix.logger import cx_exception, cx_debug


def determine_parent_context(
    args: Any,
    event_context_extractor: Optional[Callable[[Any], Context]],
    upstream_context_extractor: Callable[[Any], Context],
) -> Optional[Context]:
    """Determine the parent context for the current Lambda invocation.

    See more:
    https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/instrumentation/aws-lambda.md#determining-the-parent-of-a-span

    Args:
        args: lambda provided arguments.
        event_context_extractor: a method which takes the Lambda
            Event as input and extracts an OTel Context from it. By default,
            the context is extracted from the HTTP headers of an API Gateway
            request.
    Returns:
        A Context with configuration found in the carrier.
    """

    if event_context_extractor is None:
        return default_context_extractor(args, upstream_context_extractor)

    return event_context_extractor(args)

def default_context_extractor(
        args: Any,
        upstream_context_extractor: Callable[[Any], Context]
) -> Optional[Context]:
    """Default way of extracting the context from the Lambda Event for Coralogix Instrumentation.

    This is a wrapper for _determine_parent_context.

    Args:
        args: lambda provided arguments.
    Returns:
        A Context with configuration found in the event.
    """
    if len(args) < 2:
        cx_exception(Exception("Not enough arguments"), "Not enough arguments to get the parent context")

    cx_debug("Getting parent context from arguments", args)

    lambda_event = args[0]
    context = args[1]

    # Try to extract context from Lambda Event
    parent_context : Optional[Context] = None
    try:
        parent_context = get_global_textmap().extract(context.client_context.custom)
    except Exception as ex:  # pylint: disable=broad-except
        cx_exception(ex, "Error extracting context from Lambda Event")
        return None

    cx_debug("Parent context extracted from Lambda Event", parent_context)

    if parent_context:
        return parent_context

    # Use the upstream context extractor if no context was found in the Lambda Event
    try:
        parent_context = upstream_context_extractor(lambda_event)
    except Exception as ex:  # pylint: disable=broad-except
        cx_exception(ex, "Error extracting context from upstream")

    cx_debug("Parent context extracted from upstream", parent_context)

    return parent_context
