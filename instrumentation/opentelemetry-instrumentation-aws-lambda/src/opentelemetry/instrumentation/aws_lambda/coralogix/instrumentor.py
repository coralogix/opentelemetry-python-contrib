"""
This module provides functionality for instrumenting AWS Lambda functions with OpenTelemetry.
"""

from typing import Any, List, Type, Optional

from opentelemetry.trace import Tracer, Span
from opentelemetry.context.context import Context
from opentelemetry.semconv.resource import ResourceAttributes
from opentelemetry.semconv.trace import SpanAttributes

from opentelemetry.instrumentation.aws_lambda.coralogix.span import CoralogixSpan
from opentelemetry.instrumentation.aws_lambda.coralogix.span.api_gateway import APIGatewaySpan
from opentelemetry.instrumentation.aws_lambda.coralogix.span.cognito import CognitoSpan
from opentelemetry.instrumentation.aws_lambda.coralogix.span.dynamo import DynamoSpan
from opentelemetry.instrumentation.aws_lambda.coralogix.span.event_bridge import EventBridgeSpan
from opentelemetry.instrumentation.aws_lambda.coralogix.span.kinesis import KinesisSpan
from opentelemetry.instrumentation.aws_lambda.coralogix.span.s3 import S3Span
from opentelemetry.instrumentation.aws_lambda.coralogix.span.sns import SNSSpan
from opentelemetry.instrumentation.aws_lambda.coralogix.span.sqs import SQSSpan
from opentelemetry.instrumentation.aws_lambda.coralogix.span.step_functions import StepFunctionsSpan

from opentelemetry.instrumentation.aws_lambda.coralogix.logger import cx_exception

# There is no LambdaContext class in the AWS Lambda SDK, so we need to create our own.
# There is a package called types-aws-lambda but last release was in 2021.
class LambdaContext:
    function_name: str
    function_version: str
    invoked_function_arn: str
    memory_limit_in_mb: str
    aws_request_id: str
    log_group_name: str
    log_stream_name: str


def get_cx_instrumentor(
        tracer: Tracer,
        parent_context: Context,
        lambda_event: Any,
        orig_handler_name: str
) -> Optional[CoralogixSpan]:
    """
    Get the span and context for the given lambda event.
    """
    instrumentor_classes: List[Type[CoralogixSpan]] = [
        APIGatewaySpan,
        S3Span,
        SQSSpan,
        SNSSpan,
        KinesisSpan,
        DynamoSpan,
        CognitoSpan,
        EventBridgeSpan,
        StepFunctionsSpan,
    ]

    try:
        for instrumentor_class in instrumentor_classes:
            instrumentor = instrumentor_class(
                tracer, parent_context, lambda_event, orig_handler_name
            )
            if instrumentor.is_applicable():
                return instrumentor
    except Exception as ex:  # pylint: disable=broad-except
        cx_exception(ex, "Error instrumenting lambda function")

    return None

def set_cx_span_attributes(span: Span, lambda_context: LambdaContext) -> None:
    """
    Set the span attributes for the given lambda context.
    """
    span.set_attribute(
        ResourceAttributes.FAAS_ID, lambda_context.invoked_function_arn
    )
    span.set_attribute(
        SpanAttributes.FAAS_EXECUTION, lambda_context.aws_request_id,
    )
