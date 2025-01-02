# Copyright 2020, OpenTelemetry Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
The opentelemetry-instrumentation-aws-lambda package provides an Instrumentor
to traces calls within a Python AWS Lambda function.

Usage
-----

.. code:: python

    # Copy this snippet into an AWS Lambda function

    import boto3
    from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
    from opentelemetry.instrumentation.aws_lambda import AwsLambdaInstrumentor

    # Enable instrumentation
    BotocoreInstrumentor().instrument()
    AwsLambdaInstrumentor().instrument()

    # Lambda function
    def lambda_handler(event, context):
        s3 = boto3.resource('s3')
        for bucket in s3.buckets.all():
            print(bucket.name)

        return "200 OK"

API
---

The `instrument` method accepts the following keyword args:

tracer_provider (TracerProvider) - an optional tracer provider
meter_provider (MeterProvider) - an optional meter provider
event_context_extractor (Callable) - a function that returns an OTel Trace
Context given the Lambda Event the AWS Lambda was invoked with
this function signature is: def event_context_extractor(lambda_event: Any) -> Context
for example:

.. code:: python

    from opentelemetry.instrumentation.aws_lambda import AwsLambdaInstrumentor

    def custom_event_context_extractor(lambda_event):
        # If the `TraceContextTextMapPropagator` is the global propagator, we
        # can use it to parse out the context from the HTTP Headers.
        return get_global_textmap().extract(lambda_event["foo"]["headers"])

    AwsLambdaInstrumentor().instrument(
        event_context_extractor=custom_event_context_extractor
    )

---
"""

import logging
import os
import time
from importlib import import_module
from typing import Any, Callable, Collection
from urllib.parse import urlencode

from wrapt import wrap_function_wrapper

from opentelemetry.context.context import Context
from opentelemetry.instrumentation.aws_lambda.package import _instruments
from opentelemetry.instrumentation.aws_lambda.version import __version__
from opentelemetry.instrumentation.aws_lambda.utils import limit_string_size
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.metrics import MeterProvider, get_meter_provider
from opentelemetry.propagate import get_global_textmap
from opentelemetry.propagators.aws.aws_xray_propagator import (
    TRACE_HEADER_KEY,
    AwsXRayPropagator,
)
from opentelemetry.propagators import textmap
from opentelemetry.semconv.resource import ResourceAttributes
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import (
    Span,
    SpanKind,
    Link,
    Tracer,
    TracerProvider,
    get_current_span,
    get_tracer,
    get_tracer_provider,
    set_span_in_context,
    use_span,
)
from opentelemetry.trace.propagation import get_current_span
from opentelemetry.trace.span import (
    INVALID_SPAN_ID,
    format_trace_id,
    format_span_id,
)
import json
import typing
import base64
#import traceback

#tracemalloc.start(25)

logger = logging.getLogger(__name__)

_HANDLER = "_HANDLER"
_X_AMZN_TRACE_ID = "_X_AMZN_TRACE_ID"
ORIG_HANDLER = "ORIG_HANDLER"
OTEL_INSTRUMENTATION_AWS_LAMBDA_FLUSH_TIMEOUT = (
    "OTEL_INSTRUMENTATION_AWS_LAMBDA_FLUSH_TIMEOUT"
)
OTEL_LAMBDA_DISABLE_AWS_CONTEXT_PROPAGATION = (
    "OTEL_LAMBDA_DISABLE_AWS_CONTEXT_PROPAGATION"
)


def _default_event_context_extractor(args: Any) -> Context:
    """Default way of extracting the context from the Lambda Event.

    Assumes the Lambda Event is a map with the headers under the 'headers' key.
    This is the mapping to use when the Lambda is invoked by an API Gateway
    REST API where API Gateway is acting as a pure proxy for the request.
    Protects headers from being something other than dictionary, as this
    is what downstream propagators expect.

    See more:
    https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    Args:
        lambda_event: user-defined, so it could be anything, but this
            method counts on it being a map with a 'headers' key
    Returns:
        A Context with configuration found in the event.
    """

    lambda_event = args[0]

    #print("lambda_event")
    #print("args")
    #print(args)
    #print("context")
    #print(json.dumps(args[1], indent=4, sort_keys=True, default=str))
    context = args[1]
    try:
        #print(json.dumps(context.client_context.custom, indent=4, sort_keys=True, default=str))

        return get_global_textmap().extract(context.client_context.custom)
    except Exception as ex:
        #print(traceback.format_exc())
        #print("exception")
        #print(ex)
        pass

    headers = None
    try:
        headers = lambda_event["headers"]
    except (TypeError, KeyError):
        logger.debug(
            "Extracting context from Lambda Event failed: either enable X-Ray active tracing or configure API Gateway to trigger this Lambda function as a pure proxy. Otherwise, generated spans will have an invalid (empty) parent context."
        )
    if not isinstance(headers, dict):
        headers = {}
    return get_global_textmap().extract(headers)


def _determine_upstream_context(
    lambda_event: Any,
    event_context_extractor: Callable[[Any], Context],
    disable_aws_context_propagation: bool = False,
) -> Context:
    """Determine the upstream context for the current Lambda invocation.

    See more:
    https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/instrumentation/aws-lambda.md#determining-the-parent-of-a-span

    Args:
        lambda_event: user-defined, so it could be anything, but this
            method counts it being a map with a 'headers' key
        event_context_extractor: a method which takes the Lambda
            Event as input and extracts an OTel Context from it. By default,
            the context is extracted from the HTTP headers of an API Gateway
            request.
        disable_aws_context_propagation: By default, this instrumentation
            will try to read the context from the `_X_AMZN_TRACE_ID` environment
            variable set by Lambda, set this to `True` to disable this behavior.
    Returns:
        A Context with configuration found in the carrier.
    """
    upstream_context = None

    if not disable_aws_context_propagation:
        xray_env_var = os.environ.get(_X_AMZN_TRACE_ID)

        if xray_env_var:
            upstream_context = AwsXRayPropagator().extract(
                {TRACE_HEADER_KEY: xray_env_var}
            )

    if (
        upstream_context
        and get_current_span(upstream_context)
        .get_span_context()
        .trace_flags.sampled
    ):
        return upstream_context

    if event_context_extractor:
        upstream_context = event_context_extractor(lambda_event)
    else:
        upstream_context = _default_event_context_extractor(lambda_event)

    return upstream_context


def _set_api_gateway_v1_proxy_attributes(
    lambda_event: Any, span: Span
) -> Span:
    """Sets HTTP attributes for REST APIs and v1 HTTP APIs

    More info:
    https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format
    """
    span.set_attribute(
        SpanAttributes.HTTP_METHOD, lambda_event.get("httpMethod")
    )

    if lambda_event.get("body"):
        span.set_attribute(
            "http.request.body",
            limit_string_size(lambda_event.get("body")),
        )

    if lambda_event.get("headers"):
        if "User-Agent" in lambda_event["headers"]:
            span.set_attribute(
                SpanAttributes.HTTP_USER_AGENT,
                lambda_event["headers"]["User-Agent"],
            )
        if "X-Forwarded-Proto" in lambda_event["headers"]:
            span.set_attribute(
                SpanAttributes.HTTP_SCHEME,
                lambda_event["headers"]["X-Forwarded-Proto"],
            )
        if "Host" in lambda_event["headers"]:
            span.set_attribute(
                SpanAttributes.NET_HOST_NAME,
                lambda_event["headers"]["Host"],
            )
    if "resource" in lambda_event:
        span.set_attribute(SpanAttributes.HTTP_ROUTE, lambda_event["resource"])

        if lambda_event.get("queryStringParameters"):
            span.set_attribute(
                SpanAttributes.HTTP_TARGET,
                f"{lambda_event['resource']}?{urlencode(lambda_event['queryStringParameters'])}",
            )
        else:
            span.set_attribute(
                SpanAttributes.HTTP_TARGET, lambda_event["resource"]
            )

    return span


def _set_api_gateway_v2_proxy_attributes(
    lambda_event: Any, span: Span
) -> Span:
    """Sets HTTP attributes for v2 HTTP APIs

    More info:
    https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-develop-integrations-lambda.html
    """
    if "domainName" in lambda_event["requestContext"]:
        span.set_attribute(
            SpanAttributes.NET_HOST_NAME,
            lambda_event["requestContext"]["domainName"],
        )
    
    if lambda_event.get("body"):
        span.set_attribute(
            "http.request.body",
            limit_string_size(lambda_event.get("body")),
        )

    span.set_attribute(
        SpanAttributes.NET_HOST_NAME,
        lambda_event["requestContext"].get("domainName"),
    )

    if lambda_event["requestContext"].get("http"):
        if "method" in lambda_event["requestContext"]["http"]:
            span.set_attribute(
                SpanAttributes.HTTP_METHOD,
                lambda_event["requestContext"]["http"]["method"],
            )
        if "userAgent" in lambda_event["requestContext"]["http"]:
            span.set_attribute(
                SpanAttributes.HTTP_USER_AGENT,
                lambda_event["requestContext"]["http"]["userAgent"],
            )
        if "path" in lambda_event["requestContext"]["http"]:
            span.set_attribute(
                SpanAttributes.HTTP_ROUTE,
                lambda_event["requestContext"]["http"]["path"],
            )
            if lambda_event.get("rawQueryString"):
                span.set_attribute(
                    SpanAttributes.HTTP_TARGET,
                    f"{lambda_event['requestContext']['http']['path']}?{lambda_event['rawQueryString']}",
                )
            else:
                span.set_attribute(
                    SpanAttributes.HTTP_TARGET,
                    lambda_event["requestContext"]["http"]["path"],
                )

    return span


def _instrument(
    wrapped_module_name,
    wrapped_function_name,
    flush_timeout: int,
    event_context_extractor: Callable[[Any], Context],
    tracer_provider: TracerProvider = None,
    disable_aws_context_propagation: bool = False,
    meter_provider: MeterProvider = None,
):
    def _instrumented_lambda_handler_call(  # noqa pylint: disable=too-many-branches
        call_wrapped, instance, args, kwargs
    ):
        orig_handler_name = ".".join(
            [wrapped_module_name, wrapped_function_name]
        )

        lambda_event = args[0]

        upstream_context = _determine_upstream_context(
            args,
            event_context_extractor,
            disable_aws_context_propagation,
        )

        span_kind = None
        try:
            if lambda_event["Records"][0]["eventSource"] in {
                "aws:sqs",
                "aws:s3",
                "aws:sns",
                "aws:dynamodb",
            }:
                # See more:
                # https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html
                # https://docs.aws.amazon.com/lambda/latest/dg/with-sns.html
                # https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html
                # https://docs.aws.amazon.com/lambda/latest/dg/with-ddb.html
                span_kind = SpanKind.CONSUMER

            else:
                span_kind = SpanKind.SERVER
        except (IndexError, KeyError, TypeError):
            span_kind = SpanKind.SERVER
      
        tracer = get_tracer(__name__, __version__, tracer_provider)

        trigger_context = None
        triggerSpan = None

        apiGwSpan = None
        try:
            # If the request came from an API Gateway, extract http attributes from the event
            # https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/instrumentation/aws-lambda.md#api-gateway
            # https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/http.md#http-server-semantic-conventions
            if isinstance(lambda_event, dict) and lambda_event.get(
                "requestContext"
            ):
                span_name = orig_handler_name
                if lambda_event.get("resource"):
                    span_name = lambda_event.get("resource")
                if lambda_event.get("requestContext") and lambda_event["requestContext"].get("http"):
                    span_name = lambda_event["requestContext"]["http"].get("path")
                
                apiGwSpan = tracer.start_span(span_name, context=upstream_context, kind=SpanKind.CLIENT)
                if lambda_event.get("version") == "2.0":
                    apiGwSpan.set_attribute("faas.trigger.type", "Api Gateway Rest")
                else:
                    apiGwSpan.set_attribute("faas.trigger.type", "Api Gateway HTTP")
                
                apiGwSpan.set_attribute(SpanAttributes.FAAS_TRIGGER, "http")

                triggerSpan = apiGwSpan
                trigger_context = set_span_in_context(apiGwSpan)
        except Exception as ex:
            pass
        # S3 trigger new span and request attributes
        s3TriggerSpan = None
        try:
            if lambda_event["Records"][0]["eventSource"] in {
                "aws:s3",
            }:
                span_name = orig_handler_name
                if lambda_event["Records"][0].get("eventName"):
                    span_name = lambda_event["Records"][0].get("eventName")

                s3TriggerSpan = tracer.start_span(span_name, context=upstream_context, kind=SpanKind.PRODUCER)
                s3TriggerSpan.set_attribute(SpanAttributes.FAAS_TRIGGER, "datasource")
                s3TriggerSpan.set_attribute("faas.trigger.type", "S3")

                triggerSpan = s3TriggerSpan
                trigger_context = set_span_in_context(s3TriggerSpan)

                if lambda_event["Records"][0].get("s3"):
                    s3TriggerSpan.set_attribute(
                        "rpc.request.body",
                        limit_string_size(json.dumps(lambda_event["Records"][0].get("s3"))),
                    )    
        except Exception as ex:
            pass

        sqsTriggerSpan = None
        try:
            if lambda_event["Records"][0]["eventSource"] in {
                "aws:sqs",
            }:
                links = []
                queue_url = ""
                for record in lambda_event["Records"]:
                    if queue_url == "":
                        queue_url = record.get("eventSourceARN")

                    attributes = record.get("messageAttributes")
                    if attributes is not None:
                        ctx = get_global_textmap().extract(carrier=attributes, getter=SQSGetter())
                        span_ctx = get_current_span(ctx).get_span_context()
                        if span_ctx.span_id != INVALID_SPAN_ID:
                            links.append(Link(span_ctx))

                span_name = orig_handler_name
                sqsTriggerSpan = tracer.start_span(span_name, context=upstream_context, kind=SpanKind.CONSUMER, links=links)
                sqsTriggerSpan.set_attribute(SpanAttributes.FAAS_TRIGGER, "pubsub")
                sqsTriggerSpan.set_attribute("faas.trigger.type", "SQS")
                sqsTriggerSpan.set_attribute(SpanAttributes.MESSAGING_SYSTEM, "aws.sqs")
                sqsTriggerSpan.set_attribute(SpanAttributes.MESSAGING_URL, queue_url)

                try:
                    # Example queue_url: arn:aws:sqs:us-east-1:123456789012:my_queue_name
                    sqsTriggerSpan.set_attribute(SpanAttributes.MESSAGING_DESTINATION, queue_url.split(":")[-1])
                except IndexError:
                    pass
                
                triggerSpan = sqsTriggerSpan
                trigger_context = set_span_in_context(sqsTriggerSpan)

                if lambda_event["Records"][0].get("body"):
                    sqsTriggerSpan.set_attribute(
                        "rpc.request.body",
                        limit_string_size(lambda_event["Records"][0].get("body")),
                    )

        except Exception as ex:
            pass

        snsTriggerSpan = None
        try:
            if lambda_event["Records"][0]["EventSource"] == "aws:sns":
                links = []
                queue_url = ""
                for record in lambda_event["Records"]:
                    if record.get("Sns") is None:
                        continue

                    if queue_url == "":
                        queue_url = record.get("Sns").get("TopicArn")

                    attributes = record.get("Sns").get("MessageAttributes")
                    if attributes is not None:
                        ctx = get_global_textmap().extract(carrier=attributes, getter=SNSGetter())
                        span_ctx = get_current_span(ctx).get_span_context()
                        if span_ctx.span_id != INVALID_SPAN_ID:
                            links.append(Link(span_ctx))

                span_kind = SpanKind.INTERNAL
                span_name = orig_handler_name
                snsTriggerSpan = tracer.start_span(span_name, context=upstream_context, kind=SpanKind.CONSUMER, links=links)
                snsTriggerSpan.set_attribute(SpanAttributes.FAAS_TRIGGER, "pubsub")
                snsTriggerSpan.set_attribute("faas.trigger.type", "SNS")
                snsTriggerSpan.set_attribute(SpanAttributes.MESSAGING_SYSTEM, "aws.sns")
                snsTriggerSpan.set_attribute(SpanAttributes.MESSAGING_URL, queue_url)

                try:
                    # Example queue_url: arn:aws:sns:us-east-1:123456789012:my_topic_name
                    snsTriggerSpan.set_attribute(SpanAttributes.MESSAGING_DESTINATION, queue_url.split(":")[-1])
                except IndexError:
                    pass

                triggerSpan = snsTriggerSpan
                trigger_context = set_span_in_context(snsTriggerSpan)

                if lambda_event["Records"][0]["Sns"] and lambda_event["Records"][0]["Sns"].get("Message"):
                    snsTriggerSpan.set_attribute(
                        "rpc.request.body",
                        limit_string_size(lambda_event["Records"][0]["Sns"].get("Message")),
                    )    
        except Exception as ex:
            pass

        kinesisTriggerSpan = None
        try:
            if lambda_event["Records"][0]["eventSource"] == "aws:kinesis":
                links = []
                queue_url = ""

                for record in lambda_event["Records"]:
                    if record.get("kinesis") is None:
                        continue

                    if queue_url == "":
                        queue_url = record.get("eventSourceARN")

                    data = record["kinesis"].get("data")
                    if data is not None:
                        decoded_bytes = base64.b64decode(data)
                        decoded_string = decoded_bytes.decode('utf-8')
                        data = json.loads(decoded_string)
                        ctx = get_global_textmap().extract(carrier=data.get("_context"))
                        span_ctx = get_current_span(ctx).get_span_context()
                        if span_ctx.span_id != INVALID_SPAN_ID:
                            links.append(Link(span_ctx))
                span_kind = SpanKind.INTERNAL
                span_name = orig_handler_name
                kinesisTriggerSpan = tracer.start_span(span_name, context=upstream_context, kind=SpanKind.CONSUMER, links=links)
                kinesisTriggerSpan.set_attribute(SpanAttributes.FAAS_TRIGGER, "pubsub")
                kinesisTriggerSpan.set_attribute("faas.trigger.type", "Kinesis")
                kinesisTriggerSpan.set_attribute(SpanAttributes.MESSAGING_SYSTEM, "aws.kinesis")
                kinesisTriggerSpan.set_attribute(SpanAttributes.MESSAGING_URL, queue_url)

                try:
                    # Example queue_url: arn:aws:kinesis:us-east-1:123456789012:stream/my_stream_name
                    kinesisTriggerSpan.set_attribute(SpanAttributes.MESSAGING_DESTINATION, queue_url.split("/")[-1])
                except IndexError:
                    pass

                triggerSpan = kinesisTriggerSpan
                trigger_context = set_span_in_context(kinesisTriggerSpan)

                if lambda_event["Records"][0]["kinesis"] and lambda_event["Records"][0]["kinesis"].get("data"):
                    decoded_bytes = base64.b64decode(lambda_event["Records"][0]["kinesis"].get("data"))
                    decoded_string = decoded_bytes.decode('utf-8')
                    data = json.loads(decoded_string)

                    kinesisTriggerSpan.set_attribute(
                        "rpc.request.body",
                        limit_string_size(data),
                    )
        except Exception as e:
            pass

        dynamoTriggerSpan = None
        try:
            if lambda_event["Records"][0]["eventSource"] == "aws:dynamodb":
                span_name = orig_handler_name
                if lambda_event["Records"][0].get("eventName"):
                    span_name = lambda_event["Records"][0].get("eventName")

                dynamoTriggerSpan = tracer.start_span(span_name, context=upstream_context, kind=SpanKind.PRODUCER)
                dynamoTriggerSpan.set_attribute(SpanAttributes.FAAS_TRIGGER, "datasource")
                dynamoTriggerSpan.set_attribute("faas.trigger.type", "Dynamo DB")

                triggerSpan = dynamoTriggerSpan
                trigger_context = set_span_in_context(dynamoTriggerSpan)

                if lambda_event["Records"][0].get("dynamodb"):
                    dynamoTriggerSpan.set_attribute(
                        "rpc.request.body",
                        limit_string_size(json.dumps(lambda_event["Records"][0].get("dynamodb"))),
                    )    
        except Exception as ex:
            pass

        cognitoTriggerSpan = None
        try:
            if lambda_event["eventType"] == "SyncTrigger":
                span_name = orig_handler_name
                if lambda_event.get("eventType"):
                    span_name = lambda_event.get("eventType") 

                cognitoTriggerSpan = tracer.start_span(span_name, context=upstream_context, kind=SpanKind.PRODUCER)
                cognitoTriggerSpan.set_attribute(SpanAttributes.FAAS_TRIGGER, "datasource")
                cognitoTriggerSpan.set_attribute("faas.trigger.type", "Cognito")

                triggerSpan = cognitoTriggerSpan
                trigger_context = set_span_in_context(cognitoTriggerSpan)

                if lambda_event["datasetRecords"]:
                    cognitoTriggerSpan.set_attribute(
                        "rpc.request.body",
                        limit_string_size(json.dumps(lambda_event["datasetRecords"])),
                    )
        except Exception as ex:
            pass

        eventBridgeTriggerSpan = None
        try:
            if type(lambda_event) is dict and lambda_event.get("source") is not None and type(lambda_event.get("source")) is str:
                span_name = 'EventBridge event'
                if lambda_event.get("detail-type") is not None:
                    span_name = lambda_event.get("detail-type")

                links = []
                if lambda_event.get("detail") is not None and lambda_event["detail"].get("_context") is not None:
                    ctx = get_global_textmap().extract(carrier=lambda_event["detail"].get("_context"))
                    span_ctx = get_current_span(ctx).get_span_context()
                    if span_ctx.span_id != INVALID_SPAN_ID:
                        links.append(Link(span_ctx))

                eventBridgeTriggerSpan = tracer.start_span(span_name, context=upstream_context, kind=SpanKind.CONSUMER, links=links)
                eventBridgeTriggerSpan.set_attribute(SpanAttributes.FAAS_TRIGGER, "pubsub")
                eventBridgeTriggerSpan.set_attribute("faas.trigger.type", "EventBridge")
                eventBridgeTriggerSpan.set_attribute("aws.event.bridge.trigger.source", lambda_event.get("source"))

                triggerSpan = eventBridgeTriggerSpan
                trigger_context = set_span_in_context(eventBridgeTriggerSpan)

                eventBridgeTriggerSpan.set_attribute(
                    "rpc.request.body",
                    limit_string_size(json.dumps(lambda_event)),
                )
        except Exception as ex:
            pass

        if triggerSpan is not None:
            triggerSpan.set_attribute("cx.internal.span.role", "trigger")
 
        try:
            if trigger_context is not None:
                invocation_parent_context = trigger_context
            else:
                invocation_parent_context = upstream_context

            invocationSpan = tracer.start_span(
                name=orig_handler_name,
                context=invocation_parent_context,
                kind=span_kind,
            )
            invocationSpan.set_attribute("cx.internal.span.role", "invocation")
            try:
                _sendEarlySpans(
                    flush_timeout,
                    tracer,
                    tracer_provider,
                    meter_provider,
                    trigger_parent_context=upstream_context,
                    trigger_span=triggerSpan,
                    invocation_parent_context=invocation_parent_context,
                    invocation_span=invocationSpan,
                )
            except Exception as ex:
                pass

            with use_span(
                span=invocationSpan,
                end_on_exit=True,
            ) as span:
                if span.is_recording():
                    lambda_context = args[1]
                    # NOTE: The specs mention an exception here, allowing the
                    # `ResourceAttributes.FAAS_ID` attribute to be set as a span
                    # attribute instead of a resource attribute.
                    #
                    # See more:
                    # https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/faas.md#example
                    span.set_attribute(
                        ResourceAttributes.FAAS_ID,
                        lambda_context.invoked_function_arn,
                    )
                    span.set_attribute(
                        SpanAttributes.FAAS_EXECUTION,
                        lambda_context.aws_request_id,
                    )

                result = call_wrapped(*args, **kwargs)

                # If the request came from an AlambdaPI Gateway, extract http attributes from the event
                # https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/instrumentation/aws-lambda.md#api-gateway
                # https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/http.md#http-server-semantic-conventions
                try:
                    if lambda_event and apiGwSpan is not None and lambda_event.get("requestContext"):
                        apiGwSpan.set_attribute(SpanAttributes.FAAS_TRIGGER, "http")

                        if lambda_event.get("version") == "2.0":
                            _set_api_gateway_v2_proxy_attributes(lambda_event, apiGwSpan)
                        else:
                            _set_api_gateway_v1_proxy_attributes(lambda_event, apiGwSpan)

                        if isinstance(result, dict) and result.get("statusCode"):
                            apiGwSpan.set_attribute(
                                SpanAttributes.HTTP_STATUS_CODE,
                                result.get("statusCode"),
                            )
                        if isinstance(result, dict) and result.get("body"):
                            apiGwSpan.set_attribute(
                                "http.response.body",
                                limit_string_size(result.get("body")),
                            )
                        if lambda_event.get("headers"):
                            for key, value in lambda_event.get("headers").items():
                                apiGwSpan.set_attribute("http.request.header." + key.lower().replace("-", "_"), value)

                        if lambda_event["requestContext"].get("domainName") and lambda_event["requestContext"].get("http") and lambda_event["requestContext"].get("http").get("path"):
                            apiGwSpan.set_attribute(
                                SpanAttributes.HTTP_URL,
                                lambda_event["requestContext"].get("domainName") + lambda_event["requestContext"].get("http").get("path")
                            )
                    try:
                        if lambda_event["Records"][0]["eventSource"] == "aws:sqs":
                            span.set_attribute(SpanAttributes.FAAS_TRIGGER, "pubsub")
                            span.set_attribute("messaging.message",
                                            limit_string_size(lambda_event["Records"][0].get("body")))
                    except Exception as ex:
                        #print(traceback.format_exc())
                        #print("exception")
                        #print(ex)
                        pass
                except Exception as ex:
                    # TODO check why we get exception
                    # logger.error(
                    #    "TracerProvider was missing `force_flush` method. This is necessary in case of a Lambda freeze and would exist in the OTel SDK implementation."
                    # )
                    pass

                # S3 trigger response attributes
                if lambda_event and s3TriggerSpan is not None:
                    try:
                        if isinstance(result, dict) and result.get("statusCode"):
                            s3TriggerSpan.set_attribute(
                                SpanAttributes.HTTP_STATUS_CODE,
                                result.get("statusCode"),
                            )
                        if isinstance(result, dict) and result.get("body"):
                            s3TriggerSpan.set_attribute(
                                "rpc.response.body",
                                limit_string_size(result.get("body")),
                            )
                    except Exception:
                        pass

                # SQS trigger response attributes
                if lambda_event and sqsTriggerSpan is not None:
                    try:
                        if isinstance(result, dict) and result.get("statusCode"):
                            sqsTriggerSpan.set_attribute(
                                SpanAttributes.HTTP_STATUS_CODE,
                                result.get("statusCode"),
                            )
                        if isinstance(result, dict) and result.get("body"):
                            sqsTriggerSpan.set_attribute(
                                "rpc.response.body",
                                limit_string_size(result.get("body")),
                            )
                    except Exception:
                        pass

                if lambda_event and snsTriggerSpan is not None:
                    try:
                        if isinstance(result, dict) and result.get("statusCode"):
                            snsTriggerSpan.set_attribute(
                                SpanAttributes.HTTP_STATUS_CODE,
                                result.get("statusCode"),
                            )
                        if isinstance(result, dict) and result.get("body"):
                            snsTriggerSpan.set_attribute(
                                "rpc.response.body",
                                limit_string_size(result.get("body")),
                            )
                    except Exception:
                        pass

                if lambda_event and kinesisTriggerSpan is not None:
                    try:
                        if isinstance(result, dict) and result.get("ResponseMetadata"):
                            if result["ResponseMetadata"].get("HTTPStatusCode"):
                                kinesisTriggerSpan.set_attribute(
                                    SpanAttributes.HTTP_STATUS_CODE,
                                    result["ResponseMetadata"]["HTTPStatusCode"],
                                )
                    except Exception:
                        pass

                if lambda_event and dynamoTriggerSpan is not None:
                    try:
                        if isinstance(result, dict) and result.get("statusCode"):
                            dynamoTriggerSpan.set_attribute(
                                SpanAttributes.HTTP_STATUS_CODE,
                                result.get("statusCode"),
                            )
                        if isinstance(result, dict) and result.get("body"):
                            dynamoTriggerSpan.set_attribute(
                                "rpc.response.body",
                                limit_string_size(result.get("body")),
                            )
                    except Exception:
                        pass

                if lambda_event and cognitoTriggerSpan is not None:
                    try:
                        if isinstance(result, dict) and result.get("statusCode"):
                            cognitoTriggerSpan.set_attribute(
                                SpanAttributes.HTTP_STATUS_CODE,
                                result.get("statusCode"),
                            )
                        if isinstance(result, dict) and result.get("body"):
                            cognitoTriggerSpan.set_attribute(
                                "rpc.response.body",
                                limit_string_size(result.get("body")),
                            )
                    except Exception:
                        pass
                
                if lambda_event and eventBridgeTriggerSpan is not None:
                    try:
                        if isinstance(result, dict) and result.get("statusCode"):
                            eventBridgeTriggerSpan.set_attribute(
                                SpanAttributes.HTTP_STATUS_CODE,
                                result.get("statusCode"),
                            )
                        if isinstance(result, dict) and result.get("body"):
                            eventBridgeTriggerSpan.set_attribute(
                                "rpc.response.body",
                                limit_string_size(result.get("body")),
                            )
                    except Exception:
                        pass

        except Exception as e:
            raise e

        finally:
            if triggerSpan is not None:
                triggerSpan.end()
            _flush(flush_timeout, tracer_provider, meter_provider)

        return result

    wrap_function_wrapper(
        wrapped_module_name,
        wrapped_function_name,
        _instrumented_lambda_handler_call,
    )

def _sendEarlySpans(
    flush_timeout: int,
    tracer: Tracer,
    tracer_provider: TracerProvider,
    meter_provider: MeterProvider,
    trigger_parent_context: Context,
    trigger_span: Span,
    invocation_parent_context: Context,
    invocation_span: Span,
) -> None:
    if trigger_span is not None and hasattr(trigger_span, 'name'):
        early_trigger = _createEarlySpan(
            tracer,
            parent_context=trigger_parent_context,
            span=trigger_span
        )
        early_trigger.end()

    if invocation_span is not None and hasattr(invocation_span, 'name'):
        early_invocation = _createEarlySpan(
            tracer,
            parent_context=invocation_parent_context,
            span=invocation_span
        )
        early_invocation.end()

    _flush_traces(flush_timeout, tracer_provider)

def _createEarlySpan(
    tracer: Tracer,
    parent_context: Context,
    span: Span,
) -> Span:
    early_span = tracer.start_span(
        name=span.name,
        context=parent_context,
        kind=span.kind,
        attributes=span.attributes,
        links = span.links
    )
    early_span.set_attribute("cx.internal.span.state", "early")
    early_span.set_attribute("cx.internal.trace.id", format_trace_id(span.get_span_context().trace_id))
    early_span.set_attribute("cx.internal.span.id", format_span_id(span.get_span_context().span_id))
    return early_span

def _flush(
    flush_timeout: int,
    tracer_provider: TracerProvider = None,
    meter_provider: MeterProvider = None,
) -> None:
    _flush_traces(flush_timeout, tracer_provider)
    _flush_metrics(flush_timeout, meter_provider)

def _flush_traces(
    flush_timeout: int,
    tracer_provider: TracerProvider = None,
) -> None:
    _tracer_provider = tracer_provider or get_tracer_provider()
    if hasattr(_tracer_provider, "force_flush"):
        try:
            # NOTE: `force_flush` before function quit in case of Lambda freeze.
            _tracer_provider.force_flush(flush_timeout)
        except Exception:  # pylint: disable=broad-except
            logger.exception("TracerProvider failed to flush metrics")
    else:
        logger.warning(
            "TracerProvider was missing `force_flush` method. This is necessary in case of a Lambda freeze and would exist in the OTel SDK implementation."
        )

def _flush_metrics(
    flush_timeout: int,
    meter_provider: MeterProvider = None,
) -> None:
    _meter_provider = meter_provider or get_meter_provider()
    if hasattr(_meter_provider, "force_flush"):
        try:
            # NOTE: `force_flush` before function quit in case of Lambda freeze.
            _meter_provider.force_flush(flush_timeout)
        except Exception:  # pylint: disable=broad-except
            logger.exception("MeterProvider failed to flush metrics")
    else:
        logger.warning(
            "MeterProvider was missing `force_flush` method. This is necessary in case of a Lambda freeze and would exist in the OTel SDK implementation."
        )


class AwsLambdaInstrumentor(BaseInstrumentor):
    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs):
        """Instruments Lambda Handlers on AWS Lambda.

        See more:
        https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/instrumentation/aws-lambda.md#instrumenting-aws-lambda

        Args:
            **kwargs: Optional arguments
                ``tracer_provider``: a TracerProvider, defaults to global
                ``meter_provider``: a MeterProvider, defaults to global
                ``event_context_extractor``: a method which takes the Lambda
                    Event as input and extracts an OTel Context from it. By default,
                    the context is extracted from the HTTP headers of an API Gateway
                    request.
                ``disable_aws_context_propagation``: By default, this instrumentation
                    will try to read the context from the `_X_AMZN_TRACE_ID` environment
                    variable set by Lambda, set this to `True` to disable this behavior.
        """
        lambda_handler = os.environ.get(ORIG_HANDLER, os.environ.get(_HANDLER))
        # pylint: disable=attribute-defined-outside-init
        (
            self._wrapped_module_name,
            self._wrapped_function_name,
        ) = lambda_handler.rsplit(".", 1)

        flush_timeout_env = os.environ.get(
            OTEL_INSTRUMENTATION_AWS_LAMBDA_FLUSH_TIMEOUT, None
        )
        flush_timeout = 30000
        try:
            if flush_timeout_env is not None:
                flush_timeout = int(flush_timeout_env)
        except ValueError:
            logger.warning(
                "Could not convert OTEL_INSTRUMENTATION_AWS_LAMBDA_FLUSH_TIMEOUT value %s to int",
                flush_timeout_env,
            )

        disable_aws_context_propagation = kwargs.get(
            "disable_aws_context_propagation", True
        ) or os.getenv(
            OTEL_LAMBDA_DISABLE_AWS_CONTEXT_PROPAGATION, "True"
        ).strip().lower() in (
            "true",
            "1",
            "t",
        )

        _instrument(
            self._wrapped_module_name,
            self._wrapped_function_name,
            flush_timeout,
            event_context_extractor=kwargs.get(
                "event_context_extractor", _default_event_context_extractor
            ),
            tracer_provider=kwargs.get("tracer_provider"),
            disable_aws_context_propagation=disable_aws_context_propagation,
            meter_provider=kwargs.get("meter_provider"),
        )

    def _uninstrument(self, **kwargs):
        unwrap(
            import_module(self._wrapped_module_name),
            self._wrapped_function_name,
        )


class SQSGetter():
    def get(
        self, carrier: typing.Mapping[str, textmap.CarrierValT], key: str
    ) -> typing.Optional[typing.List[str]]:
        """Getter implementation to retrieve a value from a dictionary.

        Args:
            carrier: dictionary in which to get value
            key: the key used to get the value
        Returns:
            A list with a single string with the value if it exists, else None.
        """
        val = carrier.get(key, None)
        if val is None:
            return None
        if val.get("stringValue") is not None:
            return [val.get("stringValue")]
        return None

    def keys(
        self, carrier: typing.Mapping[str, textmap.CarrierValT]
    ) -> typing.List[str]:
        """Keys implementation that returns all keys from a dictionary."""
        return list(carrier.keys())


class SNSGetter():
    def get(
        self, carrier: typing.Mapping[str, textmap.CarrierValT], key: str
    ) -> typing.Optional[typing.List[str]]:
        """Getter implementation to retrieve a value from a dictionary.

        Args:
            carrier: dictionary in which to get value
            key: the key used to get the value
        Returns:
            A list with a single string with the value if it exists, else None.
        """
        val = carrier.get(key, None)
        if val is None:
            return None
        if val.get("Value") is not None:
            return [val.get("Value")]
        return None

    def keys(
        self, carrier: typing.Mapping[str, textmap.CarrierValT]
    ) -> typing.List[str]:
        """Keys implementation that returns all keys from a dictionary."""
        return list(carrier.keys())
