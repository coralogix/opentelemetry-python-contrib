# Copyright The OpenTelemetry Authors
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
Instrument `Botocore`_ to trace service requests.

There are two options for instrumenting code. The first option is to use the
``opentelemetry-instrument`` executable which will automatically
instrument your Botocore client. The second is to programmatically enable
instrumentation via the following code:

.. _Botocore: https://pypi.org/project/botocore/

Usage
-----

.. code:: python

    from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
    import botocore


    # Instrument Botocore
    BotocoreInstrumentor().instrument()

    # This will create a span with Botocore-specific attributes
    session = botocore.session.get_session()
    session.set_credentials(
        access_key="access-key", secret_key="secret-key"
    )
    ec2 = self.session.create_client("ec2", region_name="us-west-2")
    ec2.describe_instances()

API
---

The `instrument` method accepts the following keyword args:

tracer_provider (TracerProvider) - an optional tracer provider
request_hook (Callable) - a function with extra user-defined logic to be performed before performing the request
this function signature is:  def request_hook(span: Span, service_name: str, operation_name: str, api_params: dict) -> None
response_hook (Callable) - a function with extra user-defined logic to be performed after performing the request
this function signature is:  def request_hook(span: Span, service_name: str, operation_name: str, result: dict) -> None

for example:

.. code: python

    from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
    import botocore

    def request_hook(span, service_name, operation_name, api_params):
        # request hook logic

    def response_hook(span, service_name, operation_name, result):
        # response hook logic

    # Instrument Botocore with hooks
    BotocoreInstrumentor().instrument(request_hook=request_hook, response_hook=response_hook)

    # This will create a span with Botocore-specific attributes, including custom attributes added from the hooks
    session = botocore.session.get_session()
    session.set_credentials(
        access_key="access-key", secret_key="secret-key"
    )
    ec2 = self.session.create_client("ec2", region_name="us-west-2")
    ec2.describe_instances()
"""

import logging
import json
import io
import os
from opentelemetry.instrumentation.botocore.utils import limit_string_size, get_payload_size_limit
from typing import Any, Callable, Collection, Dict, Optional, Tuple

from botocore.client import BaseClient
from botocore.endpoint import Endpoint
from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from wrapt import wrap_function_wrapper

from opentelemetry import context as context_api

# FIXME: fix the importing of this private attribute when the location of the _SUPPRESS_HTTP_INSTRUMENTATION_KEY is defined.
from opentelemetry.context import _SUPPRESS_HTTP_INSTRUMENTATION_KEY
from opentelemetry.instrumentation.botocore.extensions import _find_extension
from opentelemetry.instrumentation.botocore.extensions.types import (
    _AwsSdkCallContext,
)
from opentelemetry.instrumentation.botocore.package import _instruments
from opentelemetry.instrumentation.botocore.version import __version__
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import (
    _SUPPRESS_INSTRUMENTATION_KEY,
    unwrap,
)
from opentelemetry.propagate import inject
from opentelemetry.propagators import textmap
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import get_tracer, SpanKind
from opentelemetry.trace.span import Span
import base64
import typing
logger = logging.getLogger(__name__)


# pylint: disable=unused-argument
def _patched_endpoint_prepare_request(wrapped, instance, args, kwargs):
    request = args[0]
    headers = request.headers
    inject(headers)

    return wrapped(*args, **kwargs)


class BotocoreInstrumentor(BaseInstrumentor):
    """An instrumentor for Botocore.

    See `BaseInstrumentor`
    """

    def __init__(self):
        super().__init__()
        self.request_hook = None
        self.response_hook = None

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs):
        # pylint: disable=attribute-defined-outside-init
        self._tracer = get_tracer(
            __name__, __version__, kwargs.get("tracer_provider")
        )

        self.request_hook = kwargs.get("request_hook")
        self.response_hook = kwargs.get("response_hook")
        wrap_function_wrapper(
            "botocore.client",
            "BaseClient._make_api_call",
            self._patched_api_call,
        )

        wrap_function_wrapper(
            "botocore.endpoint",
            "Endpoint.prepare_request",
            _patched_endpoint_prepare_request,
        )

    def _uninstrument(self, **kwargs):
        unwrap(BaseClient, "_make_api_call")
        unwrap(Endpoint, "prepare_request")

    # pylint: disable=too-many-branches
    def _patched_api_call(self, original_func, instance, args, kwargs):
        if context_api.get_value(_SUPPRESS_INSTRUMENTATION_KEY):
            return original_func(*args, **kwargs)

        call_context = _determine_call_context(instance, args)
        if call_context is None:
            return original_func(*args, **kwargs)

        extension = _find_extension(call_context)
        if not extension.should_trace_service_call():
            return original_func(*args, **kwargs)
        attributes = {
            SpanAttributes.RPC_SYSTEM: "aws-api",
            SpanAttributes.RPC_SERVICE: call_context.service_id,
            SpanAttributes.RPC_METHOD: call_context.operation,
            # TODO: update when semantic conventions exist
            "aws.region": call_context.region,
        }
        try:
            if call_context.operation == "ListObjects":
                bucket = call_context.params.get("Bucket")
                if bucket is not None:
                    attributes["rpc.request.payload"] = limit_string_size(bucket)
            elif call_context.operation == "PutObject":
                body = call_context.params.get("Body")
                if body is not None:
                    attributes["rpc.request.payload"] = limit_string_size(body.decode('ascii'))
            elif call_context.operation == "PutItem":
                body = call_context.params.get("Item")
                if body is not None:
                    attributes["rpc.request.payload"] = limit_string_size(json.dumps(body, default=str))
            elif call_context.operation == "GetItem":
                body = call_context.params.get("Key")
                if body is not None:
                    attributes["rpc.request.payload"] = limit_string_size(json.dumps(body, default=str))
            elif call_context.operation == "Publish":
                body = call_context.params.get("Message")
                if body is not None:
                    attributes["rpc.request.payload"] = limit_string_size(json.dumps(body, default=str))
            elif call_context.service == "events" and call_context.operation == "PutEvents":
                call_context.span_kind = SpanKind.PRODUCER
                attributes["rpc.request.payload"] = limit_string_size(json.dumps(call_context.params, default=str))
            elif call_context.service == "kinesis" and (call_context.operation == "PutRecord" or call_context.operation == "PutRecords"):
                call_context.span_kind = SpanKind.PRODUCER
                attributes["rpc.request.payload"] = limit_string_size(json.dumps(call_context.params, default=str))
            else:
                attributes["rpc.request.payload"] = limit_string_size(json.dumps(call_context.params, default=str))
        except Exception as ex:
            pass

        _safe_invoke(extension.extract_attributes, attributes)

        with self._tracer.start_as_current_span(
            call_context.span_name,
            kind=call_context.span_kind,
            attributes=attributes,
        ) as span:
            _safe_invoke(extension.before_service_call, span)
            self._call_request_hook(span, call_context)

            token = context_api.attach(
                context_api.set_value(_SUPPRESS_HTTP_INSTRUMENTATION_KEY, True)
            )

            try:
                if call_context.service == "lambda" and call_context.operation == "Invoke":
                    if args[1].get("ClientContext") is not None:
                        ctx = base64.b64decode(args[1].get("ClientContext")).decode('ascii')
                        inject(ctx['custom'])
                        jctx = json.dumps(ctx)
                        args[1]['ClientContext'] = base64.b64encode(jctx.encode('ascii')).decode('ascii')
                    else:
                        ctx = {'custom': {}}
                        inject(ctx['custom'])
                        jctx = json.dumps(ctx)
                        args[1]['ClientContext'] = base64.b64encode(jctx.encode('ascii')).decode('ascii')

            except Exception as ex:
                pass

            try:
                if call_context.service == "sqs" and call_context.operation == "SendMessage":
                    if args[1].get("MessageAttributes") is not None:
                        inject(carrier = args[1].get("MessageAttributes"), setter=SQSSetter())
                    else:
                        args[1]['MessageAttributes'] = {}
                        inject(carrier = args[1].get("MessageAttributes"), setter=SQSSetter())
                
                if call_context.service == "sqs" and call_context.operation == "SendMessageBatch":
                    if args[1].get("Entries") is not None:
                        for entry in args[1].get("Entries"):
                            if entry.get("MessageAttributes") is not None:
                                inject(carrier = entry.get("MessageAttributes"), setter=SQSSetter())
                            else:
                                entry['MessageAttributes'] = {}
                                inject(carrier = entry.get("MessageAttributes"), setter=SQSSetter())
                            
            except Exception as ex:
                pass

            try:
                if call_context.service == "events" and call_context.operation == "PutEvents":
                    if args[1].get("Entries") is not None:
                        for entry in args[1].get("Entries"):
                            if entry.get("Detail") is not None:
                                detailJson = json.loads(entry.get("Detail"))
                                detailJson['_context'] = {}
                                inject(carrier = detailJson['_context'])
                                entry['Detail'] = json.dumps(detailJson)
                            else:
                                detailJson = {'_context': {}}
                                inject(carrier = detailJson['_context'])
                                entry['Detail'] = json.dumps(detailJson)
            except Exception as ex:
                pass

            try:
                if call_context.service == "kinesis" and call_context.operation == "PutRecord":
                    if args[1].get("Data") is not None:
                        detailJson = json.loads(args[1].get("Data"))
                        detailJson['_context'] = {}
                        inject(carrier = detailJson['_context'])
                        args[1]["Data"] = json.dumps(detailJson)
            except Exception as e:
                pass

            result = None
            try:
                result = original_func(*args, **kwargs)
            except ClientError as error:
                result = getattr(error, "response", None)
                _apply_response_attributes(span, result)
                _safe_invoke(extension.on_error, span, error)
                raise
            else:
                _apply_response_attributes(span, result)
                _safe_invoke(extension.on_success, span, result)
            finally:
                context_api.detach(token)
                _safe_invoke(extension.after_service_call)

                self._call_response_hook(span, call_context, result)

            return result

    def _call_request_hook(self, span: Span, call_context: _AwsSdkCallContext):
        if not callable(self.request_hook):
            return
        self.request_hook(
            span,
            call_context.service,
            call_context.operation,
            call_context.params,
        )

    def _call_response_hook(
        self, span: Span, call_context: _AwsSdkCallContext, result
    ):
        if not callable(self.response_hook):
            return
        self.response_hook(
            span, call_context.service, call_context.operation, result
        )


def _apply_response_attributes(span: Span, result):
    if result is None or not span.is_recording():
        return

    metadata = result.get("ResponseMetadata")
    if metadata is None:
        return

    request_id = metadata.get("RequestId")
    if request_id is None:
        headers = metadata.get("HTTPHeaders")
        if headers is not None:
            request_id = (
                headers.get("x-amzn-RequestId")
                or headers.get("x-amz-request-id")
                or headers.get("x-amz-id-2")
            )
    if request_id:
        # TODO: update when semantic conventions exist
        span.set_attribute("aws.request_id", request_id)

    retry_attempts = metadata.get("RetryAttempts")
    if retry_attempts is not None:
        # TODO: update when semantic conventions exists
        span.set_attribute("retry_attempts", retry_attempts)

    status_code = metadata.get("HTTPStatusCode")
    if status_code is not None:
        span.set_attribute(SpanAttributes.HTTP_STATUS_CODE, status_code)

    #print("result")
    #print(json.dumps(result, indent=4, sort_keys=True, default=str))
    #print(json.dumps(metadata, indent=4, sort_keys=True, default=str))
    try:
        headers = metadata.get("HTTPHeaders")
        if headers is not None:
            server = headers.get("server")
            if server == "AmazonS3":
                buckets = result.get("Buckets")
                content = result.get("Contents")
                body = result.get("Body")
                if buckets is not None:
                    span.set_attribute(
                        "rpc.response.payload", limit_string_size(json.dumps([b.get("Name") for b in buckets])))
                elif content is not None:
                    span.set_attribute(
                        "rpc.response.payload", limit_string_size(json.dumps([b.get("Key") for b in content])))
                elif body is not None:
                    pass
                else:
                    span.set_attribute(
                        "rpc.response.payload", limit_string_size(json.dumps(result, default=str)))
                #elif body is not None:
                #    try:
                #        d = {x: result[x] for x in result if x != "Body"}
                #        payload = copy.deepcopy(d)
                #        if body._content_length is not None and int(body._content_length) < payload_size_limit:
                #            strbody = body.read()
                #            payload['Body'] = strbody1
                #            body.close()
                #            result['Body'] = StreamingBody(io.BytesIO(strbody),
                #                                           content_length=body._content_length)
                #        else:
                #            payload['Body'] = ''
                #        span.set_attribute(
                #            "rpc.response.payload", json.dumps(payload, default=str))
                #    except Exception as ex:
                #        pass
            # Lambda Invoke
            elif result.get("Payload") is not None and result.get("Payload")._content_length is not None and int(result.get("Payload")._content_length) < get_payload_size_limit():
                length = result.get("Payload")._content_length
                strbody = result.get("Payload").read()
                result.get("Payload").close()
                span.set_attribute(
                    "rpc.response.payload", limit_string_size(strbody))
                result['Payload'] = StreamingBody(io.BytesIO(strbody), content_length=length)
            # DynamoDB get item
            elif server == "Server":
                span.set_attribute(
                    "rpc.response.payload", limit_string_size(json.dumps(result, default=str)))
            else:
                span.set_attribute(
                    "rpc.response.payload", limit_string_size(json.dumps(result, default=str)))
    except Exception as ex:
        pass


def _determine_call_context(
    client: BaseClient, args: Tuple[str, Dict[str, Any]]
) -> Optional[_AwsSdkCallContext]:
    try:

        call_context = _AwsSdkCallContext(client, args)

        logger.debug(
            "AWS SDK invocation: %s %s",
            call_context.service,
            call_context.operation,
        )

        return call_context
    except Exception as ex:  # pylint:disable=broad-except
        # this shouldn't happen actually unless internals of botocore changed and
        # extracting essential attributes ('service' and 'operation') failed.
        logger.error("Error when initializing call context", exc_info=ex)
        return None


def _safe_invoke(function: Callable, *args):
    function_name = "<unknown>"
    try:
        function_name = function.__name__
        function(*args)
    except Exception as ex:  # pylint:disable=broad-except
        logger.error(
            "Error when invoking function '%s'", function_name, exc_info=ex
        )

class SQSSetter():
    def set(
        self,
        carrier: typing.MutableMapping[str, textmap.CarrierValT],
        key: str,
        value: textmap.CarrierValT,
    ) -> None:
        """Setter implementation to set a value into a dictionary.

        Args:
            carrier: dictionary in which to set value
            key: the key used to set the value
            value: the value to set
        """
        val = {"DataType": "String", "StringValue": value}
        carrier[key] = val
