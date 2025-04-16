
from typing import Any, Dict, List, Callable, MutableMapping
import json
import base64
import io

from opentelemetry.propagators import textmap
from botocore.response import StreamingBody


from opentelemetry.propagate import inject
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.instrumentation.botocore.extensions.types import (
    _AwsSdkCallContext,
)
from opentelemetry.trace import SpanKind, Span
from opentelemetry.instrumentation.botocore.utils import limit_string_size, get_payload_size_limit


# pylint: disable=unused-argument
def _patched_endpoint_prepare_request(wrapped, instance, args, kwargs):
    request = args[0]
    headers = request.headers
    inject(headers)

    return wrapped(*args, **kwargs)


RPC_REQUEST_PAYLOAD = "rpc.request.payload"
RPC_RESPONSE_PAYLOAD = "rpc.response.payload"

def add_extra_attributes(call_context: _AwsSdkCallContext, attributes: Dict[str, Any]):
    """
    Add extra attributes to the span based on the call context.
    """
    if call_context.operation == "ListObjects":
        bucket = call_context.params.get("Bucket")
        if bucket:
            attributes[RPC_REQUEST_PAYLOAD] = limit_string_size(bucket)

    elif call_context.operation == "PutObject":
        body = call_context.params.get("Body")
        if body:
            if isinstance(body, bytes):
                payload = body.decode('ascii')
            else:
                payload = str(body)
            attributes[RPC_REQUEST_PAYLOAD] = limit_string_size(payload)

    elif call_context.operation == "PutItem":
        body = call_context.params.get("Item")
        if body:
            attributes[RPC_REQUEST_PAYLOAD] = limit_string_size(json.dumps(body, default=str))

    elif call_context.operation == "GetItem":
        body = call_context.params.get("Key")
        if body:
            attributes[RPC_REQUEST_PAYLOAD] = limit_string_size(json.dumps(body, default=str))

    elif call_context.operation == "Publish":
        body = call_context.params.get("Message")
        if body:
            attributes[RPC_REQUEST_PAYLOAD] = limit_string_size(json.dumps(body, default=str))

    elif call_context.service == "events" and call_context.operation == "PutEvents":
        call_context.span_kind = SpanKind.PRODUCER
        attributes[RPC_REQUEST_PAYLOAD] = limit_string_size(
            json.dumps(call_context.params, default=str)
        )

    elif call_context.service == "kinesis" and (
        call_context.operation in ["PutRecord", "PutRecords"]
    ):
        call_context.span_kind = SpanKind.PRODUCER
        stream_name = call_context.params.get("StreamName")
        if stream_name:
            attributes[SpanAttributes.MESSAGING_SYSTEM] = "aws.kinesis"
            attributes[SpanAttributes.MESSAGING_DESTINATION] = stream_name
        attributes[RPC_REQUEST_PAYLOAD] = limit_string_size(
            json.dumps(call_context.params, default=str)
        )

    elif call_context.service == "sqs" and (
        call_context.operation in ["SendMessageBatch", "SendMessage"]
    ):
        call_context.span_kind = SpanKind.PRODUCER
        attributes[RPC_REQUEST_PAYLOAD] = limit_string_size(
            json.dumps(call_context.params, default=str)
        )

    else:
        attributes[RPC_REQUEST_PAYLOAD] = limit_string_size(
            json.dumps(call_context.params, default=str)
        )


def lambda_extra_context(call_context: _AwsSdkCallContext, args: List[Any]):
    """
    Add extra context for lambda function.
    """
    if call_context.operation == "Invoke":
        if args[1].get("ClientContext"):
            ctx = base64.b64decode(args[1].get("ClientContext")).decode('ascii')
            inject(ctx['custom'])
            jctx = json.dumps(ctx)
            args[1]['ClientContext'] = base64.b64encode(jctx.encode('ascii')).decode('ascii')
        else:
            ctx = {'custom': {}}
            inject(ctx['custom'])
            jctx = json.dumps(ctx)
            args[1]['ClientContext'] = base64.b64encode(jctx.encode('ascii')).decode('ascii')

def sqs_extra_context(call_context: _AwsSdkCallContext, args: List[Any]):
    """
    Add extra context for sqs function.
    """
    if call_context.operation == "SendMessage":
        if args[1].get("MessageAttributes"):
            inject(carrier = args[1].get("MessageAttributes"), setter=SQSSetter())
        else:
            args[1]['MessageAttributes'] = {}
            inject(carrier = args[1].get("MessageAttributes"), setter=SQSSetter())

    elif call_context.operation == "SendMessageBatch":
        if args[1].get("Entries"):
            for entry in args[1].get("Entries"):
                if entry.get("MessageAttributes"):
                    inject(carrier = entry.get("MessageAttributes"), setter=SQSSetter())
                else:
                    entry['MessageAttributes'] = {}
                    inject(carrier = entry.get("MessageAttributes"), setter=SQSSetter())


def event_extra_context(call_context: _AwsSdkCallContext, args: List[Any]):
    """
    Add extra context for event function.
    """
    if call_context.operation != "PutEvents":
        return

    if not args[1].get("Entries"):
        return

    for entry in args[1].get("Entries"):
        if entry.get("Detail"):
            detail_json = json.loads(entry.get("Detail"))
            detail_json['_context'] = {}
            inject(carrier = detail_json['_context'])
            entry['Detail'] = json.dumps(detail_json)
        else:
            detail_json = {'_context': {}}
            inject(carrier = detail_json['_context'])
            entry['Detail'] = json.dumps(detail_json)


def kinesis_extra_context(call_context: _AwsSdkCallContext, args: List[Any]):
    """
    Add extra context for kinesis function.
    """
    if call_context.operation == "PutRecord":
        if args[1].get("Data") is not None:
            detail_json = json.loads(args[1].get("Data"))
            detail_json['_context'] = {}
            inject(carrier = detail_json['_context'])
            args[1]["Data"] = json.dumps(detail_json)

    elif call_context.operation == "PutRecords":
        if args[1].get("Records") is not None:
            for entry in args[1].get("Records"):
                if entry.get("Data") is not None:
                    detail_json = json.loads(entry.get("Data"))
                    detail_json['_context'] = {}
                    inject(carrier = detail_json['_context'])
                    entry['Data'] = json.dumps(detail_json)
                else:
                    detail_json = {'_context': {}}
                    inject(carrier = detail_json['_context'])
                    entry['Data'] = json.dumps(detail_json)

def add_extra_context(call_context: _AwsSdkCallContext, args: List[Any]):
    """
    Add extra context to the span based on the call context.
    """
    if len(args) == 0:
        return

    services: Dict[str, Callable] = {
        "lambda": lambda_extra_context,
        "sqs": sqs_extra_context,
        "events": event_extra_context,
        "kinesis": kinesis_extra_context,
    }

    for service, extra_context_f in services.items():
        if call_context.service == service:
            extra_context_f(call_context, args)
            return

class SQSSetter():
    """
    Setter implementation to set a value into a dictionary.
    """

    def set(
        self,
        carrier: MutableMapping[str, textmap.CarrierValT],
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

def add_extra_attributes_after_call(metadata: Dict[str, Any], result: Dict[str, Any], span: Span):
    """
    Add extra attributes to the span after the call.
    """
    headers = metadata.get("HTTPHeaders")

    if not headers:
        return

    server = headers.get("server")

    if server == "AmazonS3":
        buckets = result.get("Buckets")
        content = result.get("Contents")
        body = result.get("Body")
        if buckets:
            payload = limit_string_size(json.dumps([b.get("Name") for b in buckets]))
            span.set_attribute(RPC_RESPONSE_PAYLOAD, payload)
        elif content:
            payload = limit_string_size(json.dumps([b.get("Key") for b in content]))
            span.set_attribute(RPC_RESPONSE_PAYLOAD, payload)
        elif body is not None:
            pass # TODO: handle body
        else:
            payload = limit_string_size(json.dumps(result, default=str))
            span.set_attribute(RPC_RESPONSE_PAYLOAD, payload)
    # Lambda Invoke
    elif result.get("Payload"):
        invoke_payload = result.get("Payload")
        length = invoke_payload._content_length # pylint: disable=protected-access
        if length and int(length) < get_payload_size_limit():
            strbody = invoke_payload.read()
            invoke_payload.close()
            payload = limit_string_size(strbody)
            span.set_attribute(RPC_RESPONSE_PAYLOAD, payload)
            result['Payload'] = StreamingBody(io.BytesIO(strbody), content_length=length)
    else:
        payload = limit_string_size(json.dumps(result, default=str))
        span.set_attribute(RPC_RESPONSE_PAYLOAD, payload)
