"""
This module provides a set of attributes for AWS Lambda functions.
"""

AWS_EVENT_BRIDGE_TRIGGER_SOURCE = "aws.event.bridge.trigger.source"

FAAS_TRIGGER_TYPE = "faas.trigger.type"

HTTP_HEADER_PREFIX = "http.request.header."
HTTP_RESPONSE_BODY = "http.response.body"

MESSAGING_MESSAGE = "messaging.message"

RPC_REQUEST_BODY = "rpc.request.body"
RPC_RESPONSE_BODY = "rpc.response.body"

# Step Functions specific attributes
AWS_STEP_FUNCTIONS_EXECUTION_ARN = "aws.stepfunctions.execution.arn"
AWS_STEP_FUNCTIONS_EXECUTION_NAME = "aws.stepfunctions.execution.name"
AWS_STEP_FUNCTIONS_EXECUTION_TYPE = "aws.stepfunctions.execution.type"
AWS_STEP_FUNCTIONS_ROLE_ARN = "aws.stepfunctions.role.arn"
AWS_STEP_FUNCTIONS_EXECUTION_START_TIME = "aws.stepfunctions.execution.start_time"
AWS_STEP_FUNCTIONS_EXECUTION_REDRIVE_COUNT = "aws.stepfunctions.execution.redrive_count"
AWS_STEP_FUNCTIONS_STATE_NAME = "aws.stepfunctions.state.name"
AWS_STEP_FUNCTIONS_STATE_ENTERED_TIME = "aws.stepfunctions.state.entered_time"
AWS_STEP_FUNCTIONS_STATE_RETRY_COUNT = "aws.stepfunctions.state.retry_count"
AWS_STEP_FUNCTIONS_STATE_MACHINE_ARN = "aws.stepfunctions.state_machine.arn"
AWS_STEP_FUNCTIONS_STATE_MACHINE_NAME = "aws.stepfunctions.state_machine.name"
