"""
This module provides functionality for creating OpenTelemetry spans for AWS Step Functions events.
"""

from typing import Optional, Tuple
import re

from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.semconv.resource import ResourceAttributes
from opentelemetry.trace import set_span_in_context, Span, SpanKind
from opentelemetry.context.context import Context
from opentelemetry.instrumentation.aws_lambda.utils import limit_string_size

from opentelemetry.instrumentation.aws_lambda.coralogix.span import CoralogixSpan
from opentelemetry.instrumentation.aws_lambda.coralogix import attributes as cx_attributes


class StepFunctionsSpan(CoralogixSpan):
    """
    Span for AWS Step Functions events.
    """

    def is_applicable(self) -> bool:
        """
        Check if the event is a Step Functions event by validating the presence of
        required context objects.
        """
        # Check for executionContext
        execution_context = self.lambda_event.get("executionContext")
        if not execution_context or not isinstance(execution_context, dict):
            return False
        
        # Validate required execution context fields
        required_execution_fields = ["Id", "Name", "RoleArn", "StartTime"]
        if not all(field in execution_context for field in required_execution_fields):
            return False
        
        # Check for stateContext
        state_context = self.lambda_event.get("stateContext")
        if not state_context or not isinstance(state_context, dict):
            return False
        
        # Validate required state context fields
        required_state_fields = ["Name", "EnteredTime", "RetryCount"]
        if not all(field in state_context for field in required_state_fields):
            return False
        
        # Check for stateMachineContext
        state_machine_context = self.lambda_event.get("stateMachineContext")
        if not state_machine_context or not isinstance(state_machine_context, dict):
            return False
        
        # Validate required state machine context fields
        required_state_machine_fields = ["Id", "Name"]
        if not all(field in state_machine_context for field in required_state_machine_fields):
            return False
        
        return True

    def before_run(self) -> Tuple[Span, Context]:
        """
        Create the span and context for the Step Functions event.
        """
        execution_context = self.lambda_event.get("executionContext", {})
        state_context = self.lambda_event.get("stateContext", {})
        state_machine_context = self.lambda_event.get("stateMachineContext", {})
        
        # Extract information from execution ARN
        execution_arn = execution_context.get("Id", "")
        arn_parts = execution_arn.split(":")
        
        # Parse ARN: arn:aws:states:region:account:express:StateMachineName:executionName:taskToken
        region = arn_parts[3] if len(arn_parts) > 3 else "unknown"
        account_id = arn_parts[4] if len(arn_parts) > 4 else "unknown"
        execution_type = arn_parts[5] if len(arn_parts) > 5 else "unknown"  # 'express' or 'standard'
        state_machine_name = arn_parts[6] if len(arn_parts) > 6 else "unknown"
        
        # Get span name (use state machine name)
        span_name = state_machine_name
        
        # Create the span
        self.generated_span = self.tracer.start_span(
            span_name,
            context=self.parent_context,
            kind=SpanKind.SERVER
        )
        
        # Set core FaaS attributes
        self.generated_span.set_attribute(SpanAttributes.FAAS_TRIGGER, "stepfunctions")
        self.generated_span.set_attribute(cx_attributes.FAAS_TRIGGER_TYPE, "Step Functions")
        
        # Set AWS Step Functions execution attributes
        self.generated_span.set_attribute(cx_attributes.AWS_STEP_FUNCTIONS_EXECUTION_ARN, execution_arn)
        self.generated_span.set_attribute(cx_attributes.AWS_STEP_FUNCTIONS_EXECUTION_NAME, execution_context.get("Name", ""))
        self.generated_span.set_attribute(cx_attributes.AWS_STEP_FUNCTIONS_EXECUTION_TYPE, execution_type)
        self.generated_span.set_attribute(cx_attributes.AWS_STEP_FUNCTIONS_ROLE_ARN, execution_context.get("RoleArn", ""))
        self.generated_span.set_attribute(cx_attributes.AWS_STEP_FUNCTIONS_EXECUTION_START_TIME, execution_context.get("StartTime", ""))
        self.generated_span.set_attribute(cx_attributes.AWS_STEP_FUNCTIONS_EXECUTION_REDRIVE_COUNT, execution_context.get("RedriveCount", 0))
        
        # Set cloud context attributes
        self.generated_span.set_attribute(ResourceAttributes.CLOUD_REGION, region)
        self.generated_span.set_attribute(ResourceAttributes.CLOUD_ACCOUNT_ID, account_id)
        
        # Set state context attributes
        self.generated_span.set_attribute(cx_attributes.AWS_STEP_FUNCTIONS_STATE_NAME, state_context.get("Name", ""))
        self.generated_span.set_attribute(cx_attributes.AWS_STEP_FUNCTIONS_STATE_ENTERED_TIME, state_context.get("EnteredTime", ""))
        self.generated_span.set_attribute(cx_attributes.AWS_STEP_FUNCTIONS_STATE_RETRY_COUNT, state_context.get("RetryCount", 0))
        
        # Set state machine context attributes
        self.generated_span.set_attribute(cx_attributes.AWS_STEP_FUNCTIONS_STATE_MACHINE_ARN, state_machine_context.get("Id", ""))
        self.generated_span.set_attribute(cx_attributes.AWS_STEP_FUNCTIONS_STATE_MACHINE_NAME, state_machine_name)
        
        # Set input data if available
        input_data = self.lambda_event.get("input")
        if input_data is not None:
            self.set_rpc_request_body(input_data)
        
        return self.generated_span, set_span_in_context(self.generated_span)

    def get_child_span_type(self) -> SpanKind:
        """
        Get the child span type for Step Functions events.
        """
        return SpanKind.SERVER
