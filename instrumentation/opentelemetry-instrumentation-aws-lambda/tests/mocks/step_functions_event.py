"""
Step Functions event for AWS Lambda integration testing
"""

MOCK_LAMBDA_STEP_FUNCTIONS_EVENT = {
    "executionContext": {
        "Id": "arn:aws:states:us-east-1:123456789012:execution:TestStateMachine:test-execution-123",
        "Name": "test-execution-123",
        "RoleArn": "arn:aws:iam::123456789012:role/StepFunctionsExecutionRole",
        "StartTime": "2025-10-15T08:30:00.000Z",
        "RedriveCount": 0
    },
    "stateContext": {
        "Name": "InvokeLambda",
        "EnteredTime": "2025-10-15T08:30:00.000Z",
        "RetryCount": 0
    },
    "stateMachineContext": {
        "Id": "arn:aws:states:us-east-1:123456789012:stateMachine:TestStateMachine",
        "Name": "TestStateMachine"
    }
}