"""Errors raised by the engine, and errors that workflows and activities raise or catch."""

from __future__ import annotations


class DurableFlowError(Exception):
    pass


class NotFound(DurableFlowError):
    pass


class NotRunning(DurableFlowError):
    pass


class NonDeterminismError(DurableFlowError):
    """Replay issued a different command than the one recorded in history."""


class ApplicationError(Exception):
    """Raise from an activity to control retries. `type` is matched against
    RetryPolicy.non_retryable, and non_retryable=True stops retries outright."""

    def __init__(self, message: str, type: str | None = None, non_retryable: bool = False):
        super().__init__(message)
        self.type = type or "ApplicationError"
        self.non_retryable = non_retryable


class ActivityError(DurableFlowError):
    """Thrown into the workflow when an activity fails for good."""

    def __init__(self, name: str, error_type: str, message: str, attempts: int):
        super().__init__(f"activity {name} failed after {attempts} attempt(s): {error_type}: {message}")
        self.name = name
        self.error_type = error_type
        self.message = message
        self.attempts = attempts


class ChildWorkflowError(DurableFlowError):
    def __init__(self, workflow_type: str, status: str, detail: object):
        super().__init__(f"child workflow {workflow_type} ended {status}: {detail}")
        self.workflow_type = workflow_type
        self.status = status
        self.detail = detail


class SignalTimeout(DurableFlowError):
    def __init__(self, name: str, timeout: float):
        super().__init__(f"no {name!r} signal within {timeout:g}s")
        self.name = name
        self.timeout = timeout


class ParallelError(DurableFlowError):
    """At least one branch of a parallel block failed. `results[i]` is the value of
    branch i or None, `errors[i]` is the exception of branch i or None."""

    def __init__(self, results: list, errors: list):
        failed = [i for i, e in enumerate(errors) if e is not None]
        super().__init__(f"{len(failed)} of {len(errors)} parallel branches failed: {errors[failed[0]]}")
        self.results = results
        self.errors = errors
