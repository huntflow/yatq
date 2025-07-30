from contextvars import ContextVar

JOB_ID: ContextVar = ContextVar("JOB_ID")
JOB_HANDLER: ContextVar = ContextVar("JOB_HANDLER")
