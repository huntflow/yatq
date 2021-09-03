DEFAULT_QUEUE_NAME = "default"
DEFAULT_QUEUE_NAMESPACE = "tasks"
DEFAULT_TIMEOUT = 5 * 60
DEFAULT_TASK_EXPIRATION = 60 * 60 * 12
DEFAULT_LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
        },
    },
    "root": {"level": "INFO", "handlers": ["console"]},
}
DEFAULT_MAX_JOBS = 8
