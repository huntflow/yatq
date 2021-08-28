import json

import pytest

from yatq.dto import Task
from yatq.worker.job.simple import SimpleJob


def test_missing_value_simple_job_creation():
    task = Task(
        "test",
        timeout=0,
        encoded_data=json.dumps({}),
    )

    class Job(SimpleJob):
        async def run(self, **kwargs):
            pass

    with pytest.raises(ValueError):
        Job(task)
