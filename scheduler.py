"""In-memory reference scheduler with leases, retries, and worker-safe locking."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from threading import Lock
import time
from uuid import uuid4


class JobState(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass(frozen=True)
class Job:
    id: str
    payload: dict[str, object]
    state: JobState = JobState.QUEUED
    attempts: int = 0
    lease_until: float = 0
    worker_id: str = ""


class JobScheduler:
    def __init__(self, max_attempts: int = 3, clock=time.monotonic) -> None:
        self.max_attempts, self.clock = max_attempts, clock
        self._jobs: dict[str, Job] = {}
        self._lock = Lock()

    def submit(self, payload: dict[str, object]) -> Job:
        job = Job(str(uuid4()), dict(payload))
        with self._lock:
            self._jobs[job.id] = job
        return job

    def claim(self, worker_id: str, lease_seconds: float = 30) -> Job | None:
        if not worker_id or lease_seconds <= 0:
            raise ValueError("worker and positive lease are required")
        now = self.clock()
        with self._lock:
            for job in self._jobs.values():
                available = job.state == JobState.QUEUED or (job.state == JobState.RUNNING and job.lease_until <= now)
                if available and job.attempts < self.max_attempts:
                    claimed = replace(job, state=JobState.RUNNING, attempts=job.attempts + 1, lease_until=now + lease_seconds, worker_id=worker_id)
                    self._jobs[job.id] = claimed
                    return claimed
        return None

    def finish(self, job_id: str, worker_id: str, succeeded: bool) -> Job:
        with self._lock:
            job = self._jobs[job_id]
            if job.state != JobState.RUNNING or job.worker_id != worker_id:
                raise ValueError("worker does not own this running job")
            state = JobState.SUCCEEDED if succeeded else (JobState.FAILED if job.attempts >= self.max_attempts else JobState.QUEUED)
            updated = replace(job, state=state, lease_until=0, worker_id="")
            self._jobs[job_id] = updated
            return updated

    def get(self, job_id: str) -> Job:
        return self._jobs[job_id]
