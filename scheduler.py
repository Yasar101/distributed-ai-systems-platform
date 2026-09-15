"""In-memory reference scheduler with leases, retries, and worker-safe locking."""

from __future__ import annotations

from dataclasses import dataclass, replace
from copy import deepcopy
import math
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
        if type(max_attempts) is not int or max_attempts < 1:
            raise ValueError("max_attempts must be a positive integer")
        self.max_attempts, self.clock = max_attempts, clock
        self._jobs: dict[str, Job] = {}
        self._lock = Lock()

    def submit(self, payload: dict[str, object]) -> Job:
        job = Job(str(uuid4()), deepcopy(payload))
        with self._lock:
            self._jobs[job.id] = job
        return deepcopy(job)

    def claim(self, worker_id: str, lease_seconds: float = 30) -> Job | None:
        if not worker_id or not math.isfinite(lease_seconds) or lease_seconds <= 0:
            raise ValueError("worker and positive lease are required")
        with self._lock:
            now = self.clock()
            self._expire_exhausted(now)
            for job in self._jobs.values():
                available = job.state == JobState.QUEUED or (job.state == JobState.RUNNING and job.lease_until <= now)
                if available and job.attempts < self.max_attempts:
                    claimed = replace(job, state=JobState.RUNNING, attempts=job.attempts + 1, lease_until=now + lease_seconds, worker_id=worker_id)
                    self._jobs[job.id] = claimed
                    return deepcopy(claimed)
        return None

    def finish(self, job_id: str, worker_id: str, succeeded: bool) -> Job:
        with self._lock:
            job = self._jobs[job_id]
            if (job.state != JobState.RUNNING or job.worker_id != worker_id
                    or job.lease_until <= self.clock()):
                raise ValueError("worker does not own this running job")
            state = JobState.SUCCEEDED if succeeded else (JobState.FAILED if job.attempts >= self.max_attempts else JobState.QUEUED)
            updated = replace(job, state=state, lease_until=0, worker_id="")
            self._jobs[job_id] = updated
            return deepcopy(updated)

    def _expire_exhausted(self, now: float) -> None:
        """Called with the lock held; expired final attempts cannot strand jobs."""
        for job in self._jobs.values():
            if (job.state == JobState.RUNNING and job.lease_until <= now
                    and job.attempts >= self.max_attempts):
                self._jobs[job.id] = replace(job, state=JobState.FAILED,
                                             worker_id="", lease_until=0)

    def get(self, job_id: str) -> Job:
        with self._lock:
            self._expire_exhausted(self.clock())
            return deepcopy(self._jobs[job_id])


def main() -> None:
    """Demonstrate an in-process job claim and completion lifecycle."""
    import argparse

    parser = argparse.ArgumentParser(description="Run a local distributed-job scheduler demonstration.")
    parser.add_argument("--worker", default="demo-worker")
    parser.add_argument("--fail", action="store_true", help="return the claimed job to the queue")
    args = parser.parse_args()
    scheduler = JobScheduler(max_attempts=2)
    submitted = scheduler.submit({"model": "demo-small", "data": "fictional"})
    claimed = scheduler.claim(args.worker, lease_seconds=30)
    assert claimed is not None
    finished = scheduler.finish(claimed.id, args.worker, not args.fail)
    print("Infrastructure: in-process scheduler; worker is simulated.")
    print(f"Submitted: {submitted.id} ({submitted.state.value})")
    print(f"Claimed by {args.worker}; attempt {claimed.attempts}; lease seconds: 30")
    print(f"Final state: {finished.state.value}")
    if args.fail:
        print("Failure remains retryable until the configured attempt limit is reached.")


if __name__ == "__main__":
    main()
