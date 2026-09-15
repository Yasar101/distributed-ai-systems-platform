# Worker Lease Scheduler Core

**TESTED CORE** · [Live demo](https://yasar101.github.io/software-engineering-portfolio/demos/scheduler.html) · [Portfolio](https://github.com/Yasar101/software-engineering-portfolio)

An in-memory job scheduler modelling claims, expiring leases and bounded retries.

## Purpose and engineering skills

Explore ownership, worker failure and terminal-state behavior before choosing a queue or database.

## Structure

scheduler.py uses immutable Job records, defensive payload copies and a process-local lock; the clock is injectable. [Source](scheduler.py).

## Run

From this directory with Python 3.11+, run this offline example using `python3` (no dependencies or credentials):

```python
from scheduler import JobScheduler, JobState
scheduler = JobScheduler(max_attempts=2)
job = scheduler.submit({"model": "demo"})
claim = scheduler.claim("worker-instance-1", lease_seconds=30)
assert claim.id == job.id
assert scheduler.finish(job.id, "worker-instance-1", True).state == JobState.SUCCEEDED
```

## Test

```sh
python3 -m compileall -q .
python3 - <<'PY'
from scheduler import JobScheduler, JobState
now = [10.0]
scheduler = JobScheduler(max_attempts=1, clock=lambda: now[0])
job = scheduler.submit({})
assert scheduler.claim("w-1", 5).id == job.id
now[0] = 16.0
assert scheduler.claim("w-2", 5) is None
assert scheduler.get(job.id).state == JobState.FAILED
print("lease-expiry example passed")
PY
```

The full regression suite (failure paths, README examples and demo checks) runs in the [portfolio repository](https://github.com/Yasar101/software-engineering-portfolio).

## Complete and remaining

**Complete:** Submission, claim/finish, ownership and expiry checks, retry limits and exhausted-expiry transition to FAILED on get/claim. Tests use a deterministic clock.

**Remaining / limitations:** Single process only: no durable queue, network workers, fencing token, heartbeat or exactly-once delivery. Use a unique worker ID for each execution attempt; reused IDs cannot distinguish stale attempts. No AI inference engine is included.

## Learning takeaway

A lease needs expiry and exhaustion semantics; a process-local lock is not a distributed consistency mechanism.

## Command-line demonstration
Run an explicit local lifecycle demonstration (workers and storage are simulated in-process):

```bash
python3 -m scheduler
python3 -m scheduler --fail
```

This is not deployed distributed infrastructure.