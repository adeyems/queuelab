# QueueLab

An offline, virtual-time retry simulator for developers designing background-job policies. Explore synthetic failures, retry timing and exhausted jobs without running a broker or waiting for timers. Python 3.10+ standard library; no dependencies.

## Run

```sh
python3 queuelab.py examples/retries.json
python3 queuelab.py examples/workers.json
python3 -m unittest discover -s tests -v
```

The example intentionally exits **1**: two jobs succeed and one exhausts its attempts. Its seven total attempts finish at virtual time 350 ms. Exit **0** means all jobs succeeded; **1** means at least one was dead-lettered; **2** means invalid input or an input-file error. Reports are JSON on stdout; input errors are JSON on stderr. No files are modified.

## Scenario version 1

See `examples/retries.json`. Required top-level fields are `version`, `retry`, and `jobs`; optional `workers` sets the worker count (integer 1–1,000, default 1). Unknown fields are rejected. Retry policy requires `max_attempts` (1–100, including the initial attempt), `backoff` (`fixed` or `exponential`), and `delay_ms` (integer 0–1,000,000,000).

Each job has a unique ASCII `id` (1–64 letters, digits, hyphens or underscores), `failures_before_success` (0–100), and optional `enqueue_ms` and `duration_ms` (each default 0, integer 0–1,000,000,000). Duration applies to every attempt, successful or failed. A job fails that many initial attempts, then succeeds if its retry allowance permits. Files are limited to 1 MiB and 1–1,000 jobs. Booleans are not accepted as integers; duplicate JSON keys are rejected.

Each attempt occupies one worker for its duration, without preemption. After failed attempt n finishes, fixed backoff waits `delay_ms`; exponential waits `delay_ms * 2**(n-1)`. Workers are free during backoff, and a ready retry may wait longer for capacity. There is no jitter or delay cap. Python integer timestamps preserve large values exactly; consumers using JavaScript numbers must handle values beyond their safe integer range.

Ready attempts are dispatched by ready time, then scheduling insertion order, to the lowest-numbered idle worker. Initial arrivals are inserted in input order before any retries. A retry therefore follows already-scheduled arrivals at the same ready time, including with zero delay. At a timestamp, all pending completions are processed before further dispatch; completion ties use dispatch order. A zero-duration attempt completes before the next dispatch at that timestamp. Job summaries preserve input order. Identical scenarios produce identical reports; no randomness, wall-clock timestamps, networking or real sleeps are used.

Each event reports a completed attempt: `time_ms` is completion time, `started_ms` is actual start, `ready_ms` is arrival or retry eligibility, `wait_ms` is time waiting for a worker, and `worker_id` identifies the occupied worker (numbered from 1). `duration_ms` is elapsed execution time. `next_attempt_ms` indicates retry eligibility, not a guaranteed start. Events are in completion order; summary `finished_ms` is the last completion. Existing zero-duration scenarios retain their timing and outcomes; event records gain these timing fields.

The worker example runs three jobs with two workers: the slow job runs from 0–30 ms, the transient job fails at 10 ms, and the waiting job uses that freed worker from 10–25 ms. The retry becomes ready at 15 ms but starts at 25 ms and finishes at 35 ms. All three jobs succeed in four attempts; the command exits 0.

## Boundaries

This is a new policy simulator, not a broker, worker runtime or production queue emulator. Dead-lettering is a terminal report state, not an external queue. It does not yet model acknowledgements, visibility timeouts, jitter, persistence or redrive. Workers have identical capacity and jobs have constant per-attempt durations. Synthetic job metadata only; do not supply customer data, credentials or real payloads. See `ROADMAP.md` for unfinished work.
