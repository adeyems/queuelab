# QueueLab

An offline, virtual-time retry simulator for developers designing background-job policies. Explore synthetic failures, retry timing and exhausted jobs without running a broker or waiting for timers. Python 3.10+ standard library; no dependencies.

## Run

```sh
python3 queuelab.py examples/retries.json
python3 -m unittest discover -s tests -v
```

The example intentionally exits **1**: two jobs succeed and one exhausts its attempts. Its seven total attempts finish at virtual time 350 ms. Exit **0** means all jobs succeeded; **1** means at least one was dead-lettered; **2** means invalid input or an input-file error. Reports are JSON on stdout; input errors are JSON on stderr. No files are modified.

## Scenario version 1

See `examples/retries.json`. Required top-level fields are `version`, `retry`, and `jobs`; unknown fields are rejected. Retry policy requires `max_attempts` (1–100, including the initial attempt), `backoff` (`fixed` or `exponential`), and `delay_ms` (integer 0–1,000,000,000).

Each job has a unique ASCII `id` (1–64 letters, digits, hyphens or underscores), `failures_before_success` (0–100), and optional `enqueue_ms` (default 0, integer 0–1,000,000,000). A job fails that many initial attempts, then succeeds if its retry allowance permits. Files are limited to 1 MiB and 1–1,000 jobs. Booleans are not accepted as integers; duplicate JSON keys are rejected.

Attempts take zero virtual time. After failed attempt n, fixed backoff waits `delay_ms`; exponential waits `delay_ms * 2**(n-1)`. There is no jitter or delay cap. Python integer timestamps preserve large values exactly; consumers using JavaScript numbers must handle values beyond their safe integer range.

Events are ordered by due time, then scheduling insertion order. Initial arrivals are inserted in input order before any retries. A retry therefore follows already-scheduled events at the same timestamp, including with zero delay. Job summaries preserve input order. Identical scenarios produce identical reports; no randomness, wall-clock timestamps, networking or real sleeps are used.

## Boundaries

This is a new policy simulator, not a broker, worker runtime or production queue emulator. Dead-lettering is a terminal report state, not an external queue. It does not yet model worker concurrency, execution duration, acknowledgements, visibility timeouts, jitter, persistence or redrive. Synthetic IDs and failure counts only; do not supply customer data, credentials or real payloads. See `ROADMAP.md` for unfinished work.
