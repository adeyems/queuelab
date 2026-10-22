# QueueLab roadmap

Completed milestone 1: validated synthetic scenarios, deterministic virtual-time scheduling, fixed/exponential retries, exhausted-job results, JSON CLI and automated tests.

Completed milestone 2: bounded worker concurrency, per-job attempt duration, completion-based backoff, deterministic dispatch/completion ties, per-attempt waiting times and overlapping-job tests.

Completed milestone 3: opt-in receipt-based acknowledgements, synthetic lost acknowledgements, visibility expiry/redelivery, overlapping duplicates, stale completion handling and bounded delivery counts.

Completed milestone 4: optional fixed/exponential delay caps and seeded full jitter, shared retry policy across simulation modes, reproducible examples and bound/isolation tests.

Remaining candidate milestones (not promises of hours worked or submission eligibility):

- Dead-letter redrive: explicitly select exhausted jobs and preserve their earlier attempt history.
- Policy comparison: compare completion time, attempt count and exhausted jobs across the same scenario.
- Scenario suites, report visualization and packaging: reproducible examples, useful diagnostics and installable CLI.

Choose and estimate these against real intended use before claiming remaining development hours. Keep all inputs synthetic; live broker integration and production execution are outside the current scope.
