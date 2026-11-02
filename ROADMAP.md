# QueueLab roadmap

Completed milestone 1: validated synthetic scenarios, deterministic virtual-time scheduling, fixed/exponential retries, exhausted-job results, JSON CLI and automated tests.

Completed milestone 2: bounded worker concurrency, per-job attempt duration, completion-based backoff, deterministic dispatch/completion ties, per-attempt waiting times and overlapping-job tests.

Completed milestone 3: opt-in receipt-based acknowledgements, synthetic lost acknowledgements, visibility expiry/redelivery, overlapping duplicates, stale completion handling and bounded delivery counts.

Completed milestone 4: optional fixed/exponential delay caps and seeded full jitter, shared retry policy across simulation modes, reproducible examples and bound/isolation tests.

Remaining candidate milestones (not promises of hours worked or submission eligibility):

- Completed scoped dead-letter redrive: explicit selection, retained history, one second batch. More advanced redrive controls require a separate scope decision.
- Policy comparison: compare completion time, attempt count and exhausted jobs across the same scenario.
- Scenario suites, report visualization and packaging: reproducible examples, useful diagnostics and installable CLI.

Choose and estimate these against real intended use before claiming remaining development hours. Keep all inputs synthetic; live broker integration and production execution are outside the current scope.

Completed follow-up: retry policy comparison. See README for scope and remaining limitations.

Completed packaging slice: installable wheel and console entry point with isolated installation checks in CI. Public package publication and broader release automation remain future work.

Completed scoped milestone: scenario suites. See README for exact semantics and remaining limitations.

Completed scoped follow-up: text event timeline. See README for acceptance scope and remaining limitations.

Completed scoped follow-up: explicit dead-letter redrive. See README for precise behavior and remaining work.

Completed scoped follow-up: local html timeline viewer. See README for limitations and remaining work.
