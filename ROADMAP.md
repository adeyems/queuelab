# QueueLab roadmap

Completed milestone 1: validated synthetic scenarios, deterministic virtual-time scheduling, fixed/exponential retries, exhausted-job results, JSON CLI and automated tests.

Remaining candidate milestones (not promises of hours worked or submission eligibility):

- Worker concurrency and job duration: model overlapping attempts and verify capacity limits and stable same-time ordering.
- Acknowledgement and visibility timeouts: demonstrate redelivery and duplicate processing with deterministic traces.
- Seeded jitter and capped backoff: reproduce identical runs and test delay bounds.
- Dead-letter redrive: explicitly select exhausted jobs and preserve their earlier attempt history.
- Policy comparison: compare completion time, attempt count and exhausted jobs across the same scenario.
- Scenario suites, report visualization and packaging: reproducible examples, useful diagnostics and installable CLI.

Choose and estimate these against real intended use before claiming remaining development hours. Keep all inputs synthetic; live broker integration and production execution are outside the current scope.
