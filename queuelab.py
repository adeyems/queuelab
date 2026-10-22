"""Deterministic, offline retry simulation for synthetic jobs. Python 3.10+."""
import argparse
import heapq
import json
import random
import sys
from pathlib import Path

MAX_INPUT = 1024 * 1024


def integer(value, name, low, high):
    if type(value) is not int or not low <= value <= high:
        raise ValueError(f'{name} must be an integer in [{low}, {high}]')
    return value


def fields(value, required, optional=()):
    if not isinstance(value, dict) or not set(required) <= value.keys() or value.keys() - set(required) - set(optional):
        raise ValueError(f'Expected fields {list(required)}; optional {list(optional)}')


def validate(scenario):
    fields(scenario, ('version', 'retry', 'jobs'), ('workers', 'visibility_timeout_ms'))
    integer(scenario['version'], 'version', 1, 1)
    integer(scenario.get('workers', 1), 'workers', 1, 1000)
    if 'visibility_timeout_ms' in scenario:
        integer(scenario['visibility_timeout_ms'], 'visibility_timeout_ms', 1, 1_000_000_000)
    retry = scenario['retry']
    fields(retry, ('max_attempts', 'backoff', 'delay_ms'), ('max_delay_ms', 'jitter', 'seed'))
    integer(retry['max_attempts'], 'max_attempts', 1, 100)
    integer(retry['delay_ms'], 'delay_ms', 0, 1_000_000_000)
    if retry['backoff'] not in ('fixed', 'exponential'):
        raise ValueError('backoff must be fixed or exponential')
    if 'max_delay_ms' in retry:
        integer(retry['max_delay_ms'], 'max_delay_ms', 0, 1_000_000_000)
    if retry.get('jitter', 'none') not in ('none', 'full'):
        raise ValueError('jitter must be none or full')
    if retry.get('jitter', 'none') == 'full':
        integer(retry.get('seed'), 'seed', 0, 2**32 - 1)
    elif 'seed' in retry:
        raise ValueError('seed requires full jitter')
    jobs = scenario['jobs']
    if not isinstance(jobs, list) or not 1 <= len(jobs) <= 1000:
        raise ValueError('jobs must contain 1 to 1000 jobs')
    ids = set()
    for job in jobs:
        fields(job, ('id', 'failures_before_success'), ('enqueue_ms', 'duration_ms', 'ack_lost_attempts'))
        name = job['id']
        if not isinstance(name, str) or not 1 <= len(name) <= 64 or not name.isascii() or not all(c.isalnum() or c in '_-' for c in name):
            raise ValueError('Job id must be 1–64 ASCII letters, digits, hyphens or underscores')
        if name in ids:
            raise ValueError(f'Duplicate job id: {name}')
        ids.add(name)
        integer(job['failures_before_success'], 'failures_before_success', 0, 100)
        integer(job.get('enqueue_ms', 0), 'enqueue_ms', 0, 1_000_000_000)
        integer(job.get('duration_ms', 0), 'duration_ms', 0, 1_000_000_000)
        lost = job.get('ack_lost_attempts', [])
        if not isinstance(lost, list) or len(lost) > 100:
            raise ValueError('ack_lost_attempts must be a list of at most 100 attempt numbers')
        for number in lost:
            integer(number, 'ack_lost_attempts entry', 1, 100)
        if len(set(lost)) != len(lost):
            raise ValueError('ack_lost_attempts must not contain duplicates')
        if 'ack_lost_attempts' in job and 'visibility_timeout_ms' not in scenario:
            raise ValueError('ack_lost_attempts requires visibility_timeout_ms')
    return scenario


class Backoff:
    """One independent random stream per simulation; only retries consume it."""

    def __init__(self, policy):
        self.policy = policy
        self.rng = random.Random(policy['seed']) if policy.get('jitter') == 'full' else None

    def delay(self, attempt):
        policy = self.policy
        delay = policy['delay_ms'] * (2 ** (attempt - 1) if policy['backoff'] == 'exponential' else 1)
        delay = min(delay, policy.get('max_delay_ms', delay))
        return self.rng.randint(0, delay) if self.rng is not None else delay


def simulate(scenario):
    """FIFO-ready dispatch with completion-first ties and lowest-ID idle workers."""
    validate(scenario)
    if 'visibility_timeout_ms' in scenario:
        return simulate_visibility(scenario)
    retry = scenario['retry']
    backoff = Backoff(retry)
    queue = []
    sequence = 0
    for job in scenario['jobs']:
        heapq.heappush(queue, (job.get('enqueue_ms', 0), sequence, 1, job))
        sequence += 1
    events, results = [], {}
    idle = list(range(1, scenario.get('workers', 1) + 1))
    running = []
    at = 0
    dispatch_sequence = 0
    while queue or running:
        if running and running[0][0] <= at:
            at, _, worker, ready, started, attempt, job = heapq.heappop(running)
            heapq.heappush(idle, worker)
        elif idle and queue and queue[0][0] <= at:
            ready, _, attempt, job = heapq.heappop(queue)
            worker = heapq.heappop(idle)
            heapq.heappush(running, (at + job.get('duration_ms', 0), dispatch_sequence,
                                     worker, ready, at, attempt, job))
            dispatch_sequence += 1
            continue
        else:
            candidates = ([running[0][0]] if running else [])
            if idle and queue:
                candidates.append(queue[0][0])
            at = min(candidates)
            continue
        succeeded = attempt > job['failures_before_success']
        terminal = succeeded or attempt == retry['max_attempts']
        event = {'time_ms': at, 'job_id': job['id'], 'attempt': attempt,
                 'worker_id': worker, 'ready_ms': ready, 'started_ms': started,
                 'wait_ms': started - ready, 'duration_ms': at - started,
                 'outcome': 'succeeded' if succeeded else 'failed'}
        if terminal:
            state = 'succeeded' if succeeded else 'dead_lettered'
            event['state'] = state
            results[job['id']] = {'id': job['id'], 'state': state, 'attempts': attempt, 'finished_ms': at}
        else:
            delay = backoff.delay(attempt)
            event.update(state='retry_scheduled', next_attempt_ms=at + delay)
            heapq.heappush(queue, (at + delay, sequence, attempt + 1, job))
            sequence += 1
        events.append(event)
    ordered = [results[job['id']] for job in scenario['jobs']]
    return {'version': 1, 'events': events, 'jobs': ordered,
            'summary': {'succeeded': sum(j['state'] == 'succeeded' for j in ordered),
                        'dead_lettered': sum(j['state'] == 'dead_lettered' for j in ordered),
                        'attempts': len(events), 'finished_ms': events[-1]['time_ms']}}


def simulate_visibility(scenario):
    """Validated opt-in receipt/lease model; expiry never cancels worker execution."""
    retry = scenario['retry']
    backoff = Backoff(retry)
    ready = []
    scheduled = []
    idle = list(range(1, scenario.get('workers', 1) + 1))
    current = {}
    results = {}
    events = []
    counts = {job['id']: 0 for job in scenario['jobs']}
    sequence = 0
    receipt = 0
    at = 0

    def enqueue(job, when):
        nonlocal sequence
        heapq.heappush(ready, (when, sequence, job))
        sequence += 1

    def settle(job, attempt, event, success=False, timeout=False):
        current.pop(job['id'], None)
        if success or attempt == retry['max_attempts']:
            state = 'succeeded' if success else 'dead_lettered'
            event['state'] = state
            results[job['id']] = dict(id=job['id'], state=state, attempts=attempt, finished_ms=at)
        else:
            delay = 0 if timeout else backoff.delay(attempt)
            event.update(state='redelivery_scheduled' if timeout else 'retry_scheduled', next_attempt_ms=at + delay)
            enqueue(job, at + delay)

    for job in scenario['jobs']:
        enqueue(job, job.get('enqueue_ms', 0))
    while ready or scheduled:
        if scheduled and scheduled[0][0] <= at:
            _, kind, token, worker, eligible, started, attempt, job = heapq.heappop(scheduled)
            active = current.get(job['id']) == token
            if kind == 1:
                if not active:
                    continue
                event = dict(time_ms=at, job_id=job['id'], attempt=attempt, receipt_id=token,
                             type='visibility_expired')
                settle(job, attempt, event, timeout=True)
            else:
                heapq.heappush(idle, worker)
                success = attempt > job['failures_before_success']
                event = dict(time_ms=at, job_id=job['id'], attempt=attempt, receipt_id=token,
                             type='attempt_completed', worker_id=worker, ready_ms=eligible,
                             started_ms=started, wait_ms=started - eligible, duration_ms=at - started,
                             outcome='succeeded' if success else 'failed')
                if not active:
                    event.update(acknowledgement='stale' if success else 'not_sent', state='stale_completion')
                elif not success:
                    event['acknowledgement'] = 'not_sent'
                    settle(job, attempt, event)
                elif attempt in job.get('ack_lost_attempts', []):
                    event.update(acknowledgement='lost', state='awaiting_visibility_timeout')
                else:
                    event['acknowledgement'] = 'accepted'
                    settle(job, attempt, event, success=True)
            events.append(event)
        elif idle and ready and ready[0][0] <= at:
            eligible, _, job = heapq.heappop(ready)
            worker = heapq.heappop(idle)
            receipt += 1
            counts[job['id']] += 1
            attempt = counts[job['id']]
            current[job['id']] = receipt
            data = (receipt, worker, eligible, at, attempt, job)
            heapq.heappush(scheduled, (at + job.get('duration_ms', 0), 0, *data))
            heapq.heappush(scheduled, (at + scenario['visibility_timeout_ms'], 1, *data))
        else:
            times = [scheduled[0][0]] if scheduled else []
            if idle and ready:
                times.append(ready[0][0])
            at = min(times)
    ordered = [results[job['id']] for job in scenario['jobs']]
    return {'version': 1, 'events': events, 'jobs': ordered,
            'summary': {'succeeded': sum(j['state'] == 'succeeded' for j in ordered),
                        'dead_lettered': sum(j['state'] == 'dead_lettered' for j in ordered),
                        'attempts': sum(counts.values()), 'finished_ms': events[-1]['time_ms']}}


def load(path):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f'Duplicate JSON key: {key}')
            result[key] = value
        return result
    with Path(path).open('rb') as source:
        raw = source.read(MAX_INPUT + 1)
    if len(raw) > MAX_INPUT:
        raise ValueError('Scenario exceeds 1 MiB')
    return json.loads(raw, object_pairs_hook=unique)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('scenario', help='Synthetic version-1 JSON scenario')
    args = parser.parse_args(argv)
    try:
        result = simulate(load(args.scenario))
    except (ValueError, OSError, RecursionError) as error:
        print(json.dumps({'error': str(error)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2))
    return 1 if result['summary']['dead_lettered'] else 0


if __name__ == '__main__':
    sys.exit(main())
