"""Deterministic, offline retry simulation for synthetic jobs. Python 3.10+."""
import argparse
import heapq
import json
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
    fields(scenario, ('version', 'retry', 'jobs'), ('workers',))
    integer(scenario['version'], 'version', 1, 1)
    integer(scenario.get('workers', 1), 'workers', 1, 1000)
    retry = scenario['retry']
    fields(retry, ('max_attempts', 'backoff', 'delay_ms'))
    integer(retry['max_attempts'], 'max_attempts', 1, 100)
    integer(retry['delay_ms'], 'delay_ms', 0, 1_000_000_000)
    if retry['backoff'] not in ('fixed', 'exponential'):
        raise ValueError('backoff must be fixed or exponential')
    jobs = scenario['jobs']
    if not isinstance(jobs, list) or not 1 <= len(jobs) <= 1000:
        raise ValueError('jobs must contain 1 to 1000 jobs')
    ids = set()
    for job in jobs:
        fields(job, ('id', 'failures_before_success'), ('enqueue_ms', 'duration_ms'))
        name = job['id']
        if not isinstance(name, str) or not 1 <= len(name) <= 64 or not name.isascii() or not all(c.isalnum() or c in '_-' for c in name):
            raise ValueError('Job id must be 1–64 ASCII letters, digits, hyphens or underscores')
        if name in ids:
            raise ValueError(f'Duplicate job id: {name}')
        ids.add(name)
        integer(job['failures_before_success'], 'failures_before_success', 0, 100)
        integer(job.get('enqueue_ms', 0), 'enqueue_ms', 0, 1_000_000_000)
        integer(job.get('duration_ms', 0), 'duration_ms', 0, 1_000_000_000)
    return scenario


def simulate(scenario):
    """FIFO-ready dispatch with completion-first ties and lowest-ID idle workers."""
    validate(scenario)
    retry = scenario['retry']
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
            delay = retry['delay_ms'] * (2 ** (attempt - 1) if retry['backoff'] == 'exponential' else 1)
            event.update(state='retry_scheduled', next_attempt_ms=at + delay)
            heapq.heappush(queue, (at + delay, sequence, attempt + 1, job))
            sequence += 1
        events.append(event)
    ordered = [results[job['id']] for job in scenario['jobs']]
    return {'version': 1, 'events': events, 'jobs': ordered,
            'summary': {'succeeded': sum(j['state'] == 'succeeded' for j in ordered),
                        'dead_lettered': sum(j['state'] == 'dead_lettered' for j in ordered),
                        'attempts': len(events), 'finished_ms': events[-1]['time_ms']}}


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
