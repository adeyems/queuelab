import copy
import json
import random
import subprocess
import sys
import unittest

from queuelab import simulate


def scenario(workers=1, jobs=None, delay=5):
    return {'version': 1, 'workers': workers,
            'retry': {'max_attempts': 3, 'backoff': 'fixed', 'delay_ms': delay},
            'jobs': jobs if jobs is not None else [job('a', 10), job('b', 10), job('c', 10)]}


def job(name, duration, failures=0, enqueue=0):
    return dict(id=name, duration_ms=duration, failures_before_success=failures, enqueue_ms=enqueue)


class WorkerTest(unittest.TestCase):
    def test_serial_capacity_and_wait(self):
        events = simulate(scenario())['events']
        self.assertEqual([e['started_ms'] for e in events], [0, 10, 20])
        self.assertEqual([e['time_ms'] for e in events], [10, 20, 30])
        self.assertEqual([e['wait_ms'] for e in events], [0, 10, 20])

    def test_parallel_capacity_and_worker_reuse(self):
        events = simulate(scenario(2))['events']
        self.assertEqual([e['started_ms'] for e in events], [0, 0, 10])
        self.assertEqual([e['worker_id'] for e in events], [1, 2, 1])
        self.assertEqual([e['time_ms'] for e in events], [10, 10, 20])

    def test_completion_order_not_input_order(self):
        result = simulate(scenario(2, [job('slow', 20), job('fast', 5)]))
        self.assertEqual([e['job_id'] for e in result['events']], ['fast', 'slow'])
        self.assertEqual([j['id'] for j in result['jobs']], ['slow', 'fast'])

    def test_backoff_starts_at_completion_and_releases_worker(self):
        result = simulate(scenario(1, [job('retry', 10, 1), job('other', 20)]))
        events = result['events']
        self.assertEqual([e['job_id'] for e in events], ['retry', 'other', 'retry'])
        self.assertEqual(events[0]['next_attempt_ms'], 15)
        self.assertEqual(events[-1]['ready_ms'], 15)
        self.assertEqual(events[-1]['started_ms'], 30)
        self.assertEqual(events[-1]['time_ms'], 40)

    def test_completion_arrival_and_zero_delay_retry_tie(self):
        result = simulate(scenario(1, [job('retry', 10, 1), job('arrival', 0, enqueue=10)], delay=0))
        self.assertEqual([(e['job_id'], e['time_ms']) for e in result['events']],
                         [('retry', 10), ('arrival', 10), ('retry', 20)])

    def test_idle_gap_and_more_workers_than_jobs(self):
        event = simulate(scenario(1000, [job('late', 4, enqueue=100)]))['events'][0]
        self.assertEqual((event['started_ms'], event['time_ms'], event['worker_id']), (100, 104, 1))

    def test_exhaustion_includes_all_durations(self):
        result = simulate(scenario(1, [job('fail', 10, 100)], delay=5))
        self.assertEqual(result['jobs'][0]['finished_ms'], 40)
        self.assertEqual(result['jobs'][0]['state'], 'dead_lettered')

    def test_invalid_worker_counts_and_durations(self):
        for key, bad_values in [('workers', [0, 1001, True, 1.5, '2', None]),
                                ('duration_ms', [-1, 10**9 + 1, True, 0.5, '10', None])]:
            for value in bad_values:
                with self.subTest(key=key, value=value):
                    data = scenario()
                    (data if key == 'workers' else data['jobs'][0])[key] = value
                    with self.assertRaises(ValueError):
                        simulate(data)

    def test_seeded_mixed_workload_invariants(self):
        rng = random.Random(42)
        for workers in [1, 2, 7]:
            data = scenario(workers, [job(f'j{i}', rng.randrange(11), rng.randrange(5), rng.randrange(20))
                                      for i in range(40)], delay=0)
            before = copy.deepcopy(data)
            result = simulate(data)
            self.assertEqual(result, simulate(data))
            self.assertEqual(data, before)
            ends = {}
            previous = {}
            for event in result['events']:
                self.assertLessEqual(event['worker_id'], workers)
                self.assertGreaterEqual(event['started_ms'], event['ready_ms'])
                self.assertGreaterEqual(event['started_ms'], ends.get(event['worker_id'], 0))
                ends[event['worker_id']] = event['time_ms']
                self.assertGreaterEqual(event['started_ms'], previous.get(event['job_id'], 0))
                previous[event['job_id']] = event['time_ms']
            self.assertEqual([e['time_ms'] for e in result['events']], sorted(e['time_ms'] for e in result['events']))
            self.assertEqual(len(result['jobs']), 40)

    def test_cli_example(self):
        result = subprocess.run([sys.executable, 'queuelab.py', 'examples/workers.json'],
                                capture_output=True, text=True, check=True)
        report = json.loads(result.stdout)
        self.assertEqual(report['summary'], dict(succeeded=3, dead_lettered=0, attempts=4, finished_ms=35))
        self.assertEqual(result.stderr, '')
