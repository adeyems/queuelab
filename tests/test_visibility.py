import copy
import json
import subprocess
import sys
import unittest

from queuelab import simulate


def scenario(duration=4, workers=2, lost=None, failures=0, attempts=3):
    return {'version': 1, 'workers': workers, 'visibility_timeout_ms': 10,
            'retry': {'max_attempts': attempts, 'backoff': 'fixed', 'delay_ms': 5},
            'jobs': [{'id': 'job', 'duration_ms': duration, 'failures_before_success': failures,
                      'ack_lost_attempts': [] if lost is None else lost}]}


class VisibilityTest(unittest.TestCase):
    def test_ack_cancels_expiry_without_extending_report_time(self):
        result = simulate(scenario())
        self.assertEqual(len(result['events']), 1)
        self.assertEqual(result['events'][0]['acknowledgement'], 'accepted')
        self.assertEqual(result['summary']['finished_ms'], 4)

    def test_lost_ack_redelivers_at_expiry_not_completion(self):
        result = simulate(scenario(lost=[1]))
        self.assertEqual([e['time_ms'] for e in result['events']], [4, 10, 14])
        self.assertEqual(result['events'][1]['type'], 'visibility_expired')
        self.assertEqual(result['summary']['attempts'], 2)
        self.assertEqual(result['jobs'][0]['state'], 'succeeded')

    def test_timeout_overlaps_and_stale_ack_does_not_succeed(self):
        result = simulate(scenario(duration=25, attempts=2))
        completions = [e for e in result['events'] if e['type'] == 'attempt_completed']
        self.assertEqual([e['started_ms'] for e in completions], [0, 10])
        self.assertEqual([e['worker_id'] for e in completions], [1, 2])
        self.assertTrue(all(e['acknowledgement'] == 'stale' for e in completions))
        self.assertEqual(result['jobs'][0]['finished_ms'], 20)
        self.assertEqual(result['summary']['finished_ms'], 35)
        self.assertEqual(result['jobs'][0]['state'], 'dead_lettered')

    def test_single_worker_not_released_by_expiry(self):
        result = simulate(scenario(duration=25, workers=1, attempts=2))
        completions = [e for e in result['events'] if e['type'] == 'attempt_completed']
        self.assertEqual([e['started_ms'] for e in completions], [0, 25])
        self.assertEqual(completions[1]['wait_ms'], 15)

    def test_completion_wins_exact_timeout_tie(self):
        result = simulate(scenario(duration=10))
        self.assertEqual(result['summary']['attempts'], 1)
        self.assertEqual(result['events'][0]['acknowledgement'], 'accepted')
        self.assertEqual(len(result['events']), 1)

    def test_lost_ack_at_exact_tie_still_expires(self):
        result = simulate(scenario(duration=10, lost=[1]))
        self.assertEqual([e['time_ms'] for e in result['events']], [10, 10, 20])
        self.assertEqual(result['jobs'][0]['state'], 'succeeded')

    def test_explicit_failure_uses_backoff_and_invalidates_old_expiry(self):
        result = simulate(scenario(failures=1))
        self.assertEqual([e['time_ms'] for e in result['events']], [4, 13])
        self.assertEqual(result['events'][1]['started_ms'], 9)
        self.assertEqual(result['summary']['attempts'], 2)

    def test_stale_failure_does_not_schedule_extra_retry(self):
        result = simulate(scenario(duration=25, failures=100, attempts=2))
        self.assertEqual(result['summary']['attempts'], 2)
        self.assertEqual(sum(e['type'] == 'visibility_expired' for e in result['events']), 2)

    def test_lost_ack_exhaustion_and_zero_duration(self):
        result = simulate(scenario(duration=0, lost=[1, 2, 3]))
        self.assertEqual(result['jobs'][0]['state'], 'dead_lettered')
        self.assertEqual(result['summary']['finished_ms'], 30)
        self.assertEqual(result['summary']['attempts'], 3)

    def test_deterministic_multi_job_capacity(self):
        value = scenario(duration=13)
        value['jobs'] += [{'id': 'other', 'duration_ms': 3, 'failures_before_success': 1}]
        original = copy.deepcopy(value)
        result = simulate(value)
        self.assertEqual(result, simulate(value))
        self.assertEqual(value, original)
        ends = {}
        for event in result['events']:
            if event['type'] == 'attempt_completed':
                self.assertGreaterEqual(event['started_ms'], ends.get(event['worker_id'], 0))
                ends[event['worker_id']] = event['time_ms']
        self.assertEqual(result['summary']['succeeded'], 1)
        self.assertEqual(result['summary']['dead_lettered'], 1)

    def test_validation(self):
        for invalid in [0, -1, True, 1.5, '10', None, 10**9 + 1]:
            value = scenario()
            value['visibility_timeout_ms'] = invalid
            with self.subTest(timeout=invalid), self.assertRaises(ValueError):
                simulate(value)
        for invalid in [True, [True], [0], [101], [1, 1], ['1'], [[]]]:
            value = scenario(lost=invalid)
            with self.subTest(lost=invalid), self.assertRaises(ValueError):
                simulate(value)
        value = scenario()
        del value['visibility_timeout_ms']
        with self.assertRaises(ValueError):
            simulate(value)

    def test_cli_example(self):
        result = subprocess.run([sys.executable, 'queuelab.py', 'examples/visibility.json'],
                                capture_output=True, text=True, check=True)
        report = json.loads(result.stdout)
        self.assertEqual(report['summary'], dict(succeeded=1, dead_lettered=0, attempts=2, finished_ms=14))
        self.assertEqual(result.stderr, '')
