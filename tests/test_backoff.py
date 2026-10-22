import copy
import json
import random
import subprocess
import sys
import unittest

from queuelab import Backoff, simulate


def scenario(**options):
    return {'version': 1, 'retry': dict(max_attempts=5, backoff='exponential', delay_ms=10, **options),
            'jobs': [{'id': 'job', 'failures_before_success': 4}]}


class BackoffTest(unittest.TestCase):
    def test_cap_applies_before_jitter_and_includes_initial_delay(self):
        value = scenario(max_delay_ms=7)
        self.assertEqual([e['time_ms'] for e in simulate(value)['events']], [0, 7, 14, 21, 28])
        value['retry']['backoff'] = 'fixed'
        self.assertEqual(simulate(value)['summary']['finished_ms'], 28)

    def test_exponential_grows_then_caps(self):
        result = simulate(scenario(max_delay_ms=25))
        self.assertEqual([e['time_ms'] for e in result['events']], [0, 10, 30, 55, 80])

    def test_zero_cap_and_zero_base(self):
        value = scenario(max_delay_ms=0, jitter='full', seed=0)
        self.assertEqual(simulate(value)['summary']['finished_ms'], 0)
        value['retry']['delay_ms'] = 0
        del value['retry']['max_delay_ms']
        self.assertEqual(simulate(value)['summary']['finished_ms'], 0)

    def test_seeded_golden_sequence_and_bounds(self):
        policy = scenario(max_delay_ms=25, jitter='full', seed=42)['retry']
        backoff = Backoff(policy)
        self.assertEqual([backoff.delay(i) for i in range(1, 5)], [10, 3, 0, 23])
        for seed in range(30):
            policy['seed'] = seed
            backoff = Backoff(policy)
            for attempt in range(1, 100):
                delay = backoff.delay(attempt)
                self.assertGreaterEqual(delay, 0)
                self.assertLessEqual(delay, min(10 * 2**(attempt - 1), 25))

    def test_reproducibility_input_and_global_rng_isolation(self):
        value = scenario(max_delay_ms=25, jitter='full', seed=42)
        original = copy.deepcopy(value)
        state = random.getstate()
        first = simulate(value)
        self.assertEqual(state, random.getstate())
        self.assertEqual(first, simulate(value))
        self.assertEqual(value, original)
        value['retry']['seed'] = 43
        self.assertNotEqual(first['events'], simulate(value)['events'])

    def test_visibility_failure_uses_same_seeded_backoff(self):
        value = scenario(max_delay_ms=25, jitter='full', seed=42)
        plain = simulate(value)
        value['visibility_timeout_ms'] = 1000
        leased = simulate(value)
        self.assertEqual([e['time_ms'] for e in plain['events']], [e['time_ms'] for e in leased['events']])

    def test_expiry_does_not_consume_jitter(self):
        value = scenario(max_delay_ms=25, jitter='full', seed=42)
        value['workers'] = 2
        value['visibility_timeout_ms'] = 10
        value['jobs'] = [{'id': 'lost', 'failures_before_success': 0, 'ack_lost_attempts': [1]},
                         {'id': 'failure', 'enqueue_ms': 11, 'failures_before_success': 1}]
        result = simulate(value)
        expiry = next(e for e in result['events'] if e['type'] == 'visibility_expired')
        self.assertEqual(expiry['next_attempt_ms'], 10)
        failure = next(e for e in result['events'] if e.get('outcome') == 'failed')
        self.assertEqual(failure['next_attempt_ms'] - failure['time_ms'], 10)

    def test_invalid_configuration(self):
        for key, values in [('max_delay_ms', [-1, True, 1.5, None, 10**9 + 1]),
                            ('jitter', ['equal', True, None, []]),
                            ('seed', [-1, True, 1.5, None, 2**32])]:
            for bad in values:
                value = scenario(jitter='full', seed=42)
                value['retry'][key] = bad
                with self.subTest(key=key, bad=bad), self.assertRaises(ValueError):
                    simulate(value)
        for value in [scenario(jitter='full'), scenario(seed=42), scenario(jitter='none', seed=42)]:
            with self.assertRaises(ValueError):
                simulate(value)

    def test_explicit_none_preserves_original_report(self):
        original = scenario()
        value = copy.deepcopy(original)
        value['retry']['jitter'] = 'none'
        self.assertEqual(simulate(original), simulate(value))

    def test_cli_example_is_repeatable(self):
        command = [sys.executable, 'queuelab.py', 'examples/jitter.json']
        first = subprocess.run(command, check=True, capture_output=True, text=True)
        second = subprocess.run(command, check=True, capture_output=True, text=True)
        self.assertEqual(first.stdout, second.stdout)
        self.assertEqual(json.loads(first.stdout)['summary']['finished_ms'], 36)
        self.assertEqual(first.stderr, '')
