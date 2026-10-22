import copy
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from queuelab import MAX_INPUT, load, simulate


def scenario(backoff='fixed', failures=2, attempts=3, delay=100):
    return {'version': 1, 'retry': {'max_attempts': attempts, 'backoff': backoff, 'delay_ms': delay},
            'jobs': [{'id': 'job', 'failures_before_success': failures}]}


class SimulationTest(unittest.TestCase):
    def test_immediate_success(self):
        result = simulate(scenario(failures=0))
        self.assertEqual(result['summary'], dict(succeeded=1, dead_lettered=0, attempts=1, finished_ms=0))

    def test_fixed_and_exponential_times(self):
        for mode, times in [('fixed', [0, 100, 200]), ('exponential', [0, 100, 300])]:
            with self.subTest(mode=mode):
                result = simulate(scenario(mode))
                self.assertEqual([e['time_ms'] for e in result['events']], times)
                self.assertEqual(result['jobs'][0]['state'], 'succeeded')

    def test_attempt_limit_includes_initial_attempt(self):
        result = simulate(scenario(failures=1, attempts=1))
        self.assertEqual(result['summary']['attempts'], 1)
        self.assertEqual(result['jobs'][0]['state'], 'dead_lettered')
        self.assertNotIn('next_attempt_ms', result['events'][0])

    def test_zero_delay_fair_ties(self):
        value = scenario(delay=0, failures=1)
        value['jobs'].append({'id': 'second', 'failures_before_success': 0})
        result = simulate(value)
        self.assertEqual([e['job_id'] for e in result['events']], ['job', 'second', 'job'])

    def test_arrival_ties_use_insertion_order(self):
        value = scenario(failures=1)
        value['jobs'].append({'id': 'later', 'enqueue_ms': 100, 'failures_before_success': 0})
        result = simulate(value)
        self.assertEqual([e['job_id'] for e in result['events']], ['job', 'later', 'job'])
        self.assertEqual(result['jobs'][1]['finished_ms'], 100)

    def test_deterministic_without_mutating_input(self):
        value = scenario('exponential')
        original = copy.deepcopy(value)
        self.assertEqual(simulate(value), simulate(value))
        self.assertEqual(value, original)

    def test_invalid_values(self):
        for field, values in [('max_attempts', [0, 101, True, 1.5]),
                              ('delay_ms', [-1, True, '100']), ('backoff', ['random', None, []])]:
            for invalid in values:
                with self.subTest(field=field, invalid=invalid):
                    value = scenario()
                    value['retry'][field] = invalid
                    with self.assertRaises(ValueError):
                        simulate(value)

    def test_invalid_jobs_and_structure(self):
        values = [None, {}, scenario(), scenario(), scenario(), scenario(), scenario()]
        values[2]['jobs'] *= 2
        values[3]['jobs'] = []
        values[4]['jobs'][0]['enqueue_ms'] = -1
        values[5]['extra'] = 1
        values[6]['jobs'][0]['id'] = 'unsafe\n'
        for value in values:
            with self.subTest(value=value), self.assertRaises(ValueError):
                simulate(value)

    def test_large_virtual_times_do_not_sleep_or_overflow(self):
        result = simulate(scenario('exponential', failures=100, attempts=100, delay=10**9))
        self.assertEqual(result['summary']['finished_ms'], (2**99 - 1) * 10**9)
        self.assertEqual(len(result['events']), 100)

    def test_file_validation(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'scenario.json'
            for raw in [b'{"version":1,"version":1}', b'{', b'x' * (MAX_INPUT + 1)]:
                path.write_bytes(raw)
                with self.assertRaises(ValueError):
                    load(path)

    def test_cli_exit_codes_and_output(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'scenario.json'
            for value, code in [(scenario(failures=0), 0), (scenario(failures=100), 1), ({}, 2)]:
                path.write_text(json.dumps(value))
                result = subprocess.run([sys.executable, 'queuelab.py', str(path)], capture_output=True, text=True)
                self.assertEqual(result.returncode, code)
                parsed = json.loads(result.stderr if code == 2 else result.stdout)
                self.assertIn('error' if code == 2 else 'summary', parsed)
                self.assertEqual(result.stdout if code == 2 else result.stderr, '')


if __name__ == '__main__':
    unittest.main()
