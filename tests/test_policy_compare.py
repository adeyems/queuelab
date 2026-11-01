import copy
import unittest
from queuelab import compare_policies

class PolicyTest(unittest.TestCase):
    def test_delta_and_history(self):
        a = {'version': 1, 'retry': {'max_attempts': 2, 'backoff': 'fixed', 'delay_ms': 10}, 'jobs': [{'id': 'one', 'failures_before_success': 2}]}
        b = copy.deepcopy(a)
        b['retry']['max_attempts'] = 3
        saved = copy.deepcopy(a)
        result = compare_policies(a, b)
        self.assertEqual(result['summary_delta'], {'succeeded': 1, 'dead_lettered': -1, 'attempts': 1, 'finished_ms': 10})
        self.assertEqual(len(result['baseline']['events']), 2)
        self.assertEqual(a, saved)
        b['jobs'][0]['failures_before_success'] = 1
        with self.assertRaises(ValueError): compare_policies(a, b)
