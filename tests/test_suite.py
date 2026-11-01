import copy
import unittest
from queuelab import simulate_suite, simulate

class SuiteTest(unittest.TestCase):
    def test_reports_and_independent_seed(self):
        scenario={'version':1,'retry':{'max_attempts':3,'backoff':'fixed','delay_ms':100,'jitter':'full','seed':42},'jobs':[{'id':'a','failures_before_success':2}]}
        result=simulate_suite({'version':1,'scenarios':[scenario,copy.deepcopy(scenario)]})
        self.assertEqual(result['reports'],[simulate(scenario)]*2)
        self.assertEqual(result['summary']['attempts'],6)
    def test_limits(self):
        for suite in [{'version':1,'scenarios':[]},{'version':True,'scenarios':[{}]},{'version':1,'scenarios':[{}]*21}]:
            with self.assertRaises(ValueError): simulate_suite(suite)
