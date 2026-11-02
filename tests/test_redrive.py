import copy
import unittest
from queuelab import redrive

class RedriveTest(unittest.TestCase):
    def scenario(self):
        return {'version':1,'retry':{'max_attempts':2,'backoff':'fixed','delay_ms':1},
                'jobs':[{'id':'failed','failures_before_success':2},{'id':'ok','failures_before_success':0}]}
    def test_preserves_history_and_consumes_prior_failures(self):
        source=self.scenario(); original=copy.deepcopy(source)
        report=redrive(source,['failed'])
        self.assertEqual(source, original)
        self.assertEqual(report['original']['summary']['dead_lettered'],1)
        self.assertEqual(report['redrive']['summary']['succeeded'],1)
        self.assertEqual(report['redrive']['jobs'][0]['attempts'],1)
        self.assertEqual(len(report['original']['events']),3)
        self.assertEqual(report['redrive_time_origin_ms'],report['original']['summary']['finished_ms'])
    def test_rejects_success_unknown_and_duplicate_ids(self):
        for selection in ([], ['ok'], ['missing'], ['failed','failed']):
            with self.assertRaises(ValueError):redrive(self.scenario(),selection)
    def test_lost_ack_attempts_are_shifted(self):
        source=self.scenario();source['visibility_timeout_ms']=5
        source['jobs']=[{'id':'failed','failures_before_success':0,'ack_lost_attempts':[1,2,3]}]
        report=redrive(source,['failed'])
        self.assertEqual(report['redrive_scenario']['jobs'][0]['ack_lost_attempts'],[1])
        self.assertEqual(report['redrive']['summary']['succeeded'],1)
        self.assertEqual(report['redrive']['summary']['attempts'],2)
