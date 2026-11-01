import contextlib
import io
import unittest
from queuelab import simulate, timeline, main

class TimelineTest(unittest.TestCase):
    def test_visibility_expiry_and_stale_work_remain_distinct(self):
        scenario = {'version':1, 'workers':2, 'visibility_timeout_ms':5,
                    'retry':{'max_attempts':2, 'backoff':'fixed', 'delay_ms':0},
                    'jobs':[{'id':'job', 'failures_before_success':0, 'duration_ms':10}]}
        report = simulate(scenario)
        rendered = timeline(report)
        self.assertIn('visibility_expired', rendered)
        self.assertIn('stale_completion', rendered)
        self.assertIn('started 0', rendered)
        self.assertIn('dead_lettered', rendered)

    def test_large_timeline_is_explicitly_truncated(self):
        report = simulate({'version':1, 'retry':{'max_attempts':1,'backoff':'fixed','delay_ms':0},
                           'jobs':[{'id':'job','failures_before_success':0}]})
        report['events'] *= 1001
        self.assertIn('showing 1000 of 1001', timeline(report))

    def test_cli_timeline_and_conflicting_modes(self):
        with contextlib.redirect_stdout(io.StringIO()) as out:
            code = main(['examples/visibility.json', '--timeline'])
        self.assertIn('QueueLab timeline', out.getvalue())
        self.assertIn(code, (0, 1))
        with contextlib.redirect_stderr(io.StringIO()) as err, contextlib.redirect_stdout(io.StringIO()) as out:
            self.assertEqual(main(['examples/visibility.json', '--timeline', '--suite']), 2)
        self.assertIn('single scenario', err.getvalue())
        self.assertEqual(out.getvalue(), '')
