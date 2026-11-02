import unittest
import contextlib
import io
import tempfile
from pathlib import Path
from queuelab import simulate, timeline_html, main
class HTMLTimelineTest(unittest.TestCase):
    def test_events_are_escaped_and_overflow_visible(self):
        report=simulate({'version':1,'retry':{'max_attempts':1,'backoff':'fixed','delay_ms':0},'jobs':[{'id':'job','failures_before_success':0,'duration_ms':10}]})
        report['events'][0]['job_id']='<script>bad</script>'
        report['events']*=1001
        rendered=timeline_html(report)
        self.assertNotIn('<script>',rendered)
        self.assertIn('&lt;script&gt;',rendered)
        self.assertIn('1000 events shown; 1 omitted',rendered)
        self.assertIn('<caption>',rendered)
        self.assertIn('width:100.0000%',rendered)

    def test_cli_exports_and_preserves_existing_file(self):
        with tempfile.TemporaryDirectory() as folder:
            output=Path(folder)/'timeline.html'
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(main(['examples/visibility.json','--timeline-html',str(output)]),0)
            original=output.read_bytes()
            self.assertIn(b'QueueLab virtual-time timeline',original)
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(main(['examples/visibility.json','--timeline-html',str(output)]),2)
            self.assertEqual(original,output.read_bytes())
