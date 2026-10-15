"""Live progress and journal outbox replay cannot change external effects."""
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from mas_host.contracts import Invocation, Outcome
from mas_host.journal import Journal
from mas_host.runner import Runner
import test_runner as runner_tests


class RunnerProgressTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        runner_tests.RunnerIntegrationTests.setUpClass()
        cls.options = {"mas_bin": runner_tests.RunnerIntegrationTests.mas,
                       "gateway_bin": runner_tests.RunnerIntegrationTests.gateway,
                       "workflow_version": "progress/1", "supervised": False}

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.effects, self.events = [], {}

    def sink(self, event):
        identity = (event["run_id"], event["message_id"])
        if identity in self.events:
            self.assertEqual(self.events[identity], event)
        self.events[identity] = event

    def adapters(self, *, assert_live=False):
        def b(context, request):
            return context.request("c", "leaf", request.input)
        def c(context, request):
            if assert_live:
                self.assertEqual(len(self.events), 2)
                self.assertTrue(all(event["kind"] == "Request" for event in self.events.values()))
            def effect():
                self.effects.append(True)
                return Invocation(request.input, {})
            return Outcome(context.invoke(effect))
        return {"a": runner_tests.FunctionAdapter(lambda _c, request: Outcome(request.input)),
                "b": runner_tests.FunctionAdapter(b), "c": runner_tests.FunctionAdapter(c)}

    def test_events_are_publicly_available_before_nested_effect_completes(self):
        with Runner(runner_tests.topology(), self.adapters(assert_live=True), self.root,
                    event_sink=self.sink, **self.options) as runner:
            self.assertEqual(runner.request("a", "b", "root", {"SECRET": "input"}).output, {"SECRET": "input"})
            self.assertEqual(len(self.events), 4)
            self.assertNotIn("SECRET", str(self.events))
            self.assertEqual(runner._journal.pending_events(), [])

    def test_completed_nested_events_recover_after_sink_outage_without_invocation_replay(self):
        def unavailable(_event):
            raise RuntimeError("private storage detail")
        adapters = self.adapters()
        with Runner(runner_tests.topology(), adapters, self.root, event_sink=unavailable, **self.options) as runner:
            runner.request("a", "b", "root", {})
            run = runner.run_dir
            self.assertEqual(len(runner._journal.pending_events()), 4)
        with Runner.resume(run, adapters, event_sink=self.sink, **self.options) as runner:
            self.assertEqual(len(self.events), 4)
            runner.request("a", "b", "root", {})
            self.assertEqual(len(self.events), 4)
        self.assertEqual(self.effects, [True])

    def test_outbox_gap_and_lost_publication_ack_are_repaired_by_stable_identity(self):
        adapters = self.adapters()
        with Runner(runner_tests.topology(), adapters, self.root, event_sink=self.sink, **self.options) as runner:
            with patch.object(Journal, "queue_event", side_effect=OSError("interrupted outbox")):
                runner.request("a", "b", "root", {})
            run = runner.run_dir
            self.assertEqual(self.events, {})
        with patch.object(Journal, "publish_event", side_effect=OSError("lost publication ACK")):
            with Runner.resume(run, adapters, event_sink=self.sink, **self.options) as runner:
                runner.request("a", "b", "root", {})
        with Runner.resume(run, adapters, event_sink=self.sink, **self.options) as runner:
            runner.request("a", "b", "root", {})
            self.assertEqual(runner._journal.pending_events(), [])
        self.assertEqual(len(self.events), 4)
        self.assertEqual(self.effects, [True])


if __name__ == "__main__":
    unittest.main()
