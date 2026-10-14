"""Opt-in network interop against the pinned real Temporal development server.

MAS_TEMPORAL_INTEROP=1 enables the test; MAS_TEMPORAL_SERVER may name an
already provisioned Temporal CLI 1.9.1 binary to avoid downloading it.
No provider credentials or model calls are used.
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import unittest

from mas_host.bridges.temporal import TemporalTarget, TemporalWorkflowClient
from mas_host.service_contracts import ServiceError

AVAILABLE = importlib.util.find_spec("temporalio") is not None
if AVAILABLE:
    from temporalio import workflow

    @workflow.defn(name="MASBridgeInterop")
    class InteropWorkflow:
        @workflow.run
        async def run(self, input: dict) -> dict:
            if input.get("wait"):
                await workflow.sleep(300)
            return {"answer": input["answer"]}


@unittest.skipUnless(AVAILABLE and os.environ.get("MAS_TEMPORAL_INTEROP") == "1",
                     "set MAS_TEMPORAL_INTEROP=1 for pinned real-server interoperability")
class TemporalServerTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_start_poll_cancel_and_lost_receipt_reconciliation(self):
        from temporalio.client import Client
        from temporalio.testing import WorkflowEnvironment
        from temporalio.worker import Worker, UnsandboxedWorkflowRunner
        existing = os.environ.get("MAS_TEMPORAL_SERVER")
        environment = await asyncio.wait_for(WorkflowEnvironment.start_local(
            dev_server_existing_path=existing, dev_server_download_version="v1.9.1"), timeout=60)
        try:
            target = TemporalTarget(environment.client.service_client.config.target_host,
                                    "default", "MASBridgeInterop", "bridge-interop", "test", tls=False)
            client = TemporalWorkflowClient(target)
            async with Worker(environment.client, task_queue=target.task_queue,
                              workflows=[InteropWorkflow], workflow_runner=UnsandboxedWorkflowRunner()):
                initial = await asyncio.to_thread(client.submit, {"request_key": "complete", "input": {"answer": 42}})
                async def terminal(remote_id):
                    for _ in range(100):
                        current = await asyncio.to_thread(client.poll, remote_id)
                        if current.state in {"completed", "cancelled", "failed"}:
                            return current
                        await asyncio.sleep(0.05)
                    self.fail("Temporal execution did not reach a terminal state")
                done = await terminal(initial.remote_id)
                self.assertEqual(done.state, "completed")
                self.assertEqual(done.result, {"answer": 42})
                slow = await asyncio.to_thread(client.submit, {"request_key": "cancel", "input": {"answer": 7, "wait": True}})
                await asyncio.to_thread(client.cancel, slow.remote_id)
                stopped = await terminal(slow.remote_id)
                self.assertEqual(stopped.state, "cancelled")

                # Send a real start request, discard its accepted reply, then
                # recover the exact run by the precommitted workflow ID/memo.
                class LoseReply:
                    def __init__(self, actual):
                        self.actual = actual
                    async def start_workflow(self, *args, **kwargs):
                        await self.actual.start_workflow(*args, **kwargs)
                        raise TimeoutError("discarded accepted response")
                async def lost_client():
                    return LoseReply(await Client.connect(target.endpoint, namespace="default"))
                uncertain = TemporalWorkflowClient(target, client_factory=lost_client)
                payload = {"request_key": "lost", "input": {"answer": 99}}
                with self.assertRaisesRegex(ServiceError, "temporal outcome unknown"):
                    await asyncio.to_thread(uncertain.submit, payload)
                exact = await asyncio.to_thread(client.lookup, payload)
                recovered = await terminal(exact)
                self.assertEqual(recovered.result, {"answer": 99})
                with self.assertRaises(ServiceError):
                    await asyncio.to_thread(client.submit, payload)
                self.assertEqual(await asyncio.to_thread(client.lookup, payload), exact)
        finally:
            await environment.shutdown()


if __name__ == "__main__":
    unittest.main()
