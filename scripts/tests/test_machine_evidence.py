"""The machine gate must retain real fixed-size BLOB spool observations."""
from contextlib import closing, redirect_stdout
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

SOURCE = Path(__file__).resolve().parents[1]/'qualification/machine_fixture.py'
SPEC = importlib.util.spec_from_file_location('machine_fixture_evidence_test', SOURCE)
fixture = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fixture)


class MachineEvidenceTests(unittest.TestCase):
    def test_snapshot_decodes_durable_blob_receipts_without_parsing_unwritten_reservations(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            spool = root/'example'
            spool.mkdir(mode=0o700)
            receipt = {'id': 'original-receipt', 'call_id': 'original-call', 'status': 'completed'}
            encoded = json.dumps(receipt).encode()
            padded = encoded+b' '*(131072-len(encoded))
            with closing(fixture.connect(spool/'worker-spool.sqlite', fixture.SPOOL, initialise=lambda _db: None)) as db, db:
                db.execute("INSERT INTO attempts VALUES('original-call',1,'{}','receipt',?,zeroblob(1024))", (padded,))
                db.execute("INSERT INTO attempts VALUES('running-call',1,'{}','executing',zeroblob(131072),zeroblob(1024))")
            output = io.StringIO()
            with patch.object(fixture, 'GUEST_STATE', root), patch.object(sys, 'argv', ['fixture', 'snapshot', '--case', 'example']), redirect_stdout(output):
                self.assertEqual(fixture.main(), 0)
            observations = json.loads(output.getvalue())['attempts']
            self.assertEqual(observations[0]['receipt'], receipt)
            self.assertEqual(observations[0]['state'], 'receipt')
            self.assertIsNone(observations[0]['ack'])
            self.assertIsNone(observations[1]['receipt'])


if __name__ == '__main__':
    unittest.main()
