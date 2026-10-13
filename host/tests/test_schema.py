"""Real-file repository compatibility and read-only rejection regressions."""
from contextlib import closing, redirect_stdout
from dataclasses import asdict
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from mas_host.admin import main
from mas_host.journal import Journal, JournalError, inspect_run
from mas_host.operations import DeploymentLedger, DeploymentPolicy, OperationsError
from mas_host.schema import JOURNAL, LEDGER


META = {"run_id": "r", "workflow_version": "schema-test/1", "topology": {}, "max_depth": 8}


def snapshot(path):
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        return (hashlib.sha256(path.read_bytes()).hexdigest(), tuple(db.iterdump()),
                db.execute("PRAGMA application_id").fetchone()[0],
                db.execute("PRAGMA user_version").fetchone()[0])


class SchemaTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def legacy_ledger(self, name="state"):
        state = self.root / name
        state.mkdir(mode=0o700)
        with closing(sqlite3.connect(state / "deployment.sqlite")) as db:
            for statement in LEDGER.statements:
                db.execute(statement)
            db.execute("PRAGMA user_version=1")
            db.executemany("INSERT INTO metadata VALUES(?,?)", [
                ("policy", json.dumps(asdict(DeploymentPolicy()))),
                ("deployment_id", "deployment-original"), ("created_at", "123"),
                ("admission_state", "active")])
            db.execute("INSERT INTO runs VALUES('r','r',123,NULL)")
            db.execute("INSERT INTO calls VALUES('r','c',100,124)")
            db.commit()
        return state

    def legacy_journal(self, name="run"):
        run = self.root / name
        run.mkdir(mode=0o700)
        with closing(sqlite3.connect(run / "host.sqlite")) as db:
            for statement in JOURNAL.statements:
                db.execute(statement)
            db.executemany("INSERT INTO metadata VALUES(?,?)",
                           [(key, json.dumps(value)) for key, value in META.items()])
            db.execute("INSERT INTO steps VALUES('step','request','{}','{}')")
            db.execute("INSERT INTO invocations VALUES('call','step','worker','completed','{}','{}',100)")
            db.commit()
        return run

    def test_readonly_health_accepts_legacy_without_migration_or_creation(self):
        state = self.legacy_ledger()
        before = snapshot(state / "deployment.sqlite")
        with redirect_stdout(io.StringIO()):
            self.assertEqual(main(["--state-dir", str(state), "health"]), 0)
        self.assertEqual(before, snapshot(state / "deployment.sqlite"))
        absent = self.root / "absent"
        with redirect_stdout(io.StringIO()):
            self.assertEqual(main(["--state-dir", str(absent), "health"]), 1)
        self.assertFalse(absent.exists())

    def test_legacy_ledger_migration_preserves_identity_policy_and_charges(self):
        state = self.legacy_ledger()
        before = DeploymentLedger(state, readonly=True).health()
        ledger = DeploymentLedger(state)
        after = ledger.health()
        # Filesystem headroom changes independently of this migration, including
        # while another deployment writes. Compare the durable contract only.
        for field in ("deployment_id", "policy", "usage", "admission_state", "retained_runs"):
            self.assertEqual(before[field], after[field], field)
        data = snapshot(ledger.path)
        self.assertEqual(data[2:], (LEDGER.application_id, LEDGER.version))
        self.assertFalse(ledger.admit_call("r", "c", 100))

    def test_legacy_journal_migrates_preserving_checkpoints(self):
        run = self.legacy_journal()
        before = inspect_run(run)
        journal = Journal(run)
        self.assertEqual(journal.metadata(), META)
        journal.close()
        self.assertEqual(before, inspect_run(run))
        self.assertEqual(snapshot(run / "host.sqlite")[2:], (JOURNAL.application_id, JOURNAL.version))

    def test_unsupported_ledger_never_changes_on_open_or_health(self):
        alterations = ["PRAGMA user_version=99", "PRAGMA application_id=42",
                      "CREATE TABLE foreign_table(value TEXT)",
                      "ALTER TABLE calls ADD COLUMN unexpected TEXT",
                      "CREATE TRIGGER hidden AFTER INSERT ON calls BEGIN DELETE FROM calls; END"]
        for index, statement in enumerate(alterations):
            with self.subTest(statement=statement):
                state = self.legacy_ledger(str(index))
                with closing(sqlite3.connect(state / "deployment.sqlite")) as db:
                    db.execute(statement)
                    db.commit()
                before = snapshot(state / "deployment.sqlite")
                with self.assertRaises(OperationsError):
                    DeploymentLedger(state)
                with redirect_stdout(io.StringIO()):
                    self.assertEqual(main(["--state-dir", str(state), "health"]), 1)
                self.assertEqual(before, snapshot(state / "deployment.sqlite"))

    def test_unsupported_journal_never_changes_on_open_or_inspect(self):
        alterations = ["PRAGMA user_version=99", "PRAGMA application_id=42",
                      "CREATE TABLE foreign_table(value TEXT)",
                      "ALTER TABLE steps ADD COLUMN unexpected TEXT"]
        for index, statement in enumerate(alterations):
            with self.subTest(statement=statement):
                run = self.legacy_journal(str(index))
                with closing(sqlite3.connect(run / "host.sqlite")) as db:
                    db.execute(statement)
                    db.commit()
                before = snapshot(run / "host.sqlite")
                with self.assertRaises(JournalError):
                    Journal(run)
                with self.assertRaises(JournalError):
                    inspect_run(run)
                self.assertEqual(before, snapshot(run / "host.sqlite"))

    def test_bootstrap_initialises_empty_file_and_checks_existing_metadata(self):
        run = self.root / "run"
        run.mkdir(mode=0o700)
        (run / "host.sqlite").touch()
        Journal(run, META).close()
        before = snapshot(run / "host.sqlite")
        Journal(run, META).close()
        self.assertEqual(before, snapshot(run / "host.sqlite"))
        with self.assertRaises(JournalError):
            Journal(run, {**META, "workflow_version": "different"})
        self.assertEqual(before, snapshot(run / "host.sqlite"))

    def test_incomplete_legacy_metadata_needs_explicit_matching_bootstrap(self):
        run = self.legacy_journal()
        with closing(sqlite3.connect(run / "host.sqlite")) as db:
            db.execute("DELETE FROM metadata")
            db.commit()
        before = snapshot(run / "host.sqlite")
        with self.assertRaises(JournalError):
            Journal(run)
        self.assertEqual(before, snapshot(run / "host.sqlite"))
        Journal(run, META).close()
        journal = Journal(run)
        self.assertEqual(journal.metadata(), META)
        journal.close()

    def test_policy_mismatch_does_not_migrate_legacy(self):
        state = self.legacy_ledger()
        before = snapshot(state / "deployment.sqlite")
        with self.assertRaises(OperationsError):
            DeploymentLedger(state, DeploymentPolicy(max_runs=99))
        self.assertEqual(before, snapshot(state / "deployment.sqlite"))


if __name__ == "__main__":
    unittest.main()
