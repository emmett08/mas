"""Regression checks for optional Cursor native-library integrity boundaries."""
from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SOURCE = Path(__file__).resolve().parents[1] / "release-deb.py"
SPEC = importlib.util.spec_from_file_location("release_deb", SOURCE)
DEB = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(DEB)


class DebianLockTests(unittest.TestCase):
    def lock(self):
        return {"format": "mas-deb-lock/1", "runtime": {"amd64": [
            {"name": name, "filename": f"{name}_1_amd64.deb", "url": DEB.ORIGIN + "pool/test.deb",
             "sha256": hashlib.sha256(b"verified").hexdigest()} for name in DEB.PACKAGES]}}

    def test_changed_archive_is_rejected_before_write(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(DEB, "download", return_value=b"changed"):
            with self.assertRaisesRegex(ValueError, "hash mismatch"):
                DEB.fetch(self.lock(), "amd64", Path(tmp))
            self.assertEqual(list(Path(tmp).iterdir()), [])

    def test_wrong_architecture_and_path_escape_are_rejected_before_download(self):
        for filename in ("../escape_amd64.deb", "gcc-14-base_1_arm64.deb"):
            lock = self.lock()
            lock["runtime"]["amd64"][0]["filename"] = filename
            with tempfile.TemporaryDirectory() as tmp, patch.object(DEB, "download") as download:
                with self.assertRaisesRegex(ValueError, "filename"):
                    DEB.fetch(lock, "amd64", Path(tmp))
                download.assert_not_called()

    def test_validated_archives_are_written_with_expected_names(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(DEB, "download", return_value=b"verified"):
            DEB.fetch(self.lock(), "amd64", Path(tmp))
            self.assertEqual({path.name for path in Path(tmp).iterdir()},
                             {f"{name}_1_amd64.deb" for name in DEB.PACKAGES})
            self.assertTrue(all(path.read_bytes() == b"verified" for path in Path(tmp).iterdir()))


if __name__ == "__main__":
    unittest.main()
