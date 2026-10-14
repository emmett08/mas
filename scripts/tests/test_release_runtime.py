"""Regressions for build-only installer code leaking into deployed images."""
from __future__ import annotations

import importlib.machinery
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


SOURCE = Path(__file__).resolve().parents[1] / "release-container-check.py"
SPEC = importlib.util.spec_from_file_location("release_container_check", SOURCE)
RUNTIME = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNTIME)


class RuntimePackageTests(unittest.TestCase):
    def check_fixture(self, present: tuple[str, ...], bootstrap_dirs: tuple[Path, ...] = ()) -> dict:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            for name in present:
                package = directory / name
                package.mkdir()
                (package / "__init__.py").write_text("")
                if name == "ensurepip":
                    bundled = package / "_bundled"
                    bundled.mkdir()
                    (bundled / "pip-unpatched.whl").write_bytes(b"bootstrap fixture")
            def find_spec(name):
                return importlib.machinery.PathFinder.find_spec(name, [str(directory)])
            with patch.object(RUNTIME.importlib.util, "find_spec", side_effect=find_spec), \
                    patch.object(RUNTIME, "BOOTSTRAP_WHEEL_DIRS", bootstrap_dirs):
                return RUNTIME.runtime_package_identity()

    def test_installed_pip_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "package installers: pip"):
            self.check_fixture(("pip",))

    def test_bootstrap_bundle_is_rejected_after_pip_removal(self):
        with self.assertRaisesRegex(RuntimeError, "package installers: ensurepip"):
            self.check_fixture(("ensurepip",))

    def test_runtime_without_installers_is_accepted(self):
        self.assertEqual(self.check_fixture(()), {"package_installers": "absent", "bootstrap_wheels": "absent"})

    def test_external_wolfi_bootstrap_wheel_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "pip-bootstrap.whl").write_bytes(b"bootstrap wheel fixture")
            with self.assertRaisesRegex(RuntimeError, "bootstrap wheel payloads"):
                self.check_fixture((), (directory,))


if __name__ == "__main__":
    unittest.main()
