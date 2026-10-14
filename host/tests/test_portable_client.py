"""Remote lifecycle clients do not require Linux execution facilities."""
import subprocess
import sys
import unittest


class PortableClientTests(unittest.TestCase):
    def test_http_client_imports_without_posix_execution_module(self):
        program = '''
import builtins
original = builtins.__import__
def restricted(name, *args, **kwargs):
    if name == 'fcntl':
        raise ImportError('fcntl is unavailable on Windows')
    return original(name, *args, **kwargs)
builtins.__import__ = restricted
from mas_host.bridges.service_client import TaskServiceClient
from mas_host.bridges.artifact_client import ArtifactTransferClient
'''
        result = subprocess.run([sys.executable, '-c', program], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == '__main__':
    unittest.main()
