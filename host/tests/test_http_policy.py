"""Explicit proxy/trust policy with offline origin and CONNECT fixtures."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import ssl
import subprocess
import tempfile
import threading
import unittest
from unittest.mock import patch

from mas_host.bridge_config import build_verifier
from mas_host.bridges.http_policy import HTTPTransportPolicy
from mas_host.bridges.mcp_transport import StreamableHTTPTransport
from mas_host.bridges.remote_http import BoundedJSONHTTP
from mas_host.bridges.service_client import http_exchange
from mas_host.service_contracts import ServiceError
from test_cursor_proxy_integration import _Server, _Origin, _TrapProxy


class HTTPPolicyTests(unittest.TestCase):
    def test_invalid_policy_and_proxy_credentials_in_urls_are_rejected(self):
        for value in ('https://proxy.example', 'http://user:secret@proxy.example',
                      'http://proxy.example/path', 'http://proxy.example?secret=x'):
            with self.subTest(value=value), self.assertRaises(ServiceError):
                HTTPTransportPolicy(proxy_url=value)
        with self.assertRaises(ServiceError):
            HTTPTransportPolicy(additional_ca_pem='not a certificate')

    def test_loopback_bypasses_proxy_and_does_not_read_proxy_credentials(self):
        policy = HTTPTransportPolicy(proxy_url='http://proxy.example:3128',
                                     proxy_authorization_file=Path('/missing-secret'))
        for endpoint in ('http://127.0.0.1:8765', 'https://localhost', 'https://[::1]'):
            wire = policy.wire(endpoint)
            self.assertIsNone(wire['proxy_url'])
            self.assertIsNone(wire['proxy_authorization'])

    def test_exact_origin_allowlist_precedes_network(self):
        policy = HTTPTransportPolicy(allowed_origins=('https://allowed.invalid',))
        with patch('subprocess.Popen') as launch:
            with self.assertRaisesRegex(ServiceError, 'endpoint denied'):
                http_exchange('https://other.invalid/rpc', transport_policy=policy)
        launch.assert_not_called()

    def test_opted_in_descriptors_bind_policy_and_legacy_descriptors_do_not_change(self):
        plain = BoundedJSONHTTP('https://peer.invalid')
        policy = HTTPTransportPolicy(proxy_url='http://proxy.invalid')
        configured = BoundedJSONHTTP('https://peer.invalid', transport_policy=policy)
        self.assertNotIn('transport_policy', plain.descriptor())
        self.assertEqual(configured.descriptor()['transport_policy'], policy.descriptor())
        self.assertNotIn('transport_policy', StreamableHTTPTransport('https://peer.invalid').descriptor())
        self.assertEqual(StreamableHTTPTransport('https://peer.invalid', transport_policy=policy)
                         .descriptor()['transport_policy'], policy.descriptor())

    def test_proxy_header_cannot_be_forwarded_as_an_origin_header(self):
        with patch('subprocess.Popen') as launch:
            with self.assertRaises(ServiceError):
                http_exchange('https://peer.invalid', headers={'Proxy-Authorization': 'Bearer secret'})
        launch.assert_not_called()

    def test_oauth_configuration_accepts_explicit_transport_without_exposing_secret_paths(self):
        auth = build_verifier({'auth': {'type': 'oauth', 'issuer': 'https://id.invalid',
            'resource': 'https://mas.invalid/mcp', 'introspection_url': 'https://id.invalid/introspect',
            'client_id': 'mas', 'client_secret_file': '/private/oauth', 'transport': {
                'proxy_url': 'http://proxy.invalid:3128', 'proxy_authorization_file': '/private/proxy',
                'allowed_origins': ['https://id.invalid']}}})
        self.assertIsInstance(auth.transport_policy, HTTPTransportPolicy)
        self.assertNotIn('/private', json.dumps(auth.transport_policy.descriptor()))
        self.assertNotIn('proxy', json.dumps(auth.metadata()))


class HTTPPolicyTLSIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        openssl = shutil.which('openssl')
        if openssl is None:
            raise unittest.SkipTest('TLS integration fixture requires the OpenSSL test CLI')
        cls.directory = tempfile.TemporaryDirectory(prefix='mas-http-policy-')
        cls.addClassCleanup(cls.directory.cleanup)
        cls.cert, cls.key = (Path(cls.directory.name) / name for name in ('ca.pem', 'key.pem'))
        subprocess.run([openssl, 'req', '-x509', '-newkey', 'rsa:2048', '-sha256',
            '-days', '2', '-nodes', '-keyout', str(cls.key), '-out', str(cls.cert),
            '-subj', '/CN=remote.invalid', '-addext', 'subjectAltName=DNS:remote.invalid,IP:127.0.0.1',
            '-addext', 'basicConstraints=critical,CA:TRUE'], check=True, capture_output=True, timeout=15)

    def serve(self, handler, *, tls=False):
        server = _Server(('127.0.0.1', 0), handler)
        server.requests = []
        server.tunnels = {}
        if tls:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(self.cert, self.key)
            server.socket = context.wrap_socket(server.socket, server_side=True)
        thread = threading.Thread(target=server.serve_forever,
                                  kwargs={'poll_interval': .01}, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 2)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return server

    def test_real_tls_additional_trust_and_hostname_checks(self):
        origin = self.serve(_Origin, tls=True)
        endpoint = f'https://127.0.0.1:{origin.server_port}/result'
        with self.assertRaisesRegex(ServiceError, 'transport failure'):
            http_exchange(endpoint, method='GET')
        policy = HTTPTransportPolicy.from_config({'additional_ca_file': str(self.cert)})
        status, _, data = http_exchange(endpoint, method='GET', transport_policy=policy)
        self.assertEqual((status, json.loads(data)), (200, {'route': 'origin'}))
        self.assertEqual(policy.descriptor()['additional_ca_sha256'],
                         hashlib.sha256(self.cert.read_bytes()).hexdigest())

    def test_real_authenticated_connect_does_not_mix_origin_and_proxy_credentials(self):
        origin, proxy = self.serve(_Origin, tls=True), self.serve(_TrapProxy)
        proxy.tunnels = {f'remote.invalid:{origin.server_port}': ('127.0.0.1', origin.server_port),
                         f'wrong.invalid:{origin.server_port}': ('127.0.0.1', origin.server_port)}
        secret = Path(self.directory.name) / 'proxy-secret'
        secret.write_text('Bearer private-proxy-credential\n')
        secret.chmod(0o600)
        policy = HTTPTransportPolicy(proxy_url=f'http://127.0.0.1:{proxy.server_port}',
            additional_ca_pem=self.cert.read_text(), proxy_authorization_file=secret)
        with patch.dict(os.environ, {'HTTPS_PROXY': 'http://unreachable.invalid:1',
                                     'SSL_CERT_FILE': '/missing-ambient-ca'}):
            status, _, data = http_exchange(f'https://remote.invalid:{origin.server_port}/result',
                method='GET', headers={'Authorization': 'Bearer private-origin-credential'}, transport_policy=policy)
        self.assertEqual((status, json.loads(data)), (200, {'route': 'origin'}))
        self.assertEqual(proxy.requests[0][1]['Proxy-Authorization'], 'Bearer private-proxy-credential')
        self.assertNotIn('Authorization', proxy.requests[0][1])
        self.assertEqual(origin.requests[0][1]['Authorization'], 'Bearer private-origin-credential')
        self.assertNotIn('Proxy-Authorization', origin.requests[0][1])
        with self.assertRaisesRegex(ServiceError, 'transport failure'):
            http_exchange(f'https://wrong.invalid:{origin.server_port}', transport_policy=policy)

