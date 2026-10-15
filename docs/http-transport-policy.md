# Explicit HTTP transport policy

The default MAS HTTP transport uses system TLS trust, connects directly and
ignores ambient proxy/CA environment variables. It never follows redirects.
An optional `HTTPTransportPolicy` enables a configured corporate HTTP CONNECT
proxy, additional private CA roots and an exact destination-origin allowlist.
It does not disable certificate or hostname verification.

```python
from mas_host.bridges.http_policy import HTTPTransportPolicy
from mas_host.bridges.service_client import TaskServiceClient

policy = HTTPTransportPolicy.from_config({
    "proxy_url": "http://proxy.example:8080",
    "proxy_authorization_file": "/run/secrets/proxy-authorization",
    "additional_ca_file": "/run/secrets/corporate-ca.pem",
    "allowed_origins": ["https://mas.example", "https://identity.example"]
})
client = TaskServiceClient(
    "https://mas.example", token_file=private_token_file,
    principal="service-client", transport_policy=policy)
```

The proxy credential file contains one `Basic ...` or `Bearer ...` header value,
optionally followed by one newline, and follows the existing private-file rules.
It is read at request time, so rotation does not require changing the frozen
transport descriptor. It is sent only in CONNECT, never to the origin. Proxy
URLs cannot contain credentials, paths, queries or fragments. This profile
supports an HTTP proxy tunnelling an HTTPS origin; HTTPS-to-proxy transport and
proxying cleartext origins are rejected. Literal loopback addresses and
`localhost` bypass the proxy and do not read its credential file.

The optional origin allowlist is exact after normalising scheme, hostname and
port. An empty list permits configured endpoints under the usual endpoint
validation rules. CA PEM content is bounded and frozen when the policy is built;
its SHA-256, the proxy origin and allowed origins participate in remote adapter
identity. CA file paths and credentials do not. Additional roots extend system
trust; they do not replace it. Refresh the configured policy and implementation
version intentionally when trust roots change.

`BoundedJSONHTTP` (including A2A clients), `StreamableHTTPTransport` (MCP),
`TaskServiceClient` and `OAuthIntrospectionVerifier` accept `transport_policy`.
For OAuth deployment configuration, add the same dictionary as `auth.transport`.
No endpoint discovery, response header or request body can override this policy.
The existing isolated subprocess still bounds DNS, connect, TLS, header and
body handling with an absolute deadline, including when invoked from a worker
thread. Artefact chunk transfers inherit their configured HTTP/MCP policy.

Offline tests use a real local TLS origin and CONNECT proxy: an untrusted CA
fails, configured trust succeeds, incorrect hostnames fail, origin/proxy
credentials remain separate, loopback bypass works, environment variables are
ignored and disallowed origins fail before a connection is attempted.
