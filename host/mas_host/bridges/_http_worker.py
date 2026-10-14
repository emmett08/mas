"""Private one-request stdlib transport worker (credentials arrive only on stdin).

The parent enforces a whole-operation deadline and kills/reaps this process.
This includes DNS, TLS, slow response headers and slow body delivery, even when
called by an orchestrator's worker thread. This helper never starts children.
"""
import base64
import http.client
import json
import ssl
import sys
from urllib.parse import urlsplit


def main():
    connection = None
    try:
        wire = sys.stdin.buffer.read(2 * 1024 * 1024 + 1)
        if len(wire) > 2 * 1024 * 1024:
            raise ValueError()
        request = json.loads(wire)
        endpoint = urlsplit(request["endpoint"])
        cls = http.client.HTTPSConnection if endpoint.scheme == "https" else http.client.HTTPConnection
        options = {"timeout": request["timeout_seconds"]}
        if endpoint.scheme == "https":
            options["context"] = ssl.create_default_context()
        connection = cls(endpoint.hostname, endpoint.port, **options)
        body = base64.b64decode(request["body"], validate=True) if request["body"] is not None else None
        connection.request(request["method"], endpoint.path or "/", body=body, headers=request["headers"])
        response = connection.getresponse()
        maximum = request["max_response_bytes"]
        data = response.read(maximum + 1)
        if len(data) > maximum:
            raise ValueError()
        value = {"status": response.status,
                 "headers": {k.lower(): v for k, v in response.getheaders()
                             if k.lower() in {"content-type", "www-authenticate", "retry-after", "location"}},
                 "body": base64.b64encode(data).decode("ascii")}
        sys.stdout.write(json.dumps(value, separators=(",", ":")))
    except Exception:
        # No upstream exception, credential, response body or URL is logged.
        sys.stdout.write('{"error":"transport_failure"}')
    finally:
        if connection is not None:
            connection.close()


if __name__ == "__main__":
    main()
