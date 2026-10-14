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


def read_response(response, maximum, sse_response_id=None):
    """Stop an opted-in SSE exchange at its complete correlated response event.

    Generic HTTP callers still read the bounded whole body. SSE comments and
    notifications count towards the same aggregate byte limit. This is framing
    only: the MCP caller subsequently validates the entire response semantics.
    """
    media = response.getheader("Content-Type", "").split(";", 1)[0].strip().lower()
    if media != "text/event-stream" or sse_response_id is None:
        data = response.read(maximum + 1)
        if len(data) > maximum:
            raise ValueError()
        return data
    captured, line, data_lines = bytearray(), bytearray(), []
    skip_lf, first_line = False, True

    def complete_line():
        nonlocal first_line
        value = bytes(line)
        line.clear()
        if first_line:
            value = value.removeprefix(b"\xef\xbb\xbf")
            first_line = False
        if value:
            field, separator, content = value.partition(b":")
            if field == b"data":
                data_lines.append(content.removeprefix(b" ") if separator else b"")
            return False
        if not data_lines:
            return False
        try:
            event = json.loads(b"\n".join(data_lines))
            return (type(event) is dict and event.get("jsonrpc") == "2.0"
                    and type(event.get("id")) is type(sse_response_id)
                    and event.get("id") == sse_response_id and "method" not in event
                    and ("result" in event or "error" in event))
        except (ValueError, UnicodeError):
            return False
        finally:
            data_lines.clear()

    while True:
        chunk = response.read1(min(8192, maximum + 1 - len(captured)))
        if not chunk:
            return bytes(captured)
        for byte in chunk:
            captured.append(byte)
            if len(captured) > maximum:
                raise ValueError()
            if skip_lf:
                skip_lf = False
                if byte == 10:
                    continue
            if byte in (10, 13):
                skip_lf = byte == 13
                if complete_line():
                    return bytes(captured)
            else:
                line.append(byte)


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
        data = read_response(response, maximum, request.get("sse_response_id"))
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
