#!/usr/bin/env python3
"""Exercise a disposable S3 bucket through installed MAS and authenticated HTTP.

Use --local-rustfs for an isolated local server, or explicitly supply a test
endpoint and credentials. The test creates and removes its own random bucket.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import uuid

from mas_host.artifacts import ArtifactError, ArtifactStore
from mas_host.bridges.artifact_client import ArtifactTransferClient
from mas_host.bridges.artifact_transfer import ArtifactScope, ArtifactTransferService
from mas_host.bridges.authentication import StaticTokenVerifier
from mas_host.bridges.server import BridgeHTTPServer
from mas_host.bridges.service_client import TaskServiceClient
from mas_host.s3_artifacts import S3ArtifactContent
from mas_host.s3_transport import S3ContentConfig, create_s3_client


@contextmanager
def local_server(binary, root):
    access, secret = "mas-test-" + uuid.uuid4().hex, uuid.uuid4().hex + uuid.uuid4().hex
    credentials = root / "credentials.json"
    credentials.write_text(json.dumps({"access_key_id": access, "secret_access_key": secret}))
    credentials.chmod(0o600)
    for name, value in (("access", access), ("secret", secret)):
        (root / name).write_text(value)
        (root / name).chmod(0o600)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    (root / "data").mkdir()
    with (root / "server.log").open("wb") as log:
        process = subprocess.Popen([str(Path(binary).resolve()), "server", "--address", f"127.0.0.1:{port}",
            "--access-key-file", str(root / "access"), "--secret-key-file", str(root / "secret"),
            "--region", "us-east-1", str(root / "data")], stdout=log, stderr=log)
        try:
            deadline = time.monotonic() + 20
            while True:
                if process.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError("local S3 server did not become ready")
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=.2):
                        break
                except OSError:
                    time.sleep(.05)
            yield f"http://127.0.0.1:{port}", credentials
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


def acceptance(config, root):
    client = create_s3_client(config)
    create = {"Bucket": config.bucket}
    if config.region != "us-east-1":
        create["CreateBucketConfiguration"] = {"LocationConstraint": config.region}
    client.create_bucket(**create)
    checks = []
    try:
        backend = S3ArtifactContent(config)
        store = ArtifactStore(root / "metadata" / "artifacts", namespace="result", principal="writer",
                              content_backend=backend)
        data = bytes(range(256)) * 521
        reference = store.put_bytes(data, readers=("reader",))
        assert store.read_bytes(reference) == data
        assert store.put_bytes(data, readers=("reader",)) == reference
        checks.append("immutable_roundtrip_and_duplicate")
        assert not list(store.directory.glob("*.blob"))
        checks.append("remote_bytes_with_local_metadata")
        # A real accepted PUT followed by a discarded response must preserve
        # quota/identity. The retry reaches a real S3 conditional-write conflict.
        class LoseReply:
            lost = False
            def put_object(self, **request):
                result = client.put_object(**request)
                if not self.lost:
                    self.lost = True
                    raise ConnectionError("discarded accepted PUT reply")
                return result
            def get_object(self, **request):
                return client.get_object(**request)
        recovering = ArtifactStore(root / "lost-reply", namespace="result", principal="writer",
            content_backend=S3ArtifactContent(config, client=LoseReply()))
        try:
            recovering.put_bytes(b"accepted before disconnect")
            raise AssertionError("lost response was not simulated")
        except ArtifactError:
            pass
        recovered = recovering.put_bytes(b"accepted before disconnect")
        assert recovering.read_bytes(recovered) == b"accepted before disconnect"
        checks.append("lost_put_reply_conditional_recovery")
        scopes = {"result": ArtifactScope(store.root, "result", {"alice": "writer", "bob": "reader", "mallory": "outsider"},
                                           content_backend=backend)}
        transfers = ArtifactTransferService(root / "transfers", scopes)
        tokens = {name: uuid.uuid4().hex + uuid.uuid4().hex for name in ("alice", "bob", "mallory")}
        verifier = StaticTokenVerifier([{"sha256": hashlib.sha256(token.encode()).hexdigest(),
            "principal": name, "scopes": ["mas:submit", "mas:read"]} for name, token in tokens.items()])
        server = BridgeHTTPServer(("127.0.0.1", 0), service=object(), verifier=verifier,
            public_url="http://127.0.0.1", allow_loopback_http=True, artifact_transfer=transfers,
            request_log=lambda _event: None)
        endpoint = "http://127.0.0.1:" + str(server.server_address[1])
        server.allowed_hosts = frozenset({endpoint.removeprefix("http://")})
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": .01})
        thread.start()
        try:
            clients = {}
            for name, token in tokens.items():
                file = root / (name + ".token")
                file.write_text(token)
                file.chmod(0o600)
                clients[name] = ArtifactTransferClient.from_service(TaskServiceClient(endpoint, principal=name,
                    token_file=file, allow_loopback_http=True))
            consumer = ArtifactStore(root / "consumer", namespace="copy", principal="consumer")
            downloaded = clients["bob"].materialize("result", reference, consumer)
            assert consumer.read_bytes(downloaded["reference"]) == data
            uploaded = clients["alice"].publish("result", consumer, downloaded["reference"], readers=("bob",))
            assert uploaded.sha256 == reference.sha256
            checks.append("authenticated_http_multichunk_upload_download")
            from mas_host.service_contracts import ServiceError
            try:
                clients["mallory"].materialize("result", reference, consumer)
                raise AssertionError("unauthorised content was disclosed")
            except ServiceError:
                pass
            clients["alice"].revoke("result", reference, readers=("bob",))
            try:
                clients["bob"].materialize("result", reference, consumer)
                raise AssertionError("revoked content was disclosed")
            except ServiceError:
                pass
            checks.append("principal_isolation_and_revocation")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(5)
        restored = root / "restored"
        shutil.copytree(root / "metadata", restored)
        (restored / ".restored-audit-only").write_text("audit only")
        audit = ArtifactStore(restored / "artifacts", namespace="result", principal="writer", readonly=True,
                              content_backend=backend)
        assert audit.read_bytes(reference) == data
        checks.append("metadata_restore_preserves_remote_reference")
        manifest = json.loads((store.directory / "object-content.json").read_text())
        key = f"{config.prefix}/{manifest['store_id']}/result/{reference.sha256}.blob"
        client.put_object(Bucket=config.bucket, Key=key, Body=b"corrupt", ContentLength=7)
        try:
            store.read_bytes(reference)
            raise AssertionError("corrupt object was exposed")
        except ArtifactError:
            pass
        checks.append("server_side_tampering_rejected")
        import boto3, botocore
        return {"ok": True, "profile": "s3-content-and-authenticated-http/1", "checks": checks,
                "boto3": boto3.__version__, "botocore": botocore.__version__,
                "live_provider_calls": 0, "object_storage": config.descriptor}
    finally:
        # The bucket is random and created by this test. Never enumerate/remove
        # objects in a user-selected existing bucket.
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=config.bucket):
            for item in page.get("Contents", []):
                client.delete_object(Bucket=config.bucket, Key=item["Key"])
        client.delete_bucket(Bucket=config.bucket)
        client.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local-rustfs", type=Path)
    parser.add_argument("--endpoint")
    parser.add_argument("--credentials-file", type=Path)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--ca-file", type=Path)
    args = parser.parse_args()
    if bool(args.local_rustfs) == bool(args.endpoint) or bool(args.endpoint) != bool(args.credentials_file):
        parser.error("choose --local-rustfs OR --endpoint with --credentials-file")
    with tempfile.TemporaryDirectory(prefix="mas-s3-acceptance-") as tmp:
        root = Path(tmp)
        bucket = "mas-acceptance-" + uuid.uuid4().hex
        if args.local_rustfs:
            with local_server(args.local_rustfs, root) as (endpoint, credentials):
                config = S3ContentConfig(endpoint, bucket, "evidence", "us-east-1", credentials, allow_loopback_http=True)
                report = acceptance(config, root)
            report["server"] = subprocess.check_output([str(args.local_rustfs.resolve()), "--version"], text=True).strip()
        else:
            config = S3ContentConfig(args.endpoint, bucket, "evidence", args.region, args.credentials_file, ca_file=args.ca_file)
            report = acceptance(config, root)
        print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
