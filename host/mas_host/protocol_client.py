"""Bounded subprocess transport for the authoritative Rust protocol tools."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
from typing import Protocol

from .contracts import RunnerError


class ProtocolTransport(Protocol):
    """Small transport port; Rust command semantics remain authoritative."""

    mas: str
    gateway: str

    def command(self, args: list[str], source: str | None = None, *,
                timeout: float = 10, empty_response: bool = False): ...

    def validate_gateway(self, database, configuration, *, timeout: float = 10) -> None: ...


class ProtocolClient:
    """Own command execution, credential-free environments and safe diagnostics."""

    def __init__(self, mas_bin: str = "mas", gateway_bin: str = "mas-gateway"):
        self.mas = shutil.which(str(mas_bin))
        self.gateway = shutil.which(str(gateway_bin))
        if not self.mas or not self.gateway:
            raise RunnerError("mas and mas-gateway binaries must be installed")

    def command(self, args: list[str], source: str | None = None, *,
                timeout: float = 10, empty_response: bool = False):
        try:
            result = subprocess.run(args, input=source, encoding="utf-8",
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    timeout=timeout, check=False,
                                    env={"PATH": os.environ.get("PATH", os.defpath)})
            if empty_response:
                if result.returncode or result.stdout:
                    raise RunnerError("protocol operation rejected; inspect run status")
                return None
            response = json.loads(result.stdout)
            if result.returncode or response.get("ok") is not True:
                raise RunnerError("protocol operation rejected; inspect run status")
            return response["result"]
        except (OSError, subprocess.TimeoutExpired, UnicodeError, ValueError, KeyError, AttributeError):
            # Never attach subprocess output, provider credentials or request bodies.
            raise RunnerError("protocol command unavailable or interrupted") from None

    def validate_gateway(self, database, configuration, *, timeout: float = 10) -> None:
        # Opening an empty broker session validates/fixes its policy fingerprint,
        # without consuming a request attempt or authorising any external effect.
        self.command([self.gateway, "serve", str(database), str(configuration)], "",
                     timeout=timeout, empty_response=True)
