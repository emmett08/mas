"""Structured inference over the same bounded OpenAI Responses client policy."""
from __future__ import annotations

import json
import re
from typing import Any

from .contracts import Invocation
from .openai_provider import _create_client
from .provider_contract import ProviderError
from .structured_provider import IDENTIFIER, MAX_OUTPUT_BYTES, StructuredRequest, validate_trace_id


class OpenAIStructuredProvider:
    def __init__(self, model: str, role: str, *, api_key: str | None = None,
                 client: Any = None, reservation_microusd: int = 100_000,
                 max_output_tokens: int = 4096):
        if type(max_output_tokens) is not int or not 1 <= max_output_tokens <= 16_384:
            raise ProviderError("structured output requires a bounded positive token limit")
        if type(model) is not str or not IDENTIFIER.fullmatch(model):
            raise ProviderError("model must be an explicit valid model identifier")
        if type(role) is not str or not IDENTIFIER.fullmatch(role):
            raise ProviderError("role must be an explicit valid role identifier")
        if type(reservation_microusd) is not int or not 1 <= reservation_microusd <= 100_000_000:
            raise ProviderError("OpenAI calls require a positive bounded spending reservation")
        self.model, self.role = model, role
        self.reservation_microusd = reservation_microusd
        self.max_output_tokens = max_output_tokens
        self._client = _create_client(api_key) if client is None else client

    def descriptor(self) -> dict:
        return {"provider": "openai", "model": self.model, "role": self.role,
                "contract": "structured-inference/1", "implementation": "openai-responses-structured/1",
                "endpoint": "https://api.openai.com/v1", "tools": [], "store": False,
                "timeout_seconds": 45, "max_retries": 0,
                "max_output_tokens": self.max_output_tokens, "max_output_bytes": MAX_OUTPUT_BYTES,
                "reservation_microusd": self.reservation_microusd}

    def infer(self, request: StructuredRequest, *, request_id: str | None = None) -> Invocation:
        validate_trace_id(request_id)
        supplied = request.descriptor
        try:
            response = self._client.responses.create(
                model=self.model,
                input=[{"role": "developer", "content": supplied["instructions"]},
                       {"role": "user", "content": json.dumps(supplied["input"], separators=(",", ":"))}],
                text={"format": {"type": "json_schema", "name": supplied["schema_name"],
                                 "strict": True, "schema": supplied["output_schema"]}},
                tools=[], store=False, max_output_tokens=self.max_output_tokens,
                extra_headers={"X-Client-Request-Id": request_id} if request_id else {})
        except Exception:
            raise ProviderError("OpenAI invocation failed; its external outcome may be unknown") from None
        if getattr(response, "status", None) != "completed":
            raise ProviderError("OpenAI response did not complete")
        for item in getattr(response, "output", []) or []:
            if getattr(item, "type", None) in {"function_call", "custom_tool_call"}:
                raise ProviderError("OpenAI returned a tool call in text-only mode")
            for content in getattr(item, "content", []) or []:
                if getattr(content, "type", None) == "refusal":
                    raise ProviderError("OpenAI refused the structured request")
        output = request.parse_output(getattr(response, "output_text", None))
        evidence = {"provider": "openai", "model": self.model, "role": self.role,
                    "request_sha256": request.identity,
                    "reserved_microusd": self.reservation_microusd,
                    "spending_basis": "configured_reservation_not_provider_invoice"}
        if request_id:
            evidence["client_request_id"] = request_id
        for field, value, pattern in (
                ("server_request_id", getattr(response, "_request_id", None), r"[A-Za-z0-9_-]{1,200}"),
                ("response_id", getattr(response, "id", None), r"resp_[A-Za-z0-9_-]{1,200}")):
            if type(value) is str and re.fullmatch(pattern, value):
                evidence[field] = value
        usage = getattr(response, "usage", None)
        tokens = {field: getattr(usage, field) for field in ("input_tokens", "output_tokens", "total_tokens")
                  if type(getattr(usage, field, None)) is int and 0 <= getattr(usage, field) <= 10**9}
        if tokens:
            evidence["usage"] = tokens
        return Invocation(output, evidence)
