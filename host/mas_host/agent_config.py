"""Explicit arbitrary-actor configuration for structured inference strategies.

The original invoice ProviderConfig remains a compatibility interface. This
module accepts any registered actor and binds instructions and operation schemas
without introducing application-specific roles into the host.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

from .codec import is_actor_identity
from .cursor_transport import SDK_DEFAULT, validate_transport_policy
from .operation_contracts import OperationContract
from .provider_config import read_api_key
from .provider_contract import ProviderError
from .structured_provider import (IDENTIFIER, MockStructuredProvider, StructuredAgentAdapter,
                                  StructuredProvider, StructuredRequest, _copy)


@dataclass(frozen=True)
class AgentConfig:
    actor: str
    provider: str
    model: str
    reservation_microusd: int
    instructions: str
    contract: OperationContract
    role: str = "agent"
    max_output_tokens: int | None = None
    cursor_transport: str = SDK_DEFAULT

    def __post_init__(self) -> None:
        if not is_actor_identity(self.actor):
            raise ProviderError("agent requires a valid registered actor identity")
        for value in (self.role, self.model):
            if type(value) is not str or not IDENTIFIER.fullmatch(value):
                raise ProviderError("agent requires explicit valid actor, role and model identities")
        if type(self.provider) is not str or self.provider not in {"mock", "openai", "cursor"}:
            raise ProviderError("structured provider must be mock, openai or cursor")
        amount = self.reservation_microusd
        if (type(amount) is not int or not 0 <= amount <= 100_000_000
                or (self.provider == "mock") != (amount == 0)):
            raise ProviderError("mock reservations must be zero and live reservations positive and bounded")
        if self.max_output_tokens is not None and (type(self.max_output_tokens) is not int
                or not 1 <= self.max_output_tokens <= 16_384):
            raise ProviderError("agent requires a bounded positive output token limit")
        if self.provider != "openai" and self.max_output_tokens is not None:
            raise ProviderError("output token limits apply only to the OpenAI structured provider")
        try:
            validate_transport_policy(self.cursor_transport)
        except (ValueError, TypeError):
            raise ProviderError("invalid Cursor transport policy") from None
        if self.provider != "cursor" and self.cursor_transport != SDK_DEFAULT:
            raise ProviderError("Cursor transport applies only to Cursor providers")
        if self.provider == "cursor" and self.model == "auto-smart":
            raise ProviderError("Cursor requires an explicit catalogue model")
        if not isinstance(self.contract, OperationContract):
            raise ProviderError("agent requires a registered operation contract")
        StructuredRequest({}, self.instructions, self.contract.output_schema,
                          schema_name="operation_result", schema_version=self.contract.operation)

    def descriptor(self) -> dict:
        # Everything is public configuration. Credentials are read separately.
        return _copy({"actor": self.actor, "role": self.role, "provider": self.provider,
                      "model": self.model, "reservation_microusd": self.reservation_microusd,
                      "instructions": self.instructions, "contract": self.contract.descriptor,
                      "max_output_tokens": self.max_output_tokens,
                      "cursor_transport": self.cursor_transport})


def create_agent_providers(configs: Mapping[str, AgentConfig], *, registered_actors: Iterable[str],
                           mock_outputs: Mapping[str, dict] | None = None,
                           environ: Mapping[str, str] | None = None) -> dict[str, StructuredProvider]:
    """Validate all identities, fixture outputs and credentials before constructing clients.

Only configured model actors need providers: tool/human adapters may occupy the
remaining registered actors. Construction never submits an inference request.
"""
    registered = set(registered_actors)
    if (not configs or not set(configs).issubset(registered)
            or any(not isinstance(item, AgentConfig) or actor != item.actor
                   for actor, item in configs.items())):
        raise ProviderError("provider configuration must name matching registered actors")
    fixtures = {} if mock_outputs is None else dict(mock_outputs)
    if set(fixtures) != {actor for actor, item in configs.items() if item.provider == "mock"}:
        raise ProviderError("each mock actor requires exactly one explicit fixture output")
    fixtures = {actor: configs[actor].contract.validate_output(output)
                for actor, output in fixtures.items()}
    credentials = {name: read_api_key(name, environ=environ)
                   for name in sorted({item.provider for item in configs.values()} - {"mock"})}
    if any(item.provider == "cursor" for item in configs.values()):
        from .cursor_local import check_runtime
        check_runtime()
    providers = {}
    for actor, item in configs.items():
        if item.provider == "mock":
            provider = MockStructuredProvider(item.role, fixtures[actor], model=item.model)
        elif item.provider == "openai":
            from .structured_openai import OpenAIStructuredProvider
            provider = OpenAIStructuredProvider(item.model, item.role, api_key=credentials["openai"],
                reservation_microusd=item.reservation_microusd,
                max_output_tokens=4096 if item.max_output_tokens is None else item.max_output_tokens)
        else:
            from .structured_cursor import CursorStructuredProvider
            provider = CursorStructuredProvider(item.model, item.role, api_key=credentials["cursor"],
                reservation_microusd=item.reservation_microusd, transport_policy=item.cursor_transport)
        providers[actor] = provider
    return providers


def create_agent_adapters(configs: Mapping[str, AgentConfig], *, registered_actors: Iterable[str],
                          mock_outputs: Mapping[str, dict] | None = None,
                          environ: Mapping[str, str] | None = None) -> dict[str, StructuredAgentAdapter]:
    providers = create_agent_providers(configs, registered_actors=registered_actors,
                                      mock_outputs=mock_outputs, environ=environ)
    return {actor: StructuredAgentAdapter(provider, instructions=configs[actor].instructions,
                                         contract=configs[actor].contract, actor=actor)
            for actor, provider in providers.items()}


def load_agent_configs(value: Mapping[str, dict], *,
                       contracts: Mapping[str, OperationContract]) -> dict[str, AgentConfig]:
    """Load a JSON object; contracts are explicitly registered by the application."""
    if type(value) is not dict or not value:
        raise ProviderError("agent configuration must be a nonempty object")
    required = {"provider", "model", "reservation_microusd", "instructions", "operation"}
    optional = {"role", "max_output_tokens", "cursor_transport"}
    configs = {}
    for actor, item in value.items():
        if (type(item) is not dict or not required.issubset(item)
                or set(item) - required - optional or type(item.get("operation")) is not str
                or item["operation"] not in contracts):
            raise ProviderError("agent configuration requires a registered operation and explicit fields")
        contract = contracts[item["operation"]]
        if contract.operation != item["operation"]:
            raise ProviderError("registered operation identity does not match configuration")
        configs[actor] = AgentConfig(actor=actor, contract=contract,
                                    **{key: val for key, val in item.items() if key != "operation"})
    return configs
