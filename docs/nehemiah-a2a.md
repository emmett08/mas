# Nehemiah and A2A

Nehemiah names the protocol retained in `emmett08/mas`. Existing repository,
crate, command and schema names stay unchanged. This naming change introduces
no new envelope version and requires no migration of stored messages.

## Responsibilities

| Concern | Nehemiah and its configured host | A2A interface |
|---|---|---|
| Participants | The host registers actors and freezes routes for a run | An Agent Card describes a service and its advertised skills |
| Compatibility | The host selects versions, operations and application schemas | The card lists supported interfaces, protocol versions and media modes |
| Authority | Trusted configuration binds callers to actors and admission limits | The endpoint authenticates clients and authorises requested operations |
| State | The repository retains message identities, delivery history and task transitions | Tasks, messages, status and artefacts form the remote exchange |
| Retry | Identical messages retain the original local admission | Send-message deduplication is permitted; its guarantee depends on the server |
| Effects | Host records and application observation establish execution outcomes | A remote task status does not establish exactly-once external effects |
| Output quality | Independent checks assess the application result | An Agent Card's skill description does not establish competence |

A fixed local workflow can use trusted configuration for identities, capabilities
and requirements. It needs no discovery exchange. A2A itself also permits static
configuration; dynamic discovery is a deployment choice.

An Agent Card becomes useful when an external client must find an endpoint,
select a supported interface, discover operations or learn authentication
requirements. Generate that card from the host's admitted configuration. Keep
service descriptions separate from credentials and authorisation decisions.
Descriptive A2A skills and media modes do not replace executable input/output
schema checks; expose those contracts through an explicitly declared extension.

## Host ownership and selected interoperability profile

The primary application host is the separate Rust
[mas-host](https://github.com/emmett08/mas-host). Native functions, CLI tools,
Python handlers and LLM adapters share its protocol admission path.
The Python `mas/host` remains transitional. Its existing A2A bridge must retain
its documented recovery guarantees until equivalent Rust behaviour is tested.
Application examples belong in
[mas-host-examples](https://github.com/emmett08/mas-host-examples).

The optional [native gateway](https://github.com/emmett08/mas-host/blob/main/docs/a2a.md)
uses JSON-RPC over HTTP. Generate its local operation description or start its
explicitly configured listener with:

```sh
mas-host describe --bundle application-bundle.json
mas-host serve-a2a --bundle application-bundle.json --state .service --config service.json
```

The public Agent Card exposes only approved public operations. An authenticated
`GetExtendedAgentCard` returns the caller's permitted operations and input/output
schemas. `SendMessage`, `GetTask` and `CancelTask` require the declared extension
`urn:mas:host-task-service:1`; its contracts preserve the host's exact-integer
JSON profile. Streaming, push notifications, `ListTasks`, continuations and
arbitrary file parts are outside this initial profile.

Bearer credentials map to configured principals and operation grants. The
listener binds only to literal loopback addresses. Remote deployment requires
an HTTPS reverse proxy and protection of the local listener; the gateway does
not implement OAuth. An ordinary local run starts no gateway listener.

The accepted compatibility boundary follows A2A release 1.0.1 and its interface
protocol version `1.0`. Cards use `supportedInterfaces`, `securitySchemes` and
`securityRequirements` as defined by the normative schema. A card signature verified against a trusted key binds its signed contents to
that key. A digest only detects content changes against a separately trusted
reference. Each mechanism needs an explicit trust policy.

For each remote invocation, the gateway must retain the remote message/task
identity, the admitted local actor and operation, and the selected contract.
It must recover an uncertain submission before creating another effect. A task
needs a separate state while waiting for input or authentication. Cancellation
records intent and observed outcome separately when the remote action may
already have occurred. These requirements belong at the host boundary and do
not require replacing the seven-field Nehemiah envelope.

The native service persists principal-scoped message identity and canonical
submission content before dispatch. Repeating the same identity and content
returns the retained task; changed content is rejected. Restart changes unfinished
submissions to `unknown` and suppresses automatic redispatch. Cancellation
records intent and remains unconfirmed while the outcome is uncertain. Local
core effect resolution does not settle or resume the service task. This initial
profile has no service-level settlement operation for retained unknown tasks.

State is bound to the bundle, configuration and host executable. Changing a token,
a grant or the executable requires an explicit deployment migration that retains
outstanding task and effect obligations. There is currently no migration command.
These limits distinguish the native profile from complete replacement parity
with the transitional Python host.

## Evidence and remaining work

The paper evaluates protocol revision `a61b3da` and a pinned scripted-provider
rehearsal. Those results remain historical. Later descriptor, gateway and Rust
host changes require their own implementation tests and bounded models. A
finite-state search establishes the checked abstraction under its stated
participants, fault choices and progress restrictions; it does not establish
unrestricted implementation liveness or deployment qualification.

The native host and example repositories retain their adapter tests and an
official A2A SDK interoperability check. Deployment qualification must cover
caller/route rejection, schema and
version mismatches, changed card/contract bindings, duplicate and conflicting
identities, restart after uncertain submission, cancellation races and terminal
state immutability. Pair the runtime traces with model transitions and retain
counterexamples from deliberately defective variants. Live-provider and remote
security qualification remain separate from offline tests.

Sources: [A2A 1.0.1 specification](https://a2a-protocol.org/v1.0.1/specification/),
[normative definitions](https://a2a-protocol.org/latest/definitions/),
[discovery options](https://a2a-protocol.org/latest/topics/agent-discovery/).
The implementation-specific [bridge documentation](bridges.md) governs the
transitional Python bridge; this document records protocol responsibilities
and the native-host direction.
