# Choosing MAS for an organisational workflow

MAS suits trusted model, tool and human-facing adapters needing explicit
communication rules, persistent budgets and recoverable execution. Its supported
deployment is one Linux host with private local state. Organisations must own
their adapters, independent result checks and operational reconciliation.

## Which approach fits?

| Requirement | Starting point | Reason |
| --- | --- | --- |
| Bounded mixed-agent work with explicit routes, durable call allocations and uncertain-effect handling | MAS | These controls share a small Rust protocol core and a reusable Python host, with implementation tests and finite SPIN models. |
| One straightforward task or agent | Ordinary application code or an existing agent SDK | Extra messaging and recovery machinery need a specific justification. |
| Distributed workers, concurrent jobs and long-running business processes | [Temporal](https://docs.temporal.io/temporal) | Durable workflow execution and worker task queues address distributed orchestration. |
| Stateful agent graphs, streaming, human intervention and memory | [LangGraph](https://docs.langchain.com/oss/python/langgraph/overview) | These are explicit framework capabilities, including persistent checkpoints. |
| Independently deployed agents across teams or organisations | [A2A](https://a2a-protocol.org/latest/topics/what-is-a2a/) | Standardised discovery and remote peer collaboration address interoperability. |
| Connecting agents to tools and data | [MCP](https://modelcontextprotocol.io/docs/learn/architecture) | It standardises access to tools and resources. |

These approaches complement one another through the shipped [MCP, A2A and
orchestrator bridges](bridges.md). Integrations must assign ownership
of retries, effect identity and accounting to prevent competing recovery
mechanisms from repeating operations.

## What MAS contributes

The gateway checks actor identity, permitted routes, retained message identity
and task transitions. Durable admission reserves calls and expenditure
allocations before effect dispatch. Recovery returns recorded outputs and
preserves uncertain invocations for explicit reconciliation. Expiry closes
protocol tasks independently of a worker reply. Finite SPIN models and
deliberately defective variants test declared protocol properties.

Useful applications include document extraction followed by deterministic
validation, engineering proposals checked by trusted tools, and research
candidates assessed by a designated solver or proof checker. The invoice demo
illustrates these mechanics; it does not establish that several models improve
accuracy, cost or productivity.

## Boundaries that affect the decision

Star, mesh and tree describe logical communication permissions over central
SQLite storage. The supplied host schedules synchronous nested delegation;
membership is frozen per run. It supplies neither concurrent fan-out nor a
distributed worker pool or high availability. Trusted adapters share a security
boundary; process supervision is not isolation for hostile code.

Expenditure reservations are allowances rather than verified provider billing
limits. Unknown effects can require operator investigation. Neither task expiry
nor local process termination proves remote cancellation. [Temporal likewise
requires application-level attention to idempotency](https://docs.temporal.io/activity-definition)
because an activity can execute more than once.

SPIN verifies the documented finite models, not the complete Python, Linux or
provider implementation. It establishes neither result truth nor exactly-once
external effects. Live-provider acceptance remains unestablished until a
credentialled run produces evidence for the selected account and models.

## Adoption pilot

Compare the smallest useful MAS topology against a single-agent baseline on a
representative workflow. Define an independent acceptance check, then measure
useful-result rate, cost, latency and reconciliation effort. Exercise crash
recovery and unknown outcomes before deployment. Use the
[adapter guide](host-runner.md), [operations guide](operations.md) and
[acceptance procedure](deployment-acceptance.md) to make that decision from
application evidence.

External documentation consulted on 13 October 2026.
