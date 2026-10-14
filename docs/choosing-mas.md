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

In this context, **bounded** means declared participants, routes, call/message
limits and deadlines, supplemented by application-specific authority and
acceptance checks. **Mixed agents** can be models, deterministic programs or
human-facing adapters. A **durable call allocation** records admission before
dispatch and survives a restart. An **uncertain effect** means that a call may
have changed an external system even though its response was lost. Its outcome
must be investigated before further action is authorised.

## Organisational use cases

The following are application designs, with their current implementation scope
stated explicitly. They do not establish improvements in accuracy, cost or
productivity; those require a comparison on representative tasks.

| Organisational problem | Proposed participants and routes | Existing starting point and remaining integration |
| --- | --- | --- |
| Check values extracted from documents | Extractor → reviewer → deterministic validator; failed checks return for correction. | The [invoice mesh](host-runner.md#what-the-example-does) computes and checks totals from supplied structured input. Document ingestion, extraction and validation against the original document require adapters. |
| Maintain a repository through remote agent jobs | Coordinator → coding job → tests → reviewer; publication follows acceptance. | [Cursor Cloud submission and reconciliation](providers.md#durable-cursor-cloud-jobs) are implemented. The complete testing/review/publication sequence requires application integration. |
| Investigate and repair a CI failure | Log collector → diagnosis → repair → targeted tests → reviewer. | Compose the [routing and evaluator–optimiser patterns](examples.md) with CI and repository adapters. The fixtures demonstrate control flow; they do not perform CI repairs. |
| Develop and check mathematical results | Proposers → critic → proof checker; numerical questions route to a simulation worker. | [Star/tree and review-loop fixtures](examples.md) demonstrate exchanges. Theorem representations, proof-assistant and simulation adapters, and mathematical acceptance rules remain application work. |
| Reanalyse data and update a manuscript | Data validator → analysis worker → interpretation → reviewer → document builder. | [Chaining and sequential-crew fixtures](examples.md) demonstrate stage dependencies. Dataset provenance, statistical checks, computation and publication require adapters. |
| Apply an infrastructure change | Planner → policy checker → authorised decision → deployment worker → health checker. | [Gateway controls](gateway.md) and [bridge integration](bridges.md) provide integration points. Deployment permissions, approval authentication, health checks and recovery policy remain application responsibilities. |
| Remediate a service incident | Classifier → diagnostics → approved remediation; ambiguous cases route to a person. | [Routing and simulated human-review fixtures](examples.md) demonstrate branches. Service diagnostics, authenticated decisions and remediation adapters must be supplied. |

### Limits, recovery and acceptance

1. **Document validation.** Bound document size, allowed fields and correction
   rounds. Accept only values supported by the source and the declared schema
   and arithmetic checks. Reuse saved successful calls after restart; investigate
   uncertain provider calls before repeating them. Use a parser and validator
   alone when the input format is predictable.
2. **Repository maintenance.** Fix the repository, source revision, permitted
   changes and repair budget. Require independent tests and review before merge.
   Preserve the remote agent/run identities; a submission receipt establishes
   acceptance of work, while a terminal service status still requires result
   review. Follow the [recovery walkthrough](providers.md#offline-failure-and-recovery-walkthrough).
   A single coding agent with ordinary CI can suffice when these coordination
   and recovery requirements are absent.
3. **CI repair.** Scope work to an identified failure and limit edits and test
   attempts. Accept the repair against declared checks on its exact revision.
   After an uncertain workflow dispatch, correlate the intended operation with
   CI records before considering another dispatch. A missing search result alone
   does not prove that dispatch failed. Use a script for known deterministic
   repairs; add agent roles when diagnosis and independent assessment warrant them.
4. **Mathematical research.** Declare assumptions, proof obligations, search
   limits and simulation budgets. Accept a formal result only under the selected
   checker's rules; simulation supplies separate evidence. Reconcile an uncertain
   compute-job submission using its scheduler identity. A prover or solver alone
   is a useful baseline when the problem representation is already fixed.
5. **Data analysis.** Bind work to immutable inputs, analysis settings and a
   finite computation budget. Check data coverage, analysis validity and whether
   reported claims follow from results. Recover completed computations against
   those same inputs; reconcile uncertain uploads before publishing another
   version. A reproducible analysis pipeline suffices when judgement between
   stages is unnecessary.
6. **Infrastructure changes.** Limit the target environment and permitted
   operations, with an authenticated approval policy and health criteria. The
   executor must enforce authority independently of model-generated text.
   Inspect deployment identity and observed state after a timeout; select any
   retry or compensating action through the application's recovery policy.
   Existing deployment tooling can own execution while MAS coordinates proposals
   and checks through [orchestrator bridges](orchestrators.md).
7. **Service remediation.** Restrict actions to an approved catalogue, named
   services and a maximum attempt count. Verify the intended health condition
   after action. Reconcile a restart request against service observations before
   another restart; route unresolved outcomes to an operator. A conventional
   runbook is sufficient when both diagnosis and remedy are predetermined.

Every application needs its own acceptance check. Separate model roles do not
establish independent evidence, and route permission does not by itself grant
permission to change a repository, deploy software or restart a service.

## Boundaries that affect the decision

Star, mesh and tree describe logical communication permissions over central
SQLite storage. `Runner` schedules synchronous nested delegation; membership
is frozen per run. The optional [workflow engine](composable-workflows.md)
adds durable parallel branches using separately owned Rust-backed runs, with
bounded processes and shared allocations on one Linux host. It supplies neither
a distributed worker pool nor high availability. Trusted adapters share a security
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
