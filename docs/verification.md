# Protocol verification

`make check` builds and tests the Rust crate, checks the Promela models with
SPIN, and runs the protocol examples. The shared task-service trace corpus is
`formal/task-service-traces.json`; verification requires no Python host source.

The model contract, bounds, negative controls and implementation correspondence
are documented in [formal/README.md](../formal/README.md). Generated evidence is
retained by the Protocol checks workflow. Existing committed reports describe
their recorded revisions; they do not qualify later changes.

Python, container, bridge, orchestrator, workspace and independent-machine
qualifications are maintained in
[mas-reference-host](https://github.com/emmett08/mas-reference-host/blob/main/docs/verification.md).
The compatibility workflows here run that host at a reviewed revision against
the protocol commit under test. [Migration details](reference-host-migration.md)
explain the preserved required checks and downstream consumers.
