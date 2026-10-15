# Shared payload archival contract

An authorised terminal lifecycle archive is a data-retention change, not a new
execution or a budget reset. It preserves task, execution, run, message, call,
request, remote-job and command identities, state, original binding digests and
cumulative allocation counters. It records hashes before removing retained
input, result and detailed journal payloads. Active ownership, uncertain effects,
pending deliveries and retained artifact references prevent archival.

Schema 10 records each new host command's operation, structural lifecycle/run
targets and original receipt digest. These targets come from explicit domain
fields and authoritative relations, never a search through actor input. The
archive transaction replaces applicable command receipt payloads with
`mas-archived-command/1` tombstones containing the command ID, original request
digest, original receipt digest, targets and archival time. It does not delete
command identities or release receipt capacity.

Administrative retention receipts remain available after archival. In particular,
`operator.archive` and `artifact.bind`, `artifact.release` and `artifact.retire`
retain their operator, reason, request identity and compact lifecycle/reference
metadata. Their host receipts record the operation but have no payload-compaction
targets: they contain the evidence authorising the retention change, not copies
of actor inputs or results. This preserves the audit trail that explains why a
historical result became unavailable.

Before archival, an exact command retry returns its original receipt. After
archival, that retry returns the fixed `command_result_archived` error and cannot
apply the command again. Reusing the same command ID with different parameters
still fails the original digest comparison. The trusted `command_receipt` query
returns the compact tombstone. Public task inspection reports archival and
result unavailability; the original result endpoint returns `result_unavailable`.
Applications must treat this as authorised content removal, never retry the
effect under a fresh identity.

The same transaction installs a `mas-archived-run/1` tombstone. New ownership,
resume, native protocol operations and effect admission for that run are closed.
Host step, invocation and resolution payloads and native message/call result
payloads are compacted, with ordered record counts and SHA-256 evidence retained.
Completed remote/Cloud job and worker payload copies are compacted through their
own domain repositories; operation identities and unresolved observation facts
are retained. Physical PostgreSQL disk reclamation remains a database maintenance
operation; application archival removes reachable payload values atomically.

Experimental schemas before 10 retained command digests and receipts without
structural targets. Some receipts contain a step payload but no run identifier,
so their target cannot be reconstructed reliably. Such unmapped history causes
`legacy_archive_mapping_required`; the transaction rolls back all compaction.
An operator must supply a separately reviewed legacy mapping migration before
content archival. The service never guesses a target or erases unrelated live
receipt history. Fresh namespaces install the complete schema before accepting
commands and have structural mappings from their first receipt.

The actual PostgreSQL test captures historical command identities, archives a
completed task, retries a lost historical response, checks that the run cannot
be claimed again, searches native storage for the original input marker and
verifies that the admitted call and all allocation counters remain unchanged.
