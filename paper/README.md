# MAS protocol article

[Read the PDF](mas-protocol.pdf) · [LaTeX source](mas-protocol.tex) · [Verification evidence](evidence/README.md)

**Target:** SoftwareX, Original Software Publication. The article describes an
implemented research-software contribution: durable communication and assessment
records with reproducible implementation tests and finite-state verification.
The reported results concern the selected local implementation, finite models
and a retained incident-analysis rehearsal with scripted providers.

The examined software revision is
`a61b3da98ef3ef11996dd79b4a3a048ed6c98ce1`. The manuscript and its evidence are
added later under `paper/`. The source citation identifies that examined revision.

## Build and reproduce

Use TeX Live with `elsarticle`, `bytefield`, `pgf-umlsd`, `tikz-cd`, `algorithm`, `algpseudocode`, TikZ,
`natbib`, `xurl`, `hyperref`, `lineno`, Latin Modern, BibTeX and `latexmk`.
On Debian/Ubuntu these are supplied by `latexmk`, `lmodern`,
`texlive-latex-extra`, `texlive-pictures`, `texlive-science` and `texlive-publishers`.

```sh
make -C paper
```

The build writes `paper/mas-protocol.pdf`. Intermediate files stay in the ignored
`paper/build/` directory. The committed PDF is a review draft.

To rerun the selected Rust, SPIN and offline-fixture checks, install the pinned
Rust toolchain, SPIN, a C compiler and Python 3 on Linux, then run:

```sh
python3 paper/scripts/reproduce-evidence.py --out /tmp/mas-paper-reproduction
```

Add `--offline` when the Cargo.lock dependencies are cached. Omitting `--out`
replaces the retained evidence in this folder. The evidence script records
commands, source hashes, versions, outcomes and raw logs. It generates the two
results tables from `verification.json`. See [the record](evidence/README.md)
for the distinction between positive searches, expected counterexamples and
reachability witnesses.

The bytefield figure shows named fields and a variable-length payload, using the
field-oriented style of the supplied packet-format example. MAS has a text
encoding, so the schematic shows field order and logical grouping. Its 76-octet
canonical example is retained in `figures/wire.txt`.
The optional codec check verifies parsing and equality with the native encoder:

```sh
cargo build --locked --manifest-path rust-protocol/Cargo.toml --bin mas-protocol-codec
python3 paper/scripts/generate-wire-figure.py \
  --codec "$PWD/rust-protocol/target/debug/mas-protocol-codec"
```

The two `pgf-umlsd` sequence diagrams show permitted host-level API exchanges:
recovery after a lost send response, and task closure before delivery
acknowledgement. They describe protocol behaviour. The separate executed-example
table reports observations. Gateway forwarding and external work are omitted
from the sequence diagrams.

The architecture figure separates the permitted actor exchanges from the
shared implementation, with direct labels for each component's responsibility.
The state figure uses `tikz-cd` to align five explicitly labelled transitions
from an open task. Its separate delivery panel shows receipt, repeated receipt
and acknowledgement. Both figures use fixed text of at least 9 pt, vector
arrows and greyscale distinctions. Their specifications, rendered alternatives,
mechanical audits and independent reviews accompany the source files.

The sixth figure compares the recorded one-pass, revision and resampling
procedures in the external incident-analysis example. It shows call order,
candidate selection and feedback dependencies. The scripted-provider rehearsal
contains 108 completed procedure runs and 324 provider calls. The revision
procedure retains 72 candidate records and 72 assessment records.

To verify and regenerate these observations, check out the cited examples
revision and run the read-only analysis:

```sh
git clone https://github.com/emmett08/mas-host-examples.git /tmp/mas-host-examples
git -C /tmp/mas-host-examples checkout f086e3e51d4c18c2ff851a00b70422bdbfd8aa50
python3 paper/scripts/analyse-incident-rehearsal.py /tmp/mas-host-examples
python3 paper/scripts/generate-incident-figure.py
```

The analysis verifies the archived file manifest, frozen source identities,
invocation inputs, candidate and assessment references, effect counts and report
calculations. It reads retained records and makes zero provider calls. The
external archive remains at its pinned source URL. The paper retains the derived
summary and figure sources. See [the evidence record](evidence/README.md) for
the distinction between archive inspection and the original host execution.

## Journal fit and submission checks

SoftwareX's [official scope](https://www.elsevier.com/researcher/author/tools-and-resources/research-elements-journals)
covers research software and its application. Its
[reviewer form](https://legacyfileshare.elsevier.com/promis_misc/softwarex-reviewer-form.pdf)
asks for a component overview, empirical evaluation, complete metadata,
installation instructions, tests, documentation and an open-source licence.
These requirements fit the evidence available here better than an article
claiming a new multi-agent theory or improved reasoning.

The publisher's [live author guide](https://www.sciencedirect.com/journal/softwarex/publish/guide-for-authors)
could not be retrieved on 17 October 2026 (HTTP 403/unavailable). Elsevier's
official page links its [OSP template](https://legacyfileshare.elsevier.com/promis_misc/softwarex-osp-template.docx).
The publisher-hosted downloads were unavailable. A publisher-authored 2026
LaTeX template was inspected in a
[third-party mirror](https://github.com/yongyin-leon/CubeScope/blob/main/docs/softwarex-template/softwarex-osp-template.tex),
blob `c77c095a372789b9490ec20c6f61898bb5ac786f`. That copy specifies 4,000 words,
up to six figures, approximately 100 abstract words and the five section
headings used here. The expanded article follows that template's 4,000-word
and six-figure limits. Older indexed guidance gives a 3,000-word limit.
Current submission-portal requirements still require confirmation.

| Requirement | Disposition |
|---|---|
| Motivation, software description, illustrative examples, impact and conclusions | Present in the required order |
| Elsevier source format and numbered references | `elsarticle`, 12-point A4 single column, `elsarticle-num` and review line numbers |
| Length | 103-word abstract. TeXcount reports 3,424 text, heading and caption words, excluding references, diagram labels and mathematical counters |
| Architecture and other visual explanations | Six vector figures: architecture, bytefield envelope structure, state transitions, two `pgf-umlsd` sequences and the recorded incident procedures |
| Faithful algorithms | Two `algorithm`/`algpseudocode` procedures checked against Rust source |
| Example and empirical evaluation | Three executed offline fixtures, 79 passing Rust tests, 41 classified formal checks and an independently inspected archive of 108 scripted-provider incident runs |
| Code metadata C1–C8 | C8 gives the public support channel. The required support email awaits author confirmation |
| Open source and documentation | Root `LICENSE`, `THIRD_PARTY_LICENSE.txt`, `README.md` and linked guides. Confirm that the portal accepts `LICENSE` where the mirrored template says `Licence.txt` |
| AI disclosure | Concise provenance remains in the formal declaration and figure captions, as required by the [current Elsevier policy](https://www.elsevier.com/about/policies-and-standards/generative-ai-policies-for-journals). Final author oversight and responsibility wording awaits confirmation |
| Author information | Name retained. Affiliation, corresponding email, any co-authors and optional ORCID need author confirmation |
| Funding, competing interests and author contributions | Author must supply the applicable declarations |
| Final approval | Author must review text, sources, results and figures and accept responsibility before submission |
| Portal-specific requirements | Recheck the live guide, declaration forms and any required highlights at submission |

This package is a technically checked, journal-targeted draft. A complete
submission requires the author information and declarations listed above. An
archival release/DOI would improve persistence.

Software: Practice and Experience is a plausible alternative for an expanded
design-and-implementation account. JOSS is less suitable at this stage because
its [current submission criteria](https://joss.readthedocs.io/en/latest/submitting.html)
require more than six months of public development and demonstrated research
use. JAAMAS would require a stronger original theoretical or empirical
multi-agent contribution than the present implementation evidence establishes.

## Claim-to-implementation mapping

| Article component | Examined source or evidence |
|---|---|
| Holzmann's five elements and their MAS implementation | Holzmann (1991), Section 2.2, p. 21 and Section 2.7, pp. 36–37. `docs/design.md`, codec, store, task and gateway sources |
| Text grammar, exact integers and limits | `rust-protocol/src/lib.rs` |
| Algorithm 1, identity, budgets, receive and acknowledgement | `rust-protocol/src/store.rs`, `tasks.rs`, `admission.rs` |
| Authority and IPC boundary | `rust-protocol/src/gateway.rs`, `host_ipc.rs` |
| Algorithm 2 and assessment reference binding | `rust-protocol/src/assessment/validation.rs`, `records.rs`, `helpers.rs` |
| In-memory history and revision limits | `rust-protocol/src/assessment/history.rs`. `formal/assessment-revision-notes.md` explicitly identifies cross-repository obligations |
| Offline observations | Star demo, `rust-protocol/examples/recovery.rs` and `gateway.rs`. Output JSON in `evidence/raw-checks.zip` |
| Model assumptions and progress | `formal/protocol.pml.in`, task/transaction/expiry models, assessment model. `evidence/verification.json` |
| Incident revision integration | Pinned `mas-host-examples` source and rehearsal archive. `evidence/incident-rehearsal.json` derives counts and checks lineage, inputs and report calculations |

## Editorial profile and verification

The requested Plain Words pass precedes New Hart's copy-editing. The effective
profile is British English with `-ise`, minimal clarifying hyphenation,
single primary quotation marks, punctuation by grammatical ownership,
day–month–year dates, words for ordinary zero–nine and numerals for measurements,
numbered citations, sentence-case headings and an Oxford comma only where it
prevents ambiguity. Identifiers, official titles, mathematics and LaTeX syntax
are protected. No chapter counter or published List of Algorithms is used in
this article class.

The revised prose states supported properties, their assumptions and application
responsibilities positively. Semicolons are reserved for the actual MAS
terminator and source-code syntax. The five-element design explanation connects
service operations, environmental premises, message meanings, encoding and
transaction-ordered exchange rules to their respective verification checks.

Review covers the manuscript, six figures, generated results and bibliography.
Forward and reverse editorial passes checked meaning and terminology. An
independent source review checked the algorithms and substantive claims.
Scanner findings in LaTeX commands/dimensions are controlled exceptions.
`implements` retains its exact software meaning.
The identical-content/changed-content comparison has the same identity as its
explicit comparator. Long scanner spans across metadata and pseudocode are
markup artefacts.

The algorithm style checker passes. The manuscript is compiled with settled
references and inspected as rendered pages. Overfull boxes and overlapping
diagram labels are corrected. The exact byte example passes the native parse
and canonical-encode checks. BibTeX entries are used in the manuscript and
identify primary sources. FIPA was omitted because its original specification
could not be inspected. The author must complete the final scientific review
and publication approval.

Drafting-status text, author instructions and descriptions of typesetting tools
are excluded from the scientific text. Necessary implementation and verification
methods remain. The figure-caption and end-matter AI declarations record the
provenance required by the publisher.
