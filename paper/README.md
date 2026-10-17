# MAS protocol article

[Read the PDF](mas-protocol.pdf) · [LaTeX source](mas-protocol.tex) · [Verification evidence](evidence/README.md)

**Target:** SoftwareX, Original Software Publication. The article describes an
implemented research-software contribution: durable communication and assessment
records with reproducible implementation tests and finite-state verification.
It does not claim improved model answers, production capacity, or a refinement
proof of the implementation.

The examined software revision is
`a61b3da98ef3ef11996dd79b4a3a048ed6c98ce1`. The manuscript and its evidence are
added later under `paper/`; the source citation deliberately names the examined
software, not a moving branch.

## Build and reproduce

Use TeX Live with `elsarticle`, `bytefield`, `algorithm`, `algpseudocode`, TikZ,
`natbib`, `xurl`, `hyperref`, `lineno`, Latin Modern, BibTeX and `latexmk`.
On Debian/Ubuntu these are supplied by `latexmk`, `lmodern`,
`texlive-latex-extra`, `texlive-science` and `texlive-publishers`.

```sh
make -C paper
```

The build writes `paper/mas-protocol.pdf`; intermediate files stay in the ignored
`paper/build/` directory. The committed PDF is a review draft, not evidence of
journal submission or acceptance.

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

The bytefield figure is generated from a 76-octet text message. Its optional
codec check verifies parsing and equality with the native canonical encoder:

```sh
cargo build --locked --manifest-path rust-protocol/Cargo.toml --bin mas-protocol-codec
python3 paper/scripts/generate-wire-figure.py \
  --codec "$PWD/rust-protocol/target/debug/mas-protocol-codec"
```

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
The publisher-hosted downloads were unavailable; a publisher-authored 2026
LaTeX template was inspected in a
[third-party mirror](https://github.com/yongyin-leon/CubeScope/blob/main/docs/softwarex-template/softwarex-osp-template.tex),
blob `c77c095a372789b9490ec20c6f61898bb5ac786f`. That copy specifies 4,000 words,
up to six figures, approximately 100 abstract words and the five section
headings used here. The manuscript adopts a stricter 3,000-word preparation
target because older indexed guidance gives that limit. The mirror is not
independent confirmation of the current submission portal.

| Requirement | Disposition |
|---|---|
| Motivation; software description; illustrative examples; impact; conclusions | Present in the required order |
| Elsevier source format and numbered references | `elsarticle`, 12-point A4 single column; `elsarticle-num`; review line numbers |
| Length | 111-word abstract; TeXcount reports 2,544 text, heading and caption words, excluding references and diagram labels |
| Architecture and other visual explanations | Three original vector figures, including a bytefield octet diagram |
| Faithful algorithms | Two `algorithm`/`algpseudocode` procedures checked against Rust source |
| Example and empirical evaluation | Three executed offline fixtures, 79 passing Rust tests and 41 classified formal checks |
| Code metadata C1–C8 | Present; C8 support email requires author confirmation |
| Open source and documentation | Root `LICENSE`, `THIRD_PARTY_LICENSE.txt`, `README.md` and linked guides; confirm that the portal accepts `LICENSE` where the mirrored template says `Licence.txt` |
| AI disclosure | Preparation statement and figure-caption disclosures follow the [current Elsevier policy](https://www.elsevier.com/about/policies-and-standards/generative-ai-policies-for-journals) |
| Author information | Name retained; affiliation, corresponding email, any co-authors and optional ORCID need author confirmation |
| Funding, competing interests and author contributions | Author must supply the applicable declarations; no negative declaration is invented |
| Final approval | Author must review text, sources, results and figures and accept responsibility before submission |
| Portal-specific requirements | Recheck the live guide, declaration forms and any required highlights at submission |

This package is a technically checked, journal-targeted draft. It is not yet a
complete submission because the author information and declarations above are
unresolved. An archival release/DOI would improve persistence; none is invented.

Software: Practice and Experience is a plausible alternative for an expanded
design-and-implementation account. JOSS is less suitable at this stage because
its [current submission criteria](https://joss.readthedocs.io/en/latest/submitting.html)
require more than six months of public development and demonstrated research
use. JAAMAS would require a stronger original theoretical or empirical
multi-agent contribution than the present implementation evidence establishes.

## Claim-to-implementation mapping

| Article component | Examined source or evidence |
|---|---|
| Text grammar, exact integers and limits | `rust-protocol/src/lib.rs` |
| Algorithm 1, identity, budgets, receive and acknowledgement | `rust-protocol/src/store.rs`, `tasks.rs`, `admission.rs` |
| Authority and IPC boundary | `rust-protocol/src/gateway.rs`, `host_ipc.rs` |
| Algorithm 2 and assessment reference binding | `rust-protocol/src/assessment/validation.rs`, `records.rs`, `helpers.rs` |
| In-memory history and revision limits | `rust-protocol/src/assessment/history.rs`; `formal/assessment-revision-notes.md` explicitly identifies cross-repository obligations |
| Offline observations | Star demo; `rust-protocol/examples/recovery.rs` and `gateway.rs`; output JSON in `evidence/raw-checks.zip` |
| Model assumptions and progress | `formal/protocol.pml.in`, task/transaction/expiry models, assessment model; `evidence/verification.json` |

## Editorial profile and verification

The requested Plain Words pass precedes New Hart's copy-editing. The effective
profile is British English with `-ise`, minimal clarifying hyphenation,
single primary quotation marks, punctuation by grammatical ownership,
day–month–year dates, words for ordinary zero–nine and numerals for measurements,
numbered citations, sentence-case headings and an Oxford comma only where it
prevents ambiguity. Identifiers, official titles, mathematics and LaTeX syntax
are protected. No chapter counter or published List of Algorithms is used in
this article class.

Review covers the manuscript, three figures, generated results and bibliography.
Forward and reverse editorial passes checked meaning and terminology; an
independent source review checked the algorithms and substantive claims.
Scanner findings in LaTeX commands/dimensions are controlled exceptions;
`implements` and `rendered` retain their exact software/typesetting meanings.
The identical-content/changed-content comparison has the same identity as its
explicit comparator. Long scanner spans across metadata and pseudocode are
markup artefacts, not single prose sentences.

The algorithm style checker passes. The manuscript is compiled with settled
references and inspected as rendered pages; overfull boxes and overlapping
diagram labels are corrected. The exact byte example passes the native parse
and canonical-encode checks. BibTeX entries are used in the manuscript and
identify primary sources; FIPA was omitted because its original specification
could not be inspected. These checks do not substitute for the author's final
scientific and publication approval.
