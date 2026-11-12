"""The published document contract: plan documents and their snapshots.

Yggdrasil's plan document and its ``plan_status`` snapshot are written and read
by programs released separately from each other, so their format is a
versioned contract. Every plan document and snapshot this release writes
carries a ``schema_version``: a positive integer per document kind that names
a compatibility family, not a software version. This package holds the
contract's runtime core:

- the document kinds, and the families this release writes and supports
  (below);
- :mod:`yggdrasil.contract.problems`: how a problem is reported, one record and
  one set of codes shared by validation, the control checks and the reader;
- :mod:`yggdrasil.contract.fields`: every field name and vocabulary word of the
  contract, spelled once; a leaf that imports nothing from the package;
- :mod:`yggdrasil.contract.versions`: reading a document's ``schema_version``;
- :mod:`yggdrasil.contract.controls`: the checks on the control fields that
  decide whether and how a plan runs (the execution tokens, the failure
  policy, the execution definition), each reporting its problems as text and
  as structured records;
- :mod:`yggdrasil.contract.timestamps`: the canonical timestamp spelling that
  writers produce, and the reading and formatting of instants;
- :mod:`yggdrasil.contract.diagnostics`: showing a recorded value in a
  diagnostic, bounded and never raising;
- :mod:`yggdrasil.contract.read`: the shared reader, one interpretation of
  what a stored plan document and its snapshot say, for Yggdrasil's own
  decisions and for any other program that reads the documents.

It also publishes the contract itself: one handwritten JSON Schema per kind
and family, in ``schemas/``, which :mod:`yggdrasil.contract.schema` loads and
validates documents against. Validation is a development and test tool, never
part of the runtime checks above, and needs the ``jsonschema`` package from
the ``dev`` extra.

Layering: the package sits in the bottom layer, beside ``config``,
``logging_utils`` and ``errors``, and imports only the standard library, so
any layer can use it and importing it loads neither the CouchDB client, the
logging setup nor a validation library. It never reads the clock: callers pass
every timestamp in.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

# Document kinds, by their contract names.
PLAN_DOCUMENT = "plan_document"
PLAN_STATUS = "plan_status"
# The attempt report has no marker of its own: it is versioned through the
# plan document that embeds it, so it appears in neither mapping below.
ATTEMPT_REPORT = "attempt_report"

# The family this release's builders write, per kind.
CURRENT_FAMILIES: Mapping[str, int] = MappingProxyType(
    {PLAN_DOCUMENT: 1, PLAN_STATUS: 1}
)

# The families this release inspects, mutates and executes, per kind. One set
# serves all three: this release supports no family for one capability only.
# Read-only, so no caller can widen support at run time.
SUPPORTED_FAMILIES: Mapping[str, frozenset[int]] = MappingProxyType(
    {PLAN_DOCUMENT: frozenset({1}), PLAN_STATUS: frozenset({1})}
)
