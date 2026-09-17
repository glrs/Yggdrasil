# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Environment & Commands

Activate the conda environment before running any tooling: `conda activate ygg` (it has pytest, ruff, black, mypy installed).

```bash
pytest                                        # full suite (~1700 tests)
pytest tests/watchers/test_manager.py         # single file
pytest tests/watchers/test_manager.py -k name # single test
ruff check .                                  # lint (rules: E4/E7/E9, F, I, UP)
black . --check                               # formatting
mypy .                                        # type checking (config in pyproject.toml)
```

Tests are unittest-style with `unittest.mock`; async code is tested via `asyncio.run(...)` inside sync test methods (pytest-asyncio is installed but tests don't use `@pytest.mark.asyncio`).

Pre-commit hooks: ruff/black/mypy at commit time, full mypy + full pytest at push time (`pre-commit install` and `pre-commit install --hook-type pre-push`). Locked deps are regenerated with `pip-compile --strip-extras -o requirements/lock.txt`.

PRs go against the `dev` branch. Version comes from git tags via setuptools-scm.

## Architecture

Yggdrasil is an event-driven orchestration framework: watchers observe external sources (CouchDB `_changes`, filesystem), route change events to **realm** modules, which produce **plans** that the **Engine** executes.

Event flow, end to end:

1. **`YggdrasilCore`** (`lib/core_utils/yggdrasil_core.py`) is the central orchestrator. It discovers realms, collects their WatchSpecs, wires watchers, and routes events to handlers via a `dict[EventType, list[BaseHandler]]` subscription model.
2. **Realms** are the plugin extension point. Each provides a `RealmDescriptor` (`realm_id`, `handler_classes`, `watchspecs`) registered through the `ygg.realm` entry-point group (legacy `ygg.handler` is deprecated). Internal realms live in `lib/realms/` (`test_realm` is dev-only).
3. **WatcherManager** (`lib/watchers/manager.py`) consumes BoundWatchSpecs, resolves connection config, deduplicates backends per `(backend, connection)`, evaluates each spec's `filter_expr`, builds payload/scope, and fans out `YggdrasilEvent`s to core. Startup wiring is validated by `lib/watchers/config_validation.py` (raises `WatcherConfigurationError`).
4. **Watcher backends** (`lib/watchers/backends/`) produce backend-agnostic `RawWatchEvent`s and persist resume positions via `CheckpointStore` (`InMemoryCheckpointStore` available for tests). Realm logic stays out of backends; the architecture is one-way — no ack/return path from realms back to backends.
5. **Handlers** (subclass `yggdrasil.flow.base_handler.BaseHandler`) declare `event_type: ClassVar[EventType]`, implement `derive_scope()` and async `generate_plan_drafts()`. Handlers generate plan *intent* (`PlanDraft`), never execute work directly.
6. Core persists plans through the injected `InternalStorageBundle` (CouchDB in production; SQLite only when explicitly configured and running in dev mode or tests). **PlanWatcher** consumes the bundle's backend-neutral change source, and the **Engine** (`yggdrasil.flow`) runs plans with per-step workdirs, fingerprint-based caching, `@step`-decorated functions receiving a `StepContext`, and event emission to `$YGG_EVENT_SPOOL`.

Namespacing: `yggdrasil/*` is the public API, `lib/*` is internal implementation. External code imports from `yggdrasil.*` only.

### Configuration

- `ConfigLoader` resolves config files from `yggdrasil_workspace/common/configurations/` or the current dir; the daemon loads `main.json`.
- Watcher/connection wiring lives under `main.json → external_systems` (endpoints + connections), resolved by `lib/core_utils/external_systems_resolver.py`.
- `main.json → internal_storage` selects internal persistence; explicit CouchDB roles reference named `external_systems` connections.
- DB managers get CouchDB params through `resolve_couchdb_params` (`lib/couchdb/couchdb_defaults.py`).
- `YggSession` singleton tracks dev-mode / manual-submission flags set by CLI flags.

### CouchDB layer

- `CouchDBHandler` (`lib/couchdb/couchdb_connection.py`) is the sync base wrapper; async consumers call it via `asyncio.to_thread`. `_changes` polling policy is centralized in `ChangesFetcher`.
- `YggdrasilDBManager` / `ProjectDBManager` extend it for specific databases; `@auto_load_and_save` persists `YggdrasilDocument` changes automatically.

### CLI

Entry point is `yggdrasil.cli:main` (`yggdrasil` console script). `yggdrasil daemon` runs watchers indefinitely (guarded by a `DaemonLock`); `yggdrasil run-doc <DOC_ID>` processes one document (`-m` forces manual HPC submission); `--dev` enables dev mode and debug logging. Root-level `ygg_trunk.py` / `ygg-mule.py` are legacy entry scripts.

## Conventions

- **Docstrings**: Google style, consistently applied — classes with several fields get an `Attributes:` section; methods document `Args:` / `Returns:` / `Raises:`; even private helpers get at least a one-liner. See `lib/core_utils/daemon_lock.py` or `lib/watchers/backends/base.py` for the canonical shape.
- **Logging**: `custom_logger(name)` from `lib.core_utils.logging_utils`; in classes: `self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")`.
- Prefer explicit construction + dependency injection over singletons (especially DB handlers/managers).
- Python ≥3.11: use `X | Y` unions, including in `isinstance` (ruff UP rules enforce this).
- Adding a watcher = adding a `WatchSpec` to a realm descriptor (with `filter_expr`, `build_scope()`, `build_payload()`); no registration in core setup code needed.
- **Doc references**: docstrings, comments and docs never cite "the PRD", "PRD §N", plan phases or acceptance-criterion numbers, since several PRDs exist. Explain the behavior itself, or name the source by path (`docs/design/prds/<name>.md`, `docs/TECH_DEBT_LEDGER.md` entry N). Write it as an explanation of how something works, what needs doing, or what was done, not as instructions to the reader.
- **Resolving tech debt**: when a `docs/TECH_DEBT_LEDGER.md` entry is resolved (marked resolved or removed), the same change updates every docstring, comment and doc that describes that gap or points at the entry, so none still describes a limitation that no longer exists. References come in several forms (`Tech Debt #17`, `docs/TECH_DEBT_LEDGER.md #15`, `entry 20`, or the ledger with no number), so search for both `TECH_DEBT_LEDGER` and `Tech Debt`.
