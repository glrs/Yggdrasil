# Configuration

Yggdrasil reads configuration files from the resolved workspace directory:

- If `YGG_HOME` is set, configuration files are loaded from `$YGG_HOME/common/configurations/`.
- If `YGG_HOME` is not set, Yggdrasil falls back to `yggdrasil_workspace/common/configurations/` beside the source tree.

`YGG_HOME` must point to the root of the Yggdrasil workspace, not to the source checkout or the conda environment.

Two key files:

| File | Purpose |
|---|---|
| `main.json` | Global settings: logging, internal storage, external systems, polling intervals |
| `dev_main.json` | Dev-mode configuration. When `--dev` is passed, it **replaces `main.json`**. Any config file with a `dev_` twin is swapped the same way. |

---

## main.json fields

> **Note:** The top-level structure of `main.json` is still evolving. Fields are expected to be reorganised in a future release.

```json
{
    "yggdrasil": {
        "log_dir": "yggdrasil_workspace/logs",
        "job_monitor_poll_interval": 5
    },
    "report_transfer": {
        "server": "<server>",
        "user": "<username>",
        "destination": "<destination_path>",
        "ssh_key": "<ssh_key_path>"
    },
    "external_systems": { ... }
}
```

| Field | Description |
|---|---|
| `yggdrasil.log_dir` | Directory where Yggdrasil writes its log files |
| `yggdrasil.job_monitor_poll_interval` | Seconds between Slurm job status polls |
| `report_transfer.server` | SSH server for transferring reports |
| `report_transfer.user` | SSH user |
| `report_transfer.destination` | Remote destination path |
| `report_transfer.ssh_key` | Path to SSH key (optional) |

---

## external_systems — endpoints and connections

`external_systems` maps logical connection names to their backends. It has three sub-keys:

```json
"external_systems": {
    "endpoints": {
        "main_couchdb": {
            "backend": "couchdb",
            "url": "<host>:<port>",
            "auth": {
                "user_env": "YGG_COUCH_USER",
                "pass_env": "YGG_COUCH_PASS"
            }
        }
    },
    "connections": {
        "projects_db": {
            "endpoint": "main_couchdb",
            "resource": { "db": "projects" },
            "watch": {
                "poll_interval": 3,
                "limit": 100,
                "start_seq": "0"
            }
        },
        "yggdrasil_db": {
            "endpoint": "main_couchdb",
            "resource": { "db": "yggdrasil" },
            "data_access": {
                "realms": {
                    "my_realm": {
                        "planning":  { "permissions": ["read"] },
                        "execution": { "permissions": ["read", "write"] }
                    }
                },
                "options": { "max_limit": 50 }
            }
        }
    },
    "defaults": {
        "couchdb": {
            "start_seq": "0",
            "max_limit": 200
        }
    }
}
```

**`endpoints`** define physical backend connections. Each entry specifies a backend type (`couchdb`), its URL, and auth credentials via environment variable names.

**`connections`** define named logical connections that realms reference. Each connection points to an `endpoint` and a `resource` (e.g. a database name), and configures one or both of:

| Key | Purpose |
|---|---|
| `watch` | Used by `WatcherManager` to poll the changes feed. Configures `poll_interval`, `limit`, `start_seq`. A `limit` of at least 25 is recommended — very low values (< 5) will cause slow recovery after downtime. |
| `data_access` | Used by `DataAccess` for realm data queries. See the table below for sub-keys. |

**`data_access` sub-keys:**

| Key | Purpose |
|---|---|
| `data_access.realms` | Maps realm_id → phase → permission list. Unlisted realms are denied. Each phase (`planning`, `execution`) has its own `permissions` array (`"read"`, `"write"`). |
| `data_access.options` | Per-connection backend options (e.g. `max_limit`). Overrides the matching key in `defaults.<backend>`. |

**`defaults`** groups all backend defaults under `defaults.<backend>`. Both `WatcherManager` and `DataAccess` read from `defaults.<backend>` — watcher settings (e.g. `start_seq`, `poll_interval`) and data-access settings (e.g. `max_limit`) all live here. Connection-level overrides (`watch` keys and `data_access.options` respectively) take precedence over these defaults.

`WatchSpec` entries in a realm's registration reference a connection by logical name (e.g. `connection="projects_db"`). The `WatcherManager` resolves the name to the concrete endpoint at startup.

---

## Environment variables

### Configuration discovery

| Variable | Purpose |
|---|---|
| `YGG_HOME` | Root of the Yggdrasil workspace. When set, config files are resolved under `$YGG_HOME/common/configurations/`. Use an absolute path in production and HPC deployments. |

### Credentials and runtime paths

Sensitive credentials should be set as environment variables, not stored in config files.

| Variable | Purpose |
|---|---|
| `YGG_COUCH_USER` | CouchDB username |
| `YGG_COUCH_PASS` | CouchDB password |
| `YGG_WORK_ROOT` | Central workspace root for all plan and step working directories (default: `/tmp/ygg_work`; `/tmp/ygg_work_dev` under `--dev`). Set by the operator before starting the daemon. Precedence: `main.json → work_root` → `$YGG_WORK_ROOT` → mode default. |
| `YGG_EVENT_SPOOL` | Root directory where `FileSpoolEmitter` writes structured event JSON files (default: `/tmp/ygg_events`; `/tmp/ygg_events_dev` under `--dev`). Set by the operator before starting the daemon. |
| `OPS_DB` | **Deprecated.** Operations database name (default: `yggdrasil_ops`). Honored only when `main.json` has no `internal_storage` block; explicit configuration ignores it. |

`YGG_WORK_ROOT` and `YGG_EVENT_SPOOL` are resolved once at daemon startup and apply to all realms. Realm code does not read these variables — step functions receive the resolved paths via `ctx.workdir` and `ctx.scope_dir`, and emit events via `ctx.emitter`. Explicit values are exact overrides and are never rewritten; only the *defaults* differ by mode so that a *prod* and a *dev* daemon on the same machine do not share caches or spools.

---

## Internal storage

Yggdrasil Core keeps its *internal* state — plan documents, watcher
checkpoints, and operations snapshots — behind a backend-neutral storage
boundary configured by the top-level `internal_storage` block.

**Production (CouchDB, explicit).** Each logical database role, names one
`external_systems` connection; the URL, credentials env-var names, and
database name all come from the referenced connection — never duplicated
here:

```json
"internal_storage": {
    "backend": "couchdb",
    "couchdb": {
        "connections": {
            "coordination": "yggdrasil_db",
            "plans": "yggdrasil_plans_db",
            "operations": "yggdrasil_ops_db"
        }
    }
}
```

**Dev and testing (SQLite, explicit).** Accepted only in dev mode
(`--dev`). All internal state lives in one disposable local file; deleting
it yields fresh state at the next startup:

```json
"internal_storage": {
    "backend": "sqlite",
    "sqlite": { "path": null }
}
```

`"path": null` selects `$YGG_HOME/internal_state/dev/yggdrasil.sqlite3`
(or the bundled workspace when `YGG_HOME` is unset); an explicit path must
be absolute. Invalid configuration is fatal — Yggdrasil never silently
falls back to another backend. Network filesystems are unsupported for the
SQLite file (WAL requires reliable host-local storage).

If the block is absent, the legacy implicit CouchDB construction applies
(deprecated; a warning is logged, and explicit configuration will be
required in a future release).

**Manual plan approval.** Plans created with `auto_run=False` remain in
`status="draft"` until an external actor approves them. Yggdrasil does not
currently ship an approval UI or command. Operators who need manual
approval must provide their own integration with the configured plan
store.

Approval changes `status` to `"approved"`. Requesting another execution
increments `run_token`; a plan is eligible only while `run_token` is
greater than `executed_run_token`. Neither field says whether a run
succeeded: `executed_run_token` marks the latest *finished* request, and
the outcome is recorded separately (see
[Plan Execution](../reference/plan_execution.md)). A SQLite integration must also advance
the plan change sequence transactionally so PlanWatcher observes the
update, and must write only if the plan is still at the revision it read
(`SQLiteInternalStore.put_document(..., expected_rev=...)`), so it never
overwrites a plan that Yggdrasil updated in the meantime. On a conflict,
reread the plan and decide again. Editing only the stored JSON is
insufficient, and SQLite tooling must be kept compatible with Yggdrasil's
internal schema.

External data sources are unaffected by the storage backend. Realm watch
sources and realm `data_access` providers still resolve through
`external_systems`. A dev SQLite daemon therefore still needs the external
systems required by its enabled realms to be reachable.

### Running prod and dev side by side

One daemon may run **per mode** per user per host (`daemon.lock` /
`daemon-dev.lock`). For safe coexistence on one machine:

- **Internal state**: point dev at SQLite (above), or at a *different*
  CouchDB server. Database names (`yggdrasil`,
  `yggdrasil_plans`, `yggdrasil_ops`) are fixed, so distinct endpoints —
  not renamed connections — are the reliable boundary. Yggdrasil does not
  detect a shared database; running two daemons against the same CouchDB
  environment is unsupported.
- **Work root and event spool**: isolated automatically via the
  `_dev`-suffixed mode defaults. If you set `YGG_WORK_ROOT` /
  `YGG_EVENT_SPOOL` explicitly, use distinct values per daemon.
- **Logs**: the shared log directory is safe — filenames carry a mode
  marker and the PID.

---

## Logging

- CLI `--dev` enables DEBUG logging and loads `dev_<name>.json` in place of `<name>.json` (if present).
- Default is INFO.
- Logs are written to the directory configured as `yggdrasil.log_dir` in `main.json` (one file per process: `yggdrasil_<timestamp>_<pid>.log`, with a `dev_` marker in dev mode), and optionally to console.
