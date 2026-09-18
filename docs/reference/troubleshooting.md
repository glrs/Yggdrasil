# Troubleshooting

Common failure modes and how to resolve them.

---

## CouchDB connectivity

### Daemon fails to connect — host unreachable

**Symptom:** Startup log shows `ConnectTimeoutError` or `requests.exceptions.ConnectionError`.

**Checks:**
1. Verify network access to the CouchDB host
2. Verify CouchDB is running: `curl http://<host>:<port>/`
3. Check `main.json` → `external_systems.endpoints.<name>.url` matches the running instance

### Daemon fails to connect — wrong database name

**Symptom:** Log shows `ConnectionError: Database <name> does not exist` (or similar). The daemon continues running but the watcher for that connection is non-functional.

**Resolution:** Check the `resource.db` value under the relevant `connections` entry in `main.json`. Correct the name and restart the daemon.

> **Note:** The daemon does not fail fast on a missing database. A future improvement will address this. For now, watch the startup log carefully.

### Authentication failures

**Symptom:** `401 Unauthorized` in logs.

**Resolution:** Set credentials as environment variables:
```bash
export YGG_COUCH_USER=admin
export YGG_COUCH_PASS=yourpassword
yggdrasil --dev daemon
```

Credentials are resolved from `external_systems.endpoints.<name>.auth.user_env` and `auth.pass_env` in `main.json`.

---

## Configuration

### Missing or invalid config key

**Symptom:** A `RuntimeError` or `KeyError` appears in logs during daemon initialization — but not at the very first moment the process starts. Config errors surface in two phases:

1. **During DB manager initialization** — a `RuntimeError` is raised if a required CouchDB endpoint block is missing from `external_systems.endpoints` in `main.json`.
2. **During watcher initialization** — a `KeyError` is raised when a watcher backend config is missing required keys. This happens after realm discovery, when `WatcherManager` starts the backends.

> **Note:** `ConfigLoader` returns `None` (silently) for any key that is absent from the config file. Mistakes that don't touch the two paths above will not surface until the code path that uses the value is actually exercised at runtime.

**Resolution:** Check `main.json` in the resolved configuration directory. If `YGG_HOME` is set, Yggdrasil reads `$YGG_HOME/common/configurations/main.json`; otherwise it falls back to `yggdrasil_workspace/common/configurations/main.json`. See [configuration.md](../getting_started/configuration.md) for the full structure.

---

## Realm not discovered

### Handler not receiving events

**Symptom:** Events fire in CouchDB but the handler is never called.

**Checks:**
1. Confirm the entry point is registered in `pyproject.toml`:
   ```toml
   [project.entry-points."ygg.realm"]
   my_realm = "my_realm:get_realm_descriptor"
   ```
2. Re-install the package after editing `pyproject.toml`: `pip install -e .`
3. Check startup log for: `Discovered realm my_realm` — if absent, the realm was not found
4. If using dev-mode gating, verify `yggdrasil --dev daemon` is used

### WatchSpec validation error at startup

**Symptom:** `RuntimeError: WatchSpec from realm 'X' references unknown handler_id 'Y'`

**Resolution:** The `target_handlers` list in your `WatchSpec` must match `handler_id` class attributes exactly. Check for typos.

**Symptom:** `RuntimeError: realm_id 'X' already registered`

**Resolution:** Two realms share the same `realm_id`. Each realm must have a globally unique `realm_id` in its `RealmDescriptor`.

---

## Plan not created

### CouchDB document changed but no plan appears in `yggdrasil_plans`

**Checks:**
1. Verify the document matches your `filter_expr`. Test the JSON Logic predicate manually:
   ```python
   from json_logic import jsonLogic
   jsonLogic(filter_expr, {"doc": your_doc, "deleted": False, "meta": {}})
   ```
2. Check `build_payload` returns a dict with the fields your handler expects
3. Confirm `connection` name in the WatchSpec matches its key in `external_systems`

---

## Plan created but not executing

### Plan stays in `status="draft"` indefinitely

**Explanation:** `auto_run=False` in the `PlanDraft` sets initial status to `"draft"`. The plan waits for manual approval.

**Resolution:** Yggdrasil does not currently ship an approval UI or command.
Use an operator-provided integration with the configured plan store to set
`status="approved"`. For SQLite, that integration must also advance the
plan change sequence transactionally; editing only the stored JSON will
not notify PlanWatcher.

### Plan is `status="approved"` but Engine never runs it

**Checks:**
1. Confirm PlanWatcher is started: `grep "PlanWatcher" yggdrasil.log`
2. Check `run_token > executed_run_token` in the plan document
3. Verify the configured internal plan store (`yggdrasil_plans` database) exists and is accessible: the CouchDB
   plans connection in production, or the SQLite file in dev mode.

---

## Step execution failures

### `ModuleNotFoundError` or `AttributeError` for `fn_ref`

**Symptom:** Step fails with `cannot import name 'run_foo' from 'my_realm.steps'`

**Resolution:**
- The `fn_ref` in your `StepSpec` must be a valid dotted Python path to a `@step`-decorated function
- The function must exist and be importable in the daemon's Python environment
- Check for typos in the module path

### `ValueError` — undecorated step function

**Symptom:** Plan execution raises:
```
ValueError: Undecorated step function detected for step '<step_id>' (fn_ref='<fn_ref>'). Decorate it with '@step' from 'yggdrasil.flow.step'.
```

**Explanation:** The Engine validates every resolved callable before execution. If `fn_ref` resolves to a plain function missing the `@step` decorator, the plan is aborted immediately. This prevents silent loss of lifecycle events (`step.started`, `step.succeeded`, `step.failed`) — which `@step` is responsible for emitting.

**Resolution:** Decorate the function with `@step`:
```python
from yggdrasil.flow.step import step
from yggdrasil.flow.model import StepContext, StepResult

@step                     # required
def my_step(ctx: StepContext, **params) -> StepResult:
    ...
```

---

## DataAccess errors

### `DataAccessDeniedError` when accessing a connection

**Symptom:** Step or plan generation fails with `DataAccessDeniedError: realm 'X' has no permissions for connection 'Y' in phase 'Z'`

**Explanation:** The connection `Y` has a `data_access.realms` block that does not include your realm for the requested phase, or the realm's `permissions` list for that phase is missing `"read"` (or `"write"` for writes).

**Resolution:** Add your realm with the correct phase permissions to the connection's `data_access.realms` block in `main.json`:
```json
{
  "external_systems": {
    "endpoints": {
      "main_couchdb": {
        "backend": "couchdb",
        "url": "<host>:<port>",
        "auth": { "user_env": "YGG_COUCH_USER", "pass_env": "YGG_COUCH_PASS" }
      }
    },
    "connections": {
      "my_db": {
        "endpoint": "main_couchdb",
        "resource": { "db": "my_database" },
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
    }
  }
}
```

> **Note:** `"write"` permission does not grant `"read"`. A realm that only has `"write"` can call `save()` but calling `get()` or `find()` will still raise `DataAccessDeniedError`. Grant `"read"` explicitly if reads are also needed.

### `DataAccessDeniedError` — no data_access policy for connection

**Symptom:** `DataAccessDeniedError: connection 'X' has no data_access policy`

**Explanation:** Connections without a `data_access` block are not accessible via `ctx.data.connection()`. This is intentional — opt-in only.

---

## Daemon lock

### "Another local Yggdrasil daemon is already running in … mode"

One daemon may run **per mode** per user per host: `daemon.lock` (prod)
and `daemon-dev.lock` (dev) in the user runtime directory. The error
message and the lock metadata (pid, hostname, user, started_at) identify
the holder.

**Resolution:**

1. Identify the holding process from the metadata in the error message.
2. Stop it, or run your daemon in the other mode / on another machine.
3. **Never delete a lock file while a daemon is running** — the advisory
   `flock` dies with the process, and deleting a *held* file lets a second
   daemon start. Stale lock files from exited daemons are harmless and can
   stay.

**Limits:** the lock does not coordinate across OS users, containers, or
hosts, and it is intentionally not database-aware — it cannot detect two
daemons sharing a CouchDB environment (the documentation and checkpoint
conflict warnings are the safeguard there).

---

## Internal storage (dev SQLite)

### Daemon refuses to start: invalid `internal_storage` configuration

Explicit configuration errors are fatal by design; Yggdrasil never falls
back to another storage backend. The message names the offending role,
connection, or path.

### "not an Yggdrasil internal-storage database" / schema version errors

The SQLite file failed lifecycle validation (unrelated file, malformed
content, or unsupported schema version). The dev database is disposable:
move the file aside — or delete it — and restart to create fresh state.
Never point the config at a file you care about; Yggdrasil refuses
symlinks and foreign-owned files.

### Approved a plan but the dev daemon does nothing

Check that the stored plan has `status="approved"` and
`run_token > executed_run_token`. Also confirm that PlanWatcher is
running and that the approval was recorded as an observable change by
the configured backend.

A daemon with no checkpoint can miss a plan approved before it started.
After confirming PlanWatcher is running, the operator-provided storage
integration can re-emit an observable change for that plan. Re-emitting a
plan that is already executing does not start a second attempt: the daemon
runs one attempt per plan at a time, and checks the plan again once the
running attempt is finished, so an already-served request is not rerun.

---

## Event spool

### No event files appearing in `$YGG_EVENT_SPOOL`

**Checks:**
1. Confirm `YGG_EVENT_SPOOL` is set (defaults to `/tmp/ygg_events`, or `/tmp/ygg_events_dev` under `--dev`)
2. Verify the directory is writable: `ls -la $YGG_EVENT_SPOOL`

### Finding events for a specific plan

```bash
find $YGG_EVENT_SPOOL -path "*/test_realm/*" -name "*.json" | sort
```

---

## See also

- [Configuration](../getting_started/configuration.md) — config file layout and environment variables
- [Test Realm](test_realm.md) — test scenarios for validating the pipeline end-to-end
- [Architecture Overview](../architecture/overview.md) — understanding the event flow
