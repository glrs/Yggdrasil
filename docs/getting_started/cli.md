# CLI Reference

## Invocation

```bash
yggdrasil [--dev] {daemon | run-doc} [OPTIONS]
```

You can also use the module form (useful when developing, since it bypasses the installed console-script):

```bash
python -m yggdrasil [--dev] {daemon | run-doc} [OPTIONS]
# or equivalently
python -m yggdrasil.cli [--dev] {daemon | run-doc} [OPTIONS]
```

---

## Global flags

| Flag    | Description |
|---------|-------------|
| `--dev` | Enable *development mode*: DEBUG-level logging, dev configuration (`main.json` is replaced by `dev_main.json` when present), enables the test realm |

`--dev` must come **before** the subcommand:

```bash
yggdrasil --dev daemon      # correct
yggdrasil daemon --dev      # incorrect (argument error)
```

---

## Subcommands

### `daemon`

Starts the long-running service:

- Instantiates all configured watchers (CouchDB, file-system, …)
- Auto-registers built-in and external realm handlers via `ygg.realm` entry-point discovery
- Processes events until stopped with **Ctrl-C**. Stopping starts no new step: a running step finishes, and a plan interrupted part-way stays eligible to run again

```bash
# Production run
yggdrasil daemon

# Verbose local run with dev settings
yggdrasil --dev daemon
```

Logs are written to the directory set in `main.json` → `yggdrasil.log_dir`, one file per process: `yggdrasil_<timestamp>_<pid>.log` (`yggdrasil_dev_<timestamp>_<pid>.log` in dev mode).

**Note**: One daemon may run **per mode** per user per host: a production daemon holds `daemon.lock` and a dev daemon holds `daemon-dev.lock` (advisory locks in the user runtime directory), so `yggdrasil daemon` and `yggdrasil --dev daemon` can run side by side while duplicates of the same mode are rejected. **Coexistence is only safe when the two daemons use different internal storage** (see [Running prod and dev side by side](configuration.md#running-prod-and-dev-side-by-side)) — the lock is intentionally not database-aware, and only one daemon should run against a shared database environment at a time. If another machine or runtime is pointed at the same databases, checkpoint conflict warnings indicate that more than one daemon may be active, but this may lead to unexpected behaviour.

### `run-doc`

**Warning**: `run-doc` used to be a core tool of an earlier version, but the core has now moved on. We've attempted to keep it afloat, but it might not work as expected anymore.

Processes **exactly one** CouchDB project document and then exits. Useful for manual re-processing or debugging a specific project.

```bash
yggdrasil run-doc DOC_ID [--plan-only | --run-once] [--force] [--timeout SECONDS] [--manual-submit]
```

| Option            | Description |
|-------------------|-------------|
| `DOC_ID`          | The `_id` of the CouchDB project document to process |
| `-p`, `--plan-only` | Create the plan(s) only, for the daemon to execute once approved. The default when no mode is given |
| `-r`, `--run-once` | Create the plan(s), execute them in this process once approved, then exit |
| `-f`, `--force`   | Overwrite an existing plan without asking |
| `-t`, `--timeout SECONDS` | With `--run-once`: how long to wait for plans to become executable, such as waiting for approval (default 1800). It never interrupts a running plan |
| `--manual-submit` | Force manual HPC submission: Yggdrasil writes a submit script but waits for you to run `sbatch` and insert the job ID back into the document |

#### Exit codes with `--run-once`

| Code | Meaning |
|------|---------|
| `0`  | Every plan ran, succeeded, and has its result recorded |
| `1`  | Any other ending for any plan. That includes a `continue_independent` plan that finished its healthy branches but had failed or blocked steps, a result that could not be recorded, and a plan still unapproved at the timeout |
| `130` | Interrupted with Ctrl-C. No new plan or step starts, the running step finishes, and unfinished plans stay eligible |

An exit code of `1` from a `continue_independent` plan does not mean its work stopped early: every branch that could run did, and the request is recorded as finished with a failed outcome. See [Plan Execution](../reference/plan_execution.md) for how to rerun it.

#### Example: manual Slurm submission

Re-process project `a1b2c3d4e5f`, but hold before Slurm submission to edit the pipeline configuration manually:

```bash
yggdrasil run-doc a1b2c3d4e5f --manual-submit
```

After the script is written, edit project configuration as needed, then submit to Slurm. Copy the resulting `job_id` into the `external_job_id` field of the project's CouchDB document, then re-run:

```bash
yggdrasil run-doc a1b2c3d4e5f --manual-submit
```

Yggdrasil picks up the running Slurm job and waits for it to complete before continuing with post-processing.

---

## Quick-reference table

| Goal | Command |
|------|---------|
| Run as background service | `yggdrasil daemon` |
| Same, with dev logging and dev servers | `yggdrasil --dev daemon` |
| Re-process one document | `yggdrasil run-doc <DOC_ID>` |
| Plan and execute one document in this process | `yggdrasil run-doc <DOC_ID> --run-once` |
| Re-process with manual Slurm submission | `yggdrasil run-doc <DOC_ID> --manual-submit` |
| Use module form (when developing) | `python -m yggdrasil ...` |

---

## See also

- [Configuration](configuration.md) — config files and environment variables
- [Quickstart](quickstart.md) — installation and first run
- [Plan Execution](../reference/plan_execution.md) — execution outcomes, reruns and snapshots
