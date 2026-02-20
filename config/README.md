# Config (sample only in repo)

This directory contains **sample** configuration only. Environment-specific config must not live in the repo (it may contain sensitive data).

## Where config lives at runtime

- **On host:** `$HOME/config` — all env definitions (e.g. `$HOME/config/initial/initial.yaml`, `$HOME/config/qa/qa.yaml`).
- **In Docker:** `/config` — the app reads from here when `GRCAI_CONFIG_HOME` is set to `/config` (default in the image).

## Override (optional)

Set **`GRCAI_CONFIG_HOME`** to override the config directory. If unset, the default is `config` (relative to the process working directory) or the value set by the Docker/install setup.

## Repo contents (samples only)

- **initial/** — sample env (e.g. `initial/initial.yaml`)
- **template/** — example templates (on-prem, cloud)
- **defaults/** — default services/connectors reference
- **reasoning_budget.yaml** — reasoning budget sample
- **env_schema.json** — schema reference

On first install, the install script or entrypoint copies this sample into `$HOME/config` or `/config` when empty.

## See also

**Sessions** (evidence, IR, RCA outputs) are also client-specific and not in the repo. They live in `$HOME/grcai_sessions` on the host; in Docker, `GRCAI_SESSIONS_HOME` points to the mounted path (e.g. `/grcai/grcai_sessions`). Create the directory if it doesn’t exist; it will be mounted so the container uses it.
