## gRCAi Client – High-Level Architecture

This document describes the high-level architecture of the `grcai-client` repository: main modules, responsibilities, external dependencies, data flow, runtime entrypoints, and assumptions about external systems/repos.

---

## Main Modules and Responsibilities

### `main.py` (CLI)
- **Responsibility**: Command-line entrypoint for the gRCAi client.
- **Key commands**:
  - `grcai collect`: run the evidence collection pipeline (no LLM).
  - `grcai rca`: run root cause analysis on an existing evidence file.
- **Behavior**:
  - Parses CLI arguments (components, environment, issue time, observations, interactive mode).
  - Instantiates `SessionOrchestrator` and delegates either:
    - `run_interactive_session()` (if implemented) or
    - `run_non_interactive(...)` for scripted evidence collection.
  - For `rca`, validates the evidence file path, resolves the OpenAI API key, selects a model, and calls `SessionOrchestrator.run_rca_on_file(...)`.

### `orchestrator.session_orchestrator`
- **Responsibility**: Orchestrates end-to-end evidence collection and invocation of reasoning for a single RCA session.
- **Key concepts**:
  - **Host-first collection pipeline**: iterates over configured hosts, then over services/components on each host.
  - **Architectural boundary enforcement**: explicitly forbidden from interpreting evidence or making diagnostic decisions; it only sequences work and forwards results.
  - **Environment-aware**: uses `ConfigLoader` to load environment YAML (hosts, access methods, services, etc.).
  - **Evidence lifecycle**:
    - Performs fast pre-checks of host reachability (`_precheck_hosts`).
    - Constructs appropriate host connectors (local/SSH/Docker) per host + component.
    - Invokes component adapters via `CONNECTOR_REGISTRY` and normalizes their outputs into a structured evidence format.
    - Tracks connection failures and per-host collection status.
    - Saves the resulting evidence bundle via `EvidenceStore.save_session(...)`, embedding metadata and minimal constraints.
  - **Reasoning invocation**:
    - `run_rca_on_file(...)` loads an existing evidence file, infers environment, constructs a reasoning client via `get_reasoning_client`, calls `analyze(...)`, and then `generate_tasks_from_rca(...)`.
- **Trust/logic boundary**:
  - Must not evaluate thresholds, classify failures, assign severity, or decide which evidence is “important”.
  - All interpretation is delegated to the external Central Reasoning Service.

### `connectors` package
- **Responsibility**: Implements pluggable adapters that collect metrics/logs/findings from specific technologies and environments.
- **Submodules**:
  - `connectors.adapters.*`: technology-specific adapters (e.g., `tomcat_adapter`, `nginx_adapter`, `os_adapter`, `postgres_adapter`, `redis_adapter`, `kafka_adapter`, `mssql_adapter`, `nodejs_adapter`, `docker_adapter`, `generic_docker_adapter`).
  - `connectors.host_connectors.*`: host-level transport abstractions (`SSHHostConnector`, `DockerHostConnector`, `LocalHostConnector`, `BaseHostConnector`).
  - `connectors.base.*`: base connector abstractions (`BaseConnector`, `BaseComponent`) shared by all adapters.
  - `connectors.historical.*`: helpers for historical log/metric collection and time filtering.
  - `connectors.registry`: centralized registry mapping logical component names (e.g., `"nginx"`, `"tomcat"`, `"os"`) to adapter classes via `_safe_register`.
- **Behavior**:
  - Each adapter implements `collect_for_host(host_info, connector)` and returns structured evidence:
    - Structured `instances` list (metrics, logs, errors, metadata) plus `findings` for backward compatibility.
  - Host connectors encapsulate connectivity to machines and Docker daemons (SSH, local Docker socket, etc.).
  - The orchestrator uses the registry to resolve component names used in config/CLI into specific adapter implementations.

### `evidence_store` package
- **Responsibility**: Persist evidence sessions to disk in a safe, structured format designed for sharing.
- **Key module**: `evidence_store.evidence_store.EvidenceStore`
  - Determines base sessions directory (`GRCAI_SESSIONS_HOME` env var or default `grcai_sessions/`).
  - `save_session(...)`:
    - Accepts context, host evidence, container evidence, OS node evidence, environment, collection timestamps, host collection status, and minimal constraints.
    - Builds a structured evidence JSON document:
      - Embeds metadata (collector version/build, contracts version/major, environment, timestamps, requested components, host collection status, user timezone, etc.).
      - Embeds schema version from `evidence_store.evidence_schema.SCHEMA_VERSION`.
      - Embeds minimal constraints (if provided) under `"_constraints"`.
    - Writes JSON to `grcai_sessions/<environment>/rca_YYYY-MM-DD_HH-MM-SS.json`.
- **Security goal**:
  - Evidence files are designed to be safe to share (no secrets, identities, or raw payloads) and suitable for attaching to tickets or backups.

### `client` package
- **Responsibility**: Client-side security hardening and communication with the Central Reasoning Service.
- **Submodules**:
  - `client.rca_client`:
    - `RCAClient`: HTTP client that sends sanitized evidence to the Central Reasoning Service over HTTPS.
      - Sanitizes evidence using `client.evidence_sanitizer` before every request (no bypass).
      - Adds versioning headers: `X-GRCAI-Contracts-Version` and `X-GRCAI-Client-Version`.
      - Implements robust retry logic with backoff for 5xx / network issues.
      - Provides:
        - `analyze(evidence_file)`: returns RCA text and caches tasks.
        - `generate_incident_response_report(evidence_file)`: returns IR text.
        - `generate_tasks_from_rca(rca_text)`: returns tasks from last `analyze()` call.
    - Factory helpers:
      - `create_rca_client(...)`: builds `RCAClient` using `GRCAI_CENTRAL_URL`.
      - `get_reasoning_client(...)`: abstraction used by orchestrator/UI; currently always returns `RCAClient`.
  - `client.evidence_sanitizer`:
    - Performs **structural removal** and **content masking** on evidence.
    - Reuses `ui.utils.data_masking.DataMasker` for pattern-based masking (IPs, tokens, emails, etc.).
    - Maintains a protected top-level key set (`host`, `containers`, `metadata`, `_constraints`, etc.) to preserve structural integrity while removing sensitive leaf fields.
    - `validate_no_sensitive_data(...)` performs a defensive second pass to detect residual secrets.
  - `client.constraint_extractor`:
    - Extracts **minimal, non-sensitive constraints** (enabled flag + responsibilities) from full environment config.
    - Handles both legacy (`services.*.config_expectations`) and host-nested (`hosts[].services.*.config_expectations`) formats.
    - Produces a compact structure embedded into evidence under `_constraints`, informing Central about expected responsibilities without exposing connection details or identities.

### `config` package
- **Responsibility**: Environment configuration loading and reasoning-budget configuration.
- **Key module**: `config.config_loader.ConfigLoader`
  - Resolves base directory from `GRCAI_CONFIG_HOME` (default `config/`).
  - `load_environment(env_name, explicit_path=None)`:
    - If `explicit_path` is a file, loads that YAML.
    - If `explicit_path` is a directory, loads `<explicit_path>/<env>.yaml`.
    - Otherwise, loads `config/<env>/<env>.yaml`.
    - Returns a dict with hosts, access, and services definitions.
  - `list_environments()`:
    - Scans `config/` for `<env>/<env>.yaml` folders.
  - `load_reasoning_budget(environment=None)`:
    - Loads `config/reasoning_budget.yaml` and merges optional `config/<env>/reasoning_budget.yaml` overrides.
  - `get_config_expectations(...)`:
    - Extracts `config_expectations` sections from environment YAML and returns a summarized view of enabled components, responsibilities, and notes.
- **Usage**:
  - `SessionOrchestrator` uses `ConfigLoader` to obtain environment config for host + service definitions and constraints extraction.

### `ui` package (Streamlit UI)
- **Responsibility**: Rich, multi-tab interactive UI for running collections, inspecting evidence, and triggering RCA/IR flows.
- **Key module**: `ui.app`
  - Streamlit app with 6 main tabs:
    1. **Environment** – environment overview and topology.
    2. **Evidence** – forensic evidence browser with hierarchical tree navigation.
    3. **Evidence Highlights** – auto-detected issues and concerns dashboard.
    4. **Evidence Review** – pre-LLM transparency and security review screen.
    5. **Incident Report** – short incident-response oriented report.
    6. **Full RCA** – full RCA report.
  - Integrates:
    - `SessionOrchestrator` for collection.
    - `get_reasoning_client` (RCAClient) for RCA/IR generation.
    - `ui.components.*` for tree view, metrics tables, log viewers, highlights, and progress tracking.
    - `ui.utils.helpers` for safe evidence loading and date/time formatting.
    - `ui.utils.data_masking` for UI-side masking and re-use in `client.evidence_sanitizer`.
  - Handles browser timezone detection via injected JavaScript and query params to ensure dates/times render in the user’s local TZ.
  - Manages `st.session_state` for selected environment, evidence file, selected tree node, active tab, and chosen model.
- **Submodules**:
  - `ui.components.*`: composable UI components (evidence tree, metrics viewer, log viewer, highlights, LLM input review, multipass progress bar).
  - `ui.utils.*`: helper utilities (safe accessors, data masking, concern extraction, log filtering, etc.).

### `utils` package
- **Responsibility**: Cross-cutting utilities used by orchestrator, client, and UI.
- **Key modules**:
  - `utils.version`:
    - Derives client version using fallback chain:
      - `__version__.__version__` (local).
      - Installed package metadata or `pyproject.toml`.
      - `git describe --tags`.
      - Fallback `"0.0.0-dev"`.
    - Provides:
      - `get_version()`
      - `get_build()` (git tag/SHA or `"unknown"`)
      - `get_contracts_path()` (env `CONTRACTS_PATH` or repo-local `./contracts`).
      - `get_contracts_version()` (reads `<contracts_path>/version.txt`).
      - `semver_major(version)`.
  - `utils.print_utils`:
    - Presentation helpers for CLI (e.g., `divider()`).

### `contracts` directory
- **Responsibility**: Local pointer and helper content for contracts (schemas, OpenAPI, events).
- **Important**:
  - Actual contract definitions live in a **separate repo** (`grcai-contracts`).
  - This repo only expects a `contracts/` folder to exist at runtime or in containers; it does not own the canonical schemas.
- **Usage**:
  - `utils.version.get_contracts_path()` and `get_contracts_version()` read versioning information from this directory.
  - Evidence metadata embeds contracts version and major version to ensure Central can interpret evidence correctly.

### `sanity` and `scripts`
- **Responsibility**:
  - `sanity/sanity_validate_reports.py`: Script-level tooling to validate generated reports/evidence.
  - Shell helpers (`sanity.sh`, `ui.sh`, `install-*.sh`) to drive local workflows and install prerequisites.

---

## External Dependencies

### Python packages (from `requirements.txt`)
- **Core / AI-related**:
  - `openai`: client library for OpenAI models (used indirectly via Central; client itself does not call LLMs in normal mode).
  - `tiktoken`: tokenization utilities.
  - `python-dotenv`: environment variable configuration.
  - `pyyaml`: YAML parsing for environment/config files.
- **Connectors / Infrastructure**:
  - `docker`: Docker API client used by `DockerHostConnector` and Docker-based adapters.
  - `paramiko`: SSH client used by `SSHHostConnector`.
  - `pyodbc`: optional MSSQL connectivity for `MSSQLAdapter`.
- **UI / Streamlit stack**:
  - `streamlit`: web UI framework for `ui.app`.
  - `click`, `tornado`, `watchdog`: runtime dependencies underlying Streamlit.
  - `numpy`, `pandas<2.2`, `protobuf`, `rich`, `pydeck`, `altair`: data handling and visualization within the UI (tabular metrics, charts, etc.).
- **HTTP and utility**:
  - `requests` (+ `urllib3` via `Retry`): HTTP transport for `RCAClient`.
  - `pytest`, `pytest-mock`, `tabulate`: testing and CLI-friendly tabular output.

### External services
- **Central Reasoning Service (remote or local)**:
  - HTTP API, assumed to expose endpoints such as:
    - `POST /api/v1/rca/analyze`
    - `POST /api/v1/rca/incident-response`
  - Receives **sanitized evidence** and returns:
    - RCA text.
    - Incident response text.
    - Structured task lists.
  - The client authenticates via bearer token representing an OpenAI API key (forwarded to Central).

---

## Data Flow Direction

### 1. Collection Phase (CLI or UI)
- **Initiation**:
  - CLI: `grcai collect ...` executes `main.py → SessionOrchestrator.run_non_interactive(...)`.
  - UI: Streamlit UI triggers orchestrator methods (direct calls from `ui.app`).
- **Configuration loading**:
  - `SessionOrchestrator` calls `ConfigLoader.load_environment(env)` to get hosts, access, and service definitions.
- **Host pre-check**:
  - `_precheck_hosts` probes reachability via:
    - SSH to VMs.
    - Docker daemon ping to Docker hosts.
    - Local assumptions for local hosts.
  - Unreachable hosts are tracked and excluded from detailed collection.
- **Evidence collection**:
  - For each host and each requested component on that host:
    - Orchestrator selects appropriate `HostConnector` (SSH, Docker, local).
    - Looks up `ConnectorCls` in `CONNECTOR_REGISTRY`.
    - Instantiates adapter and calls `collect_for_host(host_info, connector)` with timeouts and retry protection.
    - Adapters collect metrics/logs/status and return normalized structures.
  - Orchestrator:
    - Aggregates results by component and host.
    - Distinguishes between:
      - Actual data.
      - Service-not-running (liveness-only errors).
      - Connection failures (host unreachable) which are excluded from evidence and summarized separately.
- **Constraints extraction and evidence persistence**:
  - `extract_minimal_constraints` and/or `ConfigLoader.get_config_expectations` extract non-sensitive responsibilities.
  - `EvidenceStore.save_session(...)` writes the full evidence JSON:
    - `host` + `containers` + `os_nodes`.
    - `metadata` (versioning, environment, timestamps, requested components, host status).
    - `_constraints` (minimal expectations).

### 2. Reasoning Phase (RCA / Incident Response)
- **Invocation from CLI**:
  - `grcai rca <evidence_file>`:
    - `main.py` validates file and API key, then:
      - Calls `SessionOrchestrator.run_rca_on_file(evidence_file, api_key, model)`.
    - `run_rca_on_file`:
      - Optionally reads environment from evidence metadata.
      - Uses `get_reasoning_client` to obtain an `RCAClient`.
      - Calls `RCAClient.analyze(evidence_file)` → Central Reasoning Service.
      - Calls `RCAClient.generate_tasks_from_rca(...)` to obtain tasks.
    - CLI prints:
      - RCA summary.
      - Task ownership matrix (team, task, priority, effort).
- **Invocation from UI**:
  - `ui.app` uses `get_reasoning_client` similarly, passing selected evidence file and environment.
  - RCA/IR outputs flow back into the UI and populate:
    - Evidence Highlights.
    - Incident Report.
    - Full RCA tabs.
- **Security boundary**:
  - Evidence sent to Central is **not raw host data**:
    - Structural sensitive fields are removed by `sanitize_evidence_structure`.
    - Remaining string content is masked by `DataMasker`.
    - `validate_no_sensitive_data` performs a guardrail pass; any violations are logged as warnings.
  - Per `docs/security/Readme`, Central only receives abstract, behavioral observations (error categories, patterns, aggregated counts, etc.), not identities, secrets, or raw payloads.

### 3. UI Data Flow
- **Evidence loading**:
  - UI loads evidence JSON files created by `EvidenceStore` using `ui.utils.helpers.load_evidence_json`.
  - Evidence tree and metrics/log views are derived from the `host`, `containers`, and `os_nodes` structures.
- **User interactions**:
  - User selects hosts/components/time ranges, which drive filtered views over the already-collected evidence.
  - When the user triggers RCA/IR, UI invokes the remote reasoning flow described above.

---

## Runtime Entrypoints

- **CLI**:
  - `python main.py ...` or installed console script `grcai`:
    - `main()` in `main.py` with subcommands:
      - `collect` – evidence collection only.
      - `rca` – run RCA on an existing evidence JSON file.
- **Streamlit UI**:
  - `streamlit run ui/app.py`:
    - Launches the 6-tab web UI described above.
    - Relies on `GRCAI_SESSIONS_HOME` for evidence location and `GRCAI_CONFIG_HOME` (or `config/`) for environment definitions.
- **Sanity tooling**:
  - `python sanity/sanity_validate_reports.py` (plus `sanity.sh`) for developer/test workflows around evidence/report validation.
- **Container entrypoints**:
  - `entrypoint.sh` and Dockerfiles (`Dockerfile.client`, `docker-compose.dev.yml`) wrap the above entrypoints for containerized deployments.

---

## Assumptions About Other Repos and Systems

### `grcai-contracts` repository
- **Source of truth for contracts**:
  - All schemas, OpenAPI specs, and event definitions live in the **separate** `grcai-contracts` repo.
  - This client repo assumes:
    - A `contracts/` directory is available at runtime (either via `CONTRACTS_PATH` or built into the container image).
    - `contracts/version.txt` contains a semantic version used to populate evidence metadata headers.
- **Build/dev assumptions** (from `contracts/README.md`):
  - Local dev:
    - `CONTRACTS_PATH` is typically set to `../grcai-contracts` or similar.
  - Docker builds:
    - The Docker build context includes both `grcai-client` and `grcai-contracts` (e.g., parent `grcai-mvp` directory), and contracts are copied into the image at `/grcai/contracts`.

### Central Reasoning Service (separate backend)
- **Ownership**:
  - Implemented in a separate backend repository (not part of `grcai-client`).
  - Exposes HTTP APIs consumed by `RCAClient`.
- **Behavioral assumptions**:
  - Accepts sanitized evidence adhering to contracts version advertised by the client.
  - Performs all heavy reasoning, interpretation, and LLM interaction.
  - Returns:
    - Human-readable RCA and incident-response texts.
    - Structured tasks and metadata compatible with the client’s UI/CLI expectations.

### Environment/configuration sources
- **Config location**:
  - Runtime environment is expected to provide environment YAMLs under:
    - `GRCAI_CONFIG_HOME` (preferred override), or
    - Repo-local `config/` tree.
  - These files may be managed in a separate “infrastructure config” repo and mounted into containers/hosts.
- **Sessions and logs**:
  - Evidence sessions are stored in `GRCAI_SESSIONS_HOME` (or `grcai_sessions/` by default), which is assumed to be:
    - Writable by the client process.
    - Scoped to a trusted environment (stays within customer’s boundary).
  - Logs are written under `logs/grcai.log`.

---

## Summary

- The `grcai-client` repo provides a **trusted, on-premise collector and UI** that gathers diagnostic evidence from infrastructure, persists it in a safe format, and forwards **sanitized** observations to a separate Central Reasoning Service for LLM-powered RCA and incident response.
- Core responsibilities are clearly separated:
  - **Orchestrator + connectors** handle collection only.
  - **Client layer** handles sanitization and remote calls.
  - **Central service** (external) performs reasoning.
  - **UI/CLI** handle user interaction and presentation without interpreting evidence beyond visualization.

---

## Import & Path Assumptions

### Global Import Surface (by module)

- **`main.py`**
  - Standard libs: `argparse`, `os`, `sys`, `pathlib.Path`, `datetime.datetime`
  - Internal: `orchestrator.session_orchestrator.SessionOrchestrator`
- **`orchestrator.session_orchestrator`**
  - Standard libs: `os`, `sys`, `logging`, `logging.handlers`, `traceback`, `datetime.datetime`, `pathlib.Path`, `concurrent.futures`, `threading`, `time`
  - Internal:
    - `evidence_store.evidence_store.EvidenceStore`
    - `utils.print_utils.divider`
    - `client.rca_client.get_reasoning_client`
    - `connectors.registry.CONNECTOR_REGISTRY`
    - `config.config_loader.ConfigLoader`
    - `client.constraint_extractor.extract_minimal_constraints`
    - `connectors.host_connectors.{SSHHostConnector,DockerHostConnector,LocalHostConnector}`
- **`client.rca_client`**
  - Standard libs: `os`, `json`, `logging`, `time`, `pathlib.Path`, `typing` (`Optional`, `Dict`, `Any`, `List`, `Callable`)
  - Third-party: `requests`, `requests.adapters.HTTPAdapter`, `urllib3.util.retry.Retry`
  - Internal:
    - `client.evidence_sanitizer.{sanitize_evidence, validate_no_sensitive_data}`
    - `utils.version.{get_version as get_client_version, get_contracts_version}`
- **`client.evidence_sanitizer`**
  - Standard libs: `logging`, `copy`, `typing` (`Any`, `Dict`, `List`, `Tuple`), `sys`, `pathlib.Path`
  - Internal: `ui.utils.data_masking.DataMasker`
- **`client.constraint_extractor`**
  - Standard libs: `logging`
- **`evidence_store.evidence_store`**
  - Standard libs: `json`, `uuid`, `sys`, `os`, `pathlib.Path`, `datetime.datetime`, `datetime.timezone`, `importlib.util`
  - Internal:
    - `evidence_store.evidence_schema.SCHEMA_VERSION`
    - `utils.version.{get_version, get_build, get_contracts_version, semver_major}` (with dynamic import fallback)
- **`evidence_store.evidence_schema`**
  - Standard libs: `typing.TypedDict`, `typing.List`, `typing.Optional`, `datetime.datetime`
- **`config.config_loader`**
  - Standard libs: `os`, `logging`, `pathlib.Path`
  - Third-party: `yaml`
- **`utils.version`**
  - Standard libs: `os`, `subprocess`, `pathlib.Path`, `typing.Optional`, `importlib.metadata`, `tomllib` / `tomli`
- **`ui.app`**
  - Third-party: `streamlit`, `streamlit.components.v1`, `pandas`, `yaml`
  - Standard libs: `datetime` (`datetime`, `date`, `time`, `timezone`), `pathlib.Path`, `json`, `sys`, `os`, `re`, `logging`
  - Internal:
    - `orchestrator.session_orchestrator.SessionOrchestrator`
    - `client.rca_client.get_reasoning_client`
    - `config.config_loader.ConfigLoader`
    - `ui.components.*` (`evidence_tree`, `metrics_viewer`, `log_viewer`, `evidence_highlights`, `llm_input_review`, `multipass_progress`)
    - `ui.utils.helpers.*`
    - Optional: `zoneinfo` / `backports.zoneinfo`
- **`ui.components.*`**
  - `llm_input_review`: `streamlit`, `json`, `pandas`, `re`, `sys`, `copy`, `pathlib.Path`, `typing`, `ui.utils.data_masking`, `ui.utils.log_filter`, `ui.components.evidence_tree`, `config.config_loader.ConfigLoader`
  - `evidence_highlights`: `streamlit`, `re`, `typing`, `ui.utils.concern_extractor`, `ui.components.evidence_tree`
  - `metrics_viewer`, `log_viewer`, `evidence_tree`, `multipass_progress`: various `streamlit`, `typing`, and local UI utils
- **`ui.utils.*`**
  - `helpers`: `json`, `typing`, `datetime.datetime`, `datetime.timezone`, `pathlib.Path`, etc.
  - `data_masking`, `log_filter`, `concern_extractor`: `re`, `typing`, `copy`, `datetime`, etc.
- **`connectors.*`**
  - `connectors.registry`: `typing`, `connectors.base_connector.BaseConnector`
  - `connectors.base_connector` / `connectors.base.base_component`: `datetime`, `typing`
  - `connectors.historical.*`: `os`, `glob`, `gzip`, `datetime`, `typing`, `pathlib.Path`, `logging`
  - `connectors.host_connectors.*`: `docker`, `subprocess`, `signal`, `paramiko` (in legacy backends), `typing`, `logging`
  - `connectors.adapters.*`: `logging`, `traceback`, `json`, `re`, `shlex`, `collections.Counter`, `datetime`, `typing`, `os`, and `connectors.base_connector`, `connectors.historical`
- **`sanity.sanity_validate_reports`**
  - Standard libs: `json`, `sys`, `pathlib.Path`

*(Minor/legacy `connectors/backends_bak.*` modules also import `docker`, `paramiko`, `socket`, `traceback`.)*

### Imports Referencing External Repo Paths

- **Contracts repo (`grcai-contracts`) – implicit via path usage**
  - `utils.version.get_contracts_path()`:
    - Returns `os.getenv("CONTRACTS_PATH")` or `Path(__file__).resolve().parents[1] / "contracts"`.
    - Assumes a sibling `contracts/` directory that is either:
      - A copy of the external `grcai-contracts` repo, or
      - A bind-mount into the container (e.g., from a parent `grcai-mvp` dir containing both repos).
  - `utils.version.get_contracts_version()`:
    - Reads `<contracts_path>/version.txt` to determine the contracts SemVer.
  - `evidence_store.evidence_store.EvidenceStore`:
    - Uses `get_contracts_version()` and `semver_major()` to embed contracts version and major number into evidence metadata.
  - `client.rca_client.RCAClient`:
    - Uses `get_contracts_version()` to send `X-GRCAI-Contracts-Version` in every HTTP request to the Central Reasoning Service.
- **Contracts README**
  - `contracts/README.md` documents:
    - Local dev: `CONTRACTS_PATH=../grcai-contracts`.
    - Docker build: copies `../grcai-contracts` into the image at `/grcai/contracts`.

**Net effect**: While no module directly imports from `grcai-contracts`, several imports depend on a contracts directory that is assumed to be provided by that **separate repo**, typically via `CONTRACTS_PATH` or a shared parent layout (`grcai-mvp` containing both repos).

### Imports That Assume Local Filesystem Layout

- **Repository root on `sys.path`**
  - `main.py`:
    - Computes `BASE = Path(__file__).resolve().parents[0]` and `sys.path.append(str(BASE))` so `orchestrator`, `client`, `ui`, `connectors`, etc. can be imported when running `python main.py` from repo root (without installing as a package).
  - `orchestrator.session_orchestrator`:
    - Derives `_REPO_ROOT = Path(__file__).resolve().parents[1]`, ensures it is at the front of `sys.path`, and also inserts `os.getcwd()` as fallback. This assumes the code is running from within a working copy and not necessarily as an installed package.
  - `evidence_store.evidence_store`:
    - Similar pattern: computes `_REPO_ROOT` two levels up, forcibly moves it to the front of `sys.path`, and also adds `os.getcwd()`. This favors the local repo layout over installed packages for `utils.version` and other modules.
  - `client.evidence_sanitizer`:
    - Uses `Path(__file__).resolve().parents[1]` to get repo root and injects it into `sys.path` to import `ui.utils.data_masking`.

- **Config and sessions directories**
  - `config.config_loader.ConfigLoader`:
    - Defaults to `base_dir = os.environ.get("GRCAI_CONFIG_HOME", "config")`.
    - Assumes environment YAMLs live under `config/<env>/<env>.yaml` when `GRCAI_CONFIG_HOME` is not set.
  - `evidence_store.evidence_store._sessions_home()` and `EvidenceStore.__init__`:
    - Use `GRCAI_SESSIONS_HOME` or default to `grcai_sessions/` in the current working directory.
  - `orchestrator.session_orchestrator.LOG_DIR`:
    - Uses a relative `logs/` directory (`Path("logs")`) to store `grcai.log`.

- **UI assumptions**
  - `ui.app`:
    - Computes `BASE = Path(__file__).resolve().parents[1]` and appends it to `sys.path` so it can import `orchestrator`, `client`, `config`, and `ui.*` modules when launched via `streamlit run ui/app.py`.
    - `_sessions_root()` derives sessions directory from `GRCAI_SESSIONS_HOME` or `grcai_sessions/` relative to current working directory.

**Net effect**: The client is designed to run directly from a repo checkout (without installation), and many modules assume:
- A conventional repo layout where the project root is two levels up from most modules.
- Relative directories `config/`, `grcai_sessions/`, `logs/`, and `contracts/` exist (or are overridden via environment variables).

### Imports / Code That Hardcode Contracts Path

- **`utils.version.get_contracts_path()`**
  - Logic:
    - `CONTRACTS_PATH` env var (primary).
    - Else: `Path(__file__).resolve().parents[1] / "contracts"`.
  - This effectively **hardcodes** the fallback path to a `contracts/` directory at the repository root (or inside the container at `/grcai/contracts` once copied there).
- **`utils.version.get_contracts_version()`**
  - Assumes `version.txt` exists directly under the computed contracts path:
    - `Path(get_contracts_path()) / "version.txt"`.
- **Transitive consumers (but not additional hardcoding)**
  - `evidence_store.evidence_store` and `client.rca_client` rely on `get_contracts_version()` rather than hardcoding file paths themselves.

**Net effect**: The only explicit hardcoded contracts path semantics are in `utils.version.get_contracts_path()`:
- Contracts are either provided via `CONTRACTS_PATH` or expected to live in `../contracts` relative to the source tree (which, in practice, is the mounted `grcai-contracts` repo or the directory copied into the Docker image). All other modules consume this abstraction rather than hardcoding their own paths.
