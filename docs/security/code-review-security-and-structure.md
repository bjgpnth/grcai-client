# Code Review: Security, Data Leak, and Structural Improvements

**Document generated:** Wednesday, February 11, 2026 — 15:11 IST  
**Scope:** Full codebase review focused on security (client-sensitive data → LLM), data leak (especially system logs → LLM), and structural improvements.

---

## 1. Security — Does client-sensitive information ever reach the LLM?

### Remote path (RCAClient) — **Safe**

- **Single outbound path:** The only code that sends evidence off the machine is `RCAClient._make_request()` (see `docs/info-flow.md` and code).
- **Sanitization is mandatory:** In `client/rca_client.py`, every request does:
  - `sanitized_evidence, _ = sanitize_evidence(evidence, mask_sensitive=True)` (no bypass).
  - `validate_no_sensitive_data(sanitized_evidence)` (warning only; see below).
- So **all** evidence sent to the Central service (and thus to the LLM) goes through:
  - Structural removal of sensitive **keys** (`client/evidence_sanitizer.py`: passwords, tokens, `address`, etc.).
  - Content **masking** via `DataMasker` (IPs, JWTs, API keys, emails, connection strings, AWS keys, etc.) in `ui/utils/data_masking.py` and reused in `evidence_sanitizer`.

**Conclusion:** For the remote path, client-sensitive information does not reach the LLM in raw form; it is either removed or masked.

### UI "prepared evidence" and masking-off

- In `ui/components/llm_input_review.py`, the user can disable **Enable Data Masking** and confirm "I understand I'm sending raw data." The returned `final_prepared_evidence` can then be **unmasked**.
- In `ui/app.py`, when generating the RCA, that prepared evidence is written to a temp file and passed to `reasoner.analyze(tmp_evidence_file)`.
- **RCAClient.analyze()** (used when `GRCAI_REASONING_MODE != "local"`) **re-reads** that file and passes it through `_make_request()`, which **always** sanitizes again. So **even with "masking off" in the UI, the payload actually sent to the server is still sanitized.**

So in the current (remote) flow, **client-sensitive information does not reach the LLM**; the second sanitization in `RCAClient` is the enforcement point.

### Local / LLMReasoner path — **Risk if used with unmasked data**

- `get_reasoning_client()` in `client/rca_client.py` can return an `LLMReasoner` from `llm.llm_reasoner` when `GRCAI_REASONING_MODE=local`.
- That module is **not in this repo**. If a local reasoner simply reads the temp file and sends it to an LLM without sanitization, then **with "masking off" in the UI, unmasked evidence (including logs) could be sent to the LLM.**
- **Recommendation:** If you support local mode, either have `LLMReasoner` (or the code that builds the prompt) call the same `sanitize_evidence()` before sending to the LLM, or document that local mode must only be used with "masking on" in the UI and/or with evidence that is already safe to send.

---

## 2. Data leak — System logs and other data to the LLM

### Logs are part of the evidence sent to the LLM

- Evidence is built from adapters that attach logs to instances (e.g. `connectors/adapters/`: nginx, tomcat, postgres, redis, nodejs, mssql, generic_docker, etc.).
- Log content is stored in structures like `instance["logs"]["container_logs"]`, or `instance["access_log"] = {"content": "..."}`, `instance["catalina_out_tail"] = {"content": "..."}`, etc.
- This evidence (including logs) is what gets shown in the LLM Input Review, written to the temp file, and then (in remote mode) loaded by `RCAClient.analyze()` and passed to `_make_request()` → `sanitize_evidence()` → Central → LLM.

So **yes, system (and app) logs do reach the LLM**, but in remote mode only **after** structural removal and content masking. So the risk is controlled **if** masking and structural removal are sufficient and **if** log filtering doesn't inadvertently leave highly sensitive patterns in kept lines (see below).

### Log filtering vs masking

- **Log filter** (`ui/utils/log_filter.py`): Reduces **volume** (noise lines removed, context around errors kept). It does **not** redact sensitive content; it only decides which lines are kept.
- **Masking** (`DataMasker` / `sanitize_evidence`): Redacts sensitive **patterns** in all string values (including log content) in the payload that is actually sent.

So:

- **Remote path:** Logs that reach the LLM are (1) optionally reduced by the log filter and (2) **always** masked by the client before send. That limits log-driven data leak in practice.
- **Validation:** In `evidence_sanitizer.validate_no_sensitive_data()`, paths containing `.logs.` or `.log` or ending in `.content` are **skipped** for value checks (to avoid flagging "password" in log messages). So validation does **not** double-check that log content is free of sensitive patterns; the only protection there is masking. That's acceptable as long as masking is always applied on send (which it is for RCAClient).

### Log filter coverage — **Structural gap**

- In `log_filter.py`, `_filter_logs_recursive()` only runs `_filter_log_text()` on string values when **the key name** contains `"log"` (e.g. `container_logs`, `access_log`).
- Many adapters store log text under a **nested** key **without** "log" in the name, e.g. `instance["access_log"] = {"content": "..."}` — recursion goes into `access_log`, then key `"content"` (no "log") → **that string is never passed to _filter_log_text**. Same for `catalina_out_tail`, `error_log`, etc. when the actual text is under `"content"`.
- So a lot of log content **is not line-filtered** at all; only masking applies. Result:
  - **Security:** Still protected by masking on send (remote path).
  - **Data leak / size:** More log lines than intended can be sent; you rely on masking rather than reducing volume for those fields.

**Recommendation:** Extend log filtering so that string values are considered "log content" when they are under a key named `"content"` (or `"lines"`) and the parent key suggests logs (e.g. parent key contains `"log"` or is a known log key like `catalina_out_tail`), and run `_filter_log_text` on those strings as well. That way log filtering and "what goes to the LLM" stay aligned and you reduce both volume and residual risk.

---

## 3. Structural improvements

### 3.1 Single source of truth for "what is sent to the LLM"

- **Today:** The UI builds "prepared" evidence (component selection, optional masking, optional log filtering) and shows a preview. The sending path (`RCAClient`) **ignores** that preparation for security and **re-sanitizes** the whole payload. So the UI can show "what will be sent" but the actual sent payload is defined by the client's sanitizer, not by the same code path that produced the preview.
- **Improvement:** Use one shared pipeline for "evidence → sanitize → (optional) reduce for display" so that the same `sanitize_evidence()` (and, if desired, reduction) is used for both preview and send. Then the UI "payload preview" can truly reflect what is sent (e.g. after sanitization), and you avoid two different definitions of "safe payload."

### 3.2 Sanitization vs UI masking — one place to maintain

- **Today:** `client/evidence_sanitizer.py` uses `ui.utils.data_masking.DataMasker` and adds structural removal + validation. UI masking is in `ui/utils/data_masking.py` and is optional in the review tab.
- **Improvement:** Keep a single masking implementation (e.g. under `client/` or a shared `security/` or `sanitization/` package) and have both the UI "preview" and the client's send path call it. That way new sensitive patterns or structural rules are added once and apply everywhere.

### 3.3 Evidence sanitizer dependency on UI

- **Today:** `client/evidence_sanitizer.py` does `sys.path` manipulation and imports `from ui.utils.data_masking import DataMasker`. So the **client** (used also by CLI and orchestrator) depends on **UI** code.
- **Improvement:** Move the masking logic (and any shared constants) into a package that both `client` and `ui` can import (e.g. `client/sanitization.py` or a top-level `sanitization/`). Then the client does not depend on the UI, and CLI/orchestrator remain UI-free.

### 3.4 Over-removal in structural sanitization

- **Today:** `SENSITIVE_FIELDS_TO_REMOVE` includes `"address"`. Any non–top-level key whose name contains "address" (e.g. `client_address`, `remote_address`, `binding_address`) is **removed** entirely, so the LLM never sees that a field existed or a masked value (e.g. `10.45.XX.XX`).
- **Improvement:** For RCA, "there was an address (masked)" can be useful. Consider either removing `"address"` from structural removal and relying on content masking for address values, or replacing the field with a placeholder (e.g. `"client_address": "[REDACTED]"`) instead of deleting the key, so structure is preserved for reasoning.

### 3.5 Validation behavior

- **Today:** `validate_no_sensitive_data()` only **warns** (logs and returns violations); it does not block the request. So misconfiguration or a bug in sanitization could still send bad data.
- **Improvement:** Add a configurable or environment-driven "strict" mode that **fails** the request (or the export) if violations are present, so that in high-assurance deployments you can enforce "never send if validation fails."

### 3.6 Log filter API vs usage

- **Today:** The public `filter_logs(logs, ...)` in `log_filter.py` is documented as taking "Dictionary of log data," but callers pass the **full evidence** dict (`_prepare_evidence()` in `llm_input_review.py`). The implementation recurses over the whole structure, which works but is easy to misuse.
- **Improvement:** Either rename/document as "evidence dict" and keep the recursive behavior, or add a thin wrapper that clearly takes `evidence: Dict` and call it `filter_logs_in_evidence()` so the contract is obvious.

### 3.7 UI app size and responsibilities

- **Today:** `ui/app.py` is very large (thousands of lines) and mixes layout, report generation, evidence loading, and reasoning client usage.
- **Improvement:** Split by tab or feature (e.g. "Evidence Review," "Reports," "IR History") into submodules or components, and keep `app.py` as a thin router and session state coordinator. That will make it easier to trace data flow (e.g. "prepared evidence" → temp file → reasoner) and to add tests.

---

## Summary table

| Area | Finding | Severity |
|------|--------|----------|
| **Security** | Remote path always sanitizes before send; sensitive client data does not reach the LLM. | ✅ Good |
| **Security** | Local LLMReasoner path is not in repo; if it sends file content as-is, unmasked data could reach the LLM. | ⚠️ Mitigate in local reasoner or docs |
| **Data leak** | System logs are part of evidence and do reach the LLM in remote mode only after masking. | ✅ Acceptable |
| **Data leak** | Log filter skips many log fields (e.g. `"content"` under log-like keys); more lines sent than intended. | ⚠️ Improve filter coverage |
| **Structure** | Client depends on UI for masking; two definitions of "safe payload" (UI vs RCAClient). | ⚠️ Refactor for single pipeline and shared sanitization |
| **Structure** | Sanitizer removes `"address"` keys entirely; consider masking value and keeping key. | Low |
| **Structure** | Validation is warning-only; consider strict mode that blocks send. | Low |
