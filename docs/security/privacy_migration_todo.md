# gRCAi Privacy-by-Construction Migration TODO

## Goal
Move from:

collect → store raw → sanitize → send

to:

collect → enforce → store safe → send safe

After completion:

Evidence files and outbound payloads must be inherently non-sensitive even if leaked.

---

# Phase 1 — Establish the Security Boundary (Foundation)

## 1. Create Central Enforcement Library
Create a new module (example: `client/privacy_enforcer/`)

Implement:

- validate_structure(evidence)
- detect_sensitive_patterns(evidence)
- normalize_adapter_output(raw_adapter_data)
- transform(raw_adapter_data) -> safe_evidence

Rules:

- Must fail closed
- Never silently clean data
- Raise `PrivacyViolationError`

Adapters must never write directly to `EvidenceStore`.

---

## 2. Define Safe Evidence Schema
Implement strict schema validation (JSON Schema or Pydantic recommended)

Safe evidence MUST contain only:

- component name
- logical host name (no IP/FQDN)
- timestamps
- aggregated counts
- categorized events
- state flags
- resource conditions

Safe evidence MUST NOT contain:

- free-form text logs
- request paths
- payload fragments
- identifiers
- tokens
- stack trace bodies

Reject evidence if unknown fields exist.

---

## 3. Wire Enforcement Into Persistence
Modify `EvidenceStore.save_session()` call sites:

Replace:

EvidenceStore.save(raw_evidence)

With:

safe_evidence = privacy_enforcer.transform(raw_evidence)
EvidenceStore.save(safe_evidence)

Guarantee:

Nothing reaches disk without enforcement.

---

# Phase 2 — Adapter Compliance

## 4. Update Adapter Contract
Update adapter developer documentation:

Adapters return signals, not logs.

Disallowed adapter outputs:

- logs
- content
- access_log
- catalina_out
- stdout
- stderr

Allowed outputs:

- events
- metrics
- status
- counts

---

## 5. Refactor Existing Adapters
For each adapter:

Replace patterns like:

return {"logs": "...timeout connecting to redis..."}

With:

return {"events": [{"type": "dependency_timeout", "dependency": "redis"}]}

Do NOT rely on sanitizer to clean adapter output.

---

## 6. Introduce Event Vocabulary
Create `events.py` containing allowed events:

Resource
- cpu_saturation
- memory_pressure
- disk_full

Dependency
- dependency_timeout
- connection_refused
- pool_exhausted

Application
- internal_error
- exception_detected
- restart_detected

Traffic
- latency_spike
- error_rate_spike
- retry_detected

Reject unknown events.

---

# Phase 3 — Remove Log Transport

## 7. Remove Raw Logs From Evidence Schema
Delete from stored evidence:

- logs
- content
- stack_trace
- process_output

Logs may remain only in temporary in-memory UI display.

---

## 8. Update UI Review
Change UI preview:

Old:
Review logs sent to LLM

New:
Review derived observations

Remove “disable masking” option.

---

## 9. Simplify Sanitizer
After enforcement is active:

- keep sanitizer only for defense-in-depth
- validation failure should block send in strict mode

---

# Phase 4 — Network & Reasoning Safety

## 10. Harden RCAClient
Add strict mode:

If validate_no_sensitive_data() finds violations → abort request

Local reasoning mode must call sanitizer too.

---

## 11. Single Outbound Pipeline
Ensure all reasoning paths use:

safe_evidence → RCAClient → Central → LLM

No alternate LLM calls allowed.

---

# Phase 5 — Verification & Guarantees

## 12. Unit Tests (Mandatory)
Create tests asserting:

- raw log input → rejected
- IP present → rejected
- email present → rejected
- unknown fields → rejected
- safe events → accepted

---

## 13. Regression Guard
Add CI test:

Scan produced evidence JSON for:

- IPv4 patterns
- emails
- URLs
- long base64 blobs

Fail build if found.

---

## 14. Documentation
Update security documentation:

- Evidence files safe to share
- Client never exports operational data
- Privacy enforced at collection boundary

---

# Completion Criteria

The migration is complete when:

- Evidence files contain no free-form operational text
- Adapters cannot store raw logs
- Outbound payload equals stored evidence
- Sanitizer becomes fallback protection
- A leaked evidence file reveals system behavior but no customer or operational data

---

# Final Expected Security Property

Even if:

- evidence files leak
- central service compromised
- LLM provider breached

The attacker learns only:

system failure characteristics

They cannot learn:

who used the system or what data the system processed