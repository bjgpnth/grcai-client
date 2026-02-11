




            TRUSTED ENVIRONMENT (CLIENT MACHINE)
 ┌────────────────────────────────────────────────────────────┐
 │                                                            │
 │   Collect → Analyze → Sanitize → Export                    │
 │                                                            │
 └──────────────────────────────┬─────────────────────────────┘
                                │
                                │ HTTP POST (sanitized only)
                                ▼
                  UNTRUSTED REASONING SERVICE (SERVER)
                                ▼
                               LLM

1️⃣ Evidence Collection Phase (Raw Data Created)

SessionOrchestrator
    │
    ├─ SSHHostConnector.exec_cmd()
    ├─ DockerHostConnector.get_container_logs()
    ├─ read_file()
    │
    ▼
Service Adapters (nginx, tomcat, etc.)
    │
    ▼
Raw Evidence Structure
    ├─ logs (raw text)
    ├─ metrics
    ├─ stack traces
    ├─ process output
    └─ request lines

⚠️ At this stage: Contains confidential data. Nothing leaves the machine yet.

2️⃣ Evidence Persistence Phase

SessionOrchestrator
    │
    ├─ SSHHostConnector.exec_cmd()
    ├─ DockerHostConnector.get_container_logs()
    ├─ read_file()
    │
    ▼
Service Adapters (nginx, tomcat, etc.)
    │
    ▼
Raw Evidence Structure
    ├─ logs (raw text)
    ├─ metrics
    ├─ stack traces
    ├─ process output
    └─ request lines

Still raw. Still local. No network activity

3️⃣ Local Analysis & UI Phase (Optional but Important)

These operate ONLY locally:
    ConcernExtractor      → detects problems
    LogFilter             → removes normal lines
    DataMasker            → masks tokens
    Helpers/UI            → display only


4️⃣ Export Boundary — Critical Step

    RCAClient.analyze(evidence_file)

This is the ONLY outbound data path.

The Sanitization Pipeline (Most Important Part)

Inside _make_request():

Load evidence JSON
        │
        ▼
sanitize_evidence()
        │
        ├─ remove sensitive fields
        └─ mask tokens / IPs / emails
        ▼
validate_no_sensitive_data()
        │
        └─ warning only (does NOT block)
        ▼
HTTP POST to Central

So the exact outbound payload is:

    {
    "evidence": SANITIZED_EVIDENCE,
    "environment": "prod"
    }

    End to End flow 

                ┌──────────────────────────────────┐
                │        LOCAL COLLECTION          │
                └──────────────────────────────────┘

   Systems → Connectors → Adapters → Raw Evidence → JSON file
                                  (still confidential)


                ┌──────────────────────────────────┐
                │        LOCAL PROCESSING           │
                └──────────────────────────────────┘

   ConcernExtractor   (analysis only)
   LogFilter          (reduces noise)
   UI display         (local only)

   No external communication


                ┌──────────────────────────────────┐
                │       EXPORT BOUNDARY            │
                └──────────────────────────────────┘

             RCAClient.analyze()

                    │
                    ▼

            sanitize_evidence()
                    │
            remove sensitive keys
            mask tokens/IP/email
                    │
                    ▼
            validate_no_sensitive_data()
                    │
                    ▼
           HTTP POST → Central Service


                ┌──────────────────────────────────┐
                │ OUTSIDE TRUSTED ENVIRONMENT      │
                └──────────────────────────────────┘

           Central Service → LLM