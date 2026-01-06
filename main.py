# main.py
import argparse
import os
import sys
from pathlib import Path
from datetime import datetime

# make local modules importable
BASE = Path(__file__).resolve().parents[0]
sys.path.append(str(BASE))

from orchestrator.session_orchestrator import SessionOrchestrator

def cmd_collect(args):
    orchestrator = SessionOrchestrator()

    if args.interactive:
        orchestrator.run_interactive_session()
        return

    comps = [c.strip() for c in (args.components or "").split(",") if c.strip()]
    if not comps:
        print("No components specified. Use --components tomcat,os or run with --interactive")
        return

    if not args.environment:
        print("Missing --environment (dev/qa/uat/prod).")
        return

    issue_time = datetime.utcnow() if not args.issue_time else datetime.fromisoformat(args.issue_time)

    res = orchestrator.run_non_interactive(
        issue_time=issue_time,
        components=comps,
        observations=(args.observations or ""),
        environment=args.environment
    )
    print(f"\n💾 Evidence saved to: {res['evidence_file']}")

def cmd_rca(args):
    orchestrator = SessionOrchestrator()
    evidence_file = args.evidence_file
    if not Path(evidence_file).exists():
        print(f"Evidence file not found: {evidence_file}")
        return

    api_key = os.getenv("OPENAI_API_KEY") or args.api_key
    if not api_key:
        print("Missing OPENAI_API_KEY env variable or --api-key.")
        return

    model = args.model or "gpt-4o-mini"
    print("🤖 Running LLM reasoning...")
    rca_text, tasks = orchestrator.run_rca_on_file(
        evidence_file=evidence_file,
        api_key=api_key,
        model=model,
    )

    print("\n================ RCA SUMMARY ================\n")
    print(rca_text)
    print("\n=============================================\n")

    if tasks:
        print("\n================ TASK OWNERSHIP MATRIX ================\n")
        for t in tasks:
            print(f"[{t.get('team','Infra')}] {t.get('task','<task>')} — {t.get('priority','Medium')} — {t.get('effort','M')}")
    print("\nDone.")

def build_parser():
    p = argparse.ArgumentParser(prog="grcai", description="GRCAI CLI - collect evidence and run RCA")
    sp = p.add_subparsers(dest="cmd")

    # collect
    pc = sp.add_parser("collect", help="Collect evidence (no LLM).")
    pc.add_argument("--interactive", action="store_true", help="Interactive CLI mode")
    pc.add_argument("--components", type=str, help="Comma separated components: tomcat,os,nginx")
    pc.add_argument("--issue-time", type=str, help="ISO timestamp e.g. 2025-11-16T12:45:00")
    pc.add_argument("--observations", type=str, help="Observations")
    pc.add_argument("--environment", type=str, required=True, help="Environment: dev/qa/uat/prod")
    pc.set_defaults(func=cmd_collect)

    # rca
    pr = sp.add_parser("rca", help="Run RCA on existing evidence JSON")
    pr.add_argument("evidence_file", type=str)
    pr.add_argument("--api-key", type=str)
    pr.add_argument("--model", type=str)
    pr.set_defaults(func=cmd_rca)

    return p

def main():
    parser = build_parser()
    args = parser.parse_args()
    if not args.cmd:
        parser.print_help()
        return
    args.func(args)

if __name__ == "__main__":
    main()
