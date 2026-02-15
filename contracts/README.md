# Contracts (grcai-contracts)

This directory is the **contracts** payload for the client at runtime.

- **Dev (docker-compose):** When `grcai-contracts` is a sibling of `grcai-client` (e.g. in `grcai-mvp`), it is mounted here in the dev container; no need to copy.
- **Production build:** Either copy the contents of the `grcai-contracts` repo here before building the client image, or build from the monorepo root with context that includes `grcai-contracts` (see docs/dev-docker.md).

`CONTRACTS_PATH` in the client points to this directory (e.g. `/grcai/contracts` in Docker).
