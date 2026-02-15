# Contracts (not in this repo)

Contract files (schemas, OpenAPI) live in the **grcai-contracts** repo. They are not stored here.

- **Local dev:** Set `CONTRACTS_PATH` to your grcai-contracts path (e.g. `../grcai-contracts`). Dev Compose mounts `../grcai-contracts` when present.
- **Docker build:** Build from the directory that contains both `grcai-contracts` and `grcai-client` (e.g. grcai-mvp). The Dockerfile copies grcai-contracts into the image at `/grcai/contracts`.
