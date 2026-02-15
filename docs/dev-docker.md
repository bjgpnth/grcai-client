# Client development with Docker (and contracts)

Run the client in a container with the repo mounted so you can edit on your laptop and test without rebuilding. Optional: mount **grcai-contracts** so the client can use shared schemas.

## Prerequisites

- Docker and Docker Compose
- (Optional) Sibling **grcai-contracts** repo (e.g. both under `grcai-mvp`) for contracts support

## 1. Contracts (optional but recommended)

The client uses **grcai-contracts** for schemas and OpenAPI. Two ways to provide them:

**A. Dev: mount contracts (no copy)**  
If you have both repos (e.g. `grcai-mvp/grcai-client` and `grcai-mvp/grcai-contracts`), the dev Compose file mounts `../grcai-contracts` into the container at `/grcai/contracts` and sets `CONTRACTS_PATH=/grcai/contracts`. No extra steps.

**B. Production build: copy contracts into image at build time**  
- From monorepo root run `./build-with-contracts.sh` to build both images. The Dockerfiles use build context = monorepo root and COPY grcai-contracts into the image; contracts are not stored in grcai-client or grcai-central repos.
- Or from monorepo root: `docker build -f grcai-client/Dockerfile.client -t grcai/client .` (same for Central with grcai-central/Dockerfile).

The image always has a `/grcai/contracts` directory; in dev it is overridden by the volume when contracts are mounted.

## 2. One-time build

From **grcai-client**:

```bash
cd grcai-client
docker compose -f docker-compose.dev.yml build
```

## 3. Start the dev container

Set Central URL and API key (e.g. in `.env` or export):

```bash
export GRCAI_CENTRAL_URL=https://grcai-central-dev.militva.dev   # or http://host.docker.internal:8000 for local Central
export OPENAI_API_KEY=your-openai-key
docker compose -f docker-compose.dev.yml up -d
```

If **grcai-contracts** is at `../grcai-contracts`, it will be mounted automatically and `CONTRACTS_PATH` will point to it.

**Config:** The dev Compose mounts `$HOME/config` to `/config` and sets `GRCAI_CONFIG_HOME=/config`, so the app uses your host config. Create envs under `$HOME/config/<env>/<env>.yaml` (e.g. `$HOME/config/initial/initial.yaml`). Optional: set `GRCAI_CONFIG_HOME` to override.

**Sessions:** Evidence and reports are stored in `$HOME/grcai_sessions` (client-specific, sensitive). The dev Compose mounts it to `/grcai/grcai_sessions` and sets `GRCAI_SESSIONS_HOME`. Create it if missing: `mkdir -p $HOME/grcai_sessions`. Optional: set `GRCAI_SESSIONS_HOME` to override.

## 4. Run the UI inside the container

```bash
docker exec -it grcai-client-dev ./ui.sh
```

Or without the script:

```bash
docker exec -it grcai-client-dev streamlit run ui/app.py --server.address=0.0.0.0 --server.port=8501
```

Open **http://localhost:8501**. Code changes on your machine are visible in the container; restart Streamlit if needed.

## 5. Run CLI inside the container

```bash
docker exec -it grcai-client-dev python main.py collect --environment qa --components os,tomcat
```

## 6. Stop

```bash
docker compose -f docker-compose.dev.yml down
```

## Summary

| Step | Commands / notes |
|------|-------------------|
| Contracts in dev | Have `grcai-contracts` next to `grcai-client`; Compose mounts it and sets `CONTRACTS_PATH` |
| Contracts in image | Copy grcai-contracts into `grcai-client/contracts/` before `docker build`, or build from monorepo |
| Build once | `docker compose -f docker-compose.dev.yml build` |
| Start | `docker compose -f docker-compose.dev.yml up -d` (set `GRCAI_CENTRAL_URL`, `OPENAI_API_KEY`) |
| Run UI | `docker exec -it grcai-client-dev ./ui.sh` |
| Run CLI | `docker exec -it grcai-client-dev python main.py ...` |
| Config | `$HOME/config` on host → `/config` in container; optional `GRCAI_CONFIG_HOME` override |
| Sessions | `$HOME/grcai_sessions` on host → `/grcai/grcai_sessions` in container; optional `GRCAI_SESSIONS_HOME`; create if missing |
