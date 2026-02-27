#!/usr/bin/env bash
set -euo pipefail

# Deploy Client by pulling from registry.militva.dev (no builds on deploy hosts).
# Usage:
#   ./scripts/deploy-from-registry.sh [tag]
#
# Env overrides:
#   REGISTRY (default: registry.militva.dev)
#   IMAGE_REPO (default: grcai/client)
#   CONTAINER_NAME (default: grcai-client-runtime)
#   UI_PORT (default: 8501)
#   GRCAI_CENTRAL_URL (default: http://localhost:8000)
#   OPENAI_API_KEY (required)
#   DOCKER_NETWORK (optional): if set, container joins this network and defaults Central URL to http://grcai-central-service:8000
#
# Volumes (override as needed):
#   GRCAI_SESSIONS_HOME (default: $HOME/grcai_sessions)
#   GRCAI_CONFIG_HOME (default: $HOME/config)

REGISTRY="${REGISTRY:-registry.militva.dev}"
IMAGE_REPO="${IMAGE_REPO:-grcai/client}"
TAG="${1:-latest}"
IMAGE="${REGISTRY}/${IMAGE_REPO}:${TAG}"

CONTAINER_NAME="${CONTAINER_NAME:-grcai-client-runtime}"
UI_PORT="${UI_PORT:-8501}"
DOCKER_NETWORK="${DOCKER_NETWORK:-}"

if [[ -z "${GRCAI_CENTRAL_URL:-}" ]]; then
  if [[ -n "$DOCKER_NETWORK" ]]; then
    # When Central is on the same Docker network, reach it by container name.
    GRCAI_CENTRAL_URL="http://grcai-central-service:8000"
  else
    # localhost inside the client container is NOT the host; this is only correct when Central
    # is reachable via a host network mapping or external URL.
    GRCAI_CENTRAL_URL="http://localhost:8000"
  fi
fi

if [[ -z "${OPENAI_API_KEY:-}" ]]; then
  echo "❌ OPENAI_API_KEY is required (export it before running)."
  exit 1
fi

SESSIONS_DIR="${GRCAI_SESSIONS_HOME:-${HOME}/grcai_sessions}"
CONFIG_DIR="${GRCAI_CONFIG_HOME:-${HOME}/config}"

mkdir -p "$SESSIONS_DIR" "$CONFIG_DIR"

echo "Pulling: $IMAGE"
docker pull "$IMAGE"

echo "Stopping existing container (if any): $CONTAINER_NAME"
docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true

echo "Starting: $CONTAINER_NAME"
if [[ -n "$DOCKER_NETWORK" ]]; then
  if ! docker network inspect "$DOCKER_NETWORK" >/dev/null 2>&1; then
    echo "Creating network: $DOCKER_NETWORK"
    docker network create "$DOCKER_NETWORK" >/dev/null
  fi

  docker run -d \
    --name "$CONTAINER_NAME" \
    --restart unless-stopped \
    --network "$DOCKER_NETWORK" \
    -p "${UI_PORT}:8501" \
    -e GRCAI_CENTRAL_URL="$GRCAI_CENTRAL_URL" \
    -e OPENAI_API_KEY="$OPENAI_API_KEY" \
    -v "${SESSIONS_DIR}:/grcai/grcai_sessions" \
    -e GRCAI_SESSIONS_HOME=/grcai/grcai_sessions \
    -v "${CONFIG_DIR}:/config" \
    -e GRCAI_CONFIG_HOME=/config \
    -v "${HOME}/.ssh:/home/grcai/.ssh:ro" \
    -v /var/run/docker.sock:/var/run/docker.sock:ro \
    "$IMAGE" \
    streamlit run ui/app.py --server.port=8501 --server.address=0.0.0.0 --server.headless=true
else
  docker run -d \
    --name "$CONTAINER_NAME" \
    --restart unless-stopped \
    -p "${UI_PORT}:8501" \
    -e GRCAI_CENTRAL_URL="$GRCAI_CENTRAL_URL" \
    -e OPENAI_API_KEY="$OPENAI_API_KEY" \
    -v "${SESSIONS_DIR}:/grcai/grcai_sessions" \
    -e GRCAI_SESSIONS_HOME=/grcai/grcai_sessions \
    -v "${CONFIG_DIR}:/config" \
    -e GRCAI_CONFIG_HOME=/config \
    -v "${HOME}/.ssh:/home/grcai/.ssh:ro" \
    -v /var/run/docker.sock:/var/run/docker.sock:ro \
    "$IMAGE" \
    streamlit run ui/app.py --server.port=8501 --server.address=0.0.0.0 --server.headless=true
fi

echo "✅ Client deployed."
echo "UI: http://localhost:${UI_PORT}"

