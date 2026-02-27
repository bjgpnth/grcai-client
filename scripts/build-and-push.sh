#!/usr/bin/env bash
set -euo pipefail

# Build and push Client image to registry.militva.dev.
# Usage:
#   ./scripts/build-and-push.sh [tag]
#
# Env overrides:
#   REGISTRY (default: registry.militva.dev)
#   IMAGE_REPO (default: grcai/client)
#   PUSH (default: 1)  # set to 0 to skip push

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

REGISTRY="${REGISTRY:-registry.militva.dev}"
IMAGE_REPO="${IMAGE_REPO:-grcai/client}"
PUSH="${PUSH:-1}"

TAG="${1:-}"
if [[ -z "$TAG" ]]; then
  if command -v python3 >/dev/null 2>&1; then
    TAG="$(python3 -c 'from __version__ import __version__; print(__version__)' 2>/dev/null || true)"
  elif command -v python >/dev/null 2>&1; then
    TAG="$(python -c 'from __version__ import __version__; print(__version__)' 2>/dev/null || true)"
  fi
fi
if [[ -z "$TAG" ]]; then
  echo "❌ Could not determine tag. Provide one: ./scripts/build-and-push.sh 0.1.0"
  exit 1
fi

IMAGE="${REGISTRY}/${IMAGE_REPO}:${TAG}"

CONTRACTS_VERSION="unknown"
if [[ -f "contracts/version.txt" ]]; then
  CONTRACTS_VERSION="$(tr -d '\n' < contracts/version.txt)"
fi

REV="unknown"
if command -v git >/dev/null 2>&1; then
  REV="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
fi

echo "Building: $IMAGE"
echo "  - contracts: $CONTRACTS_VERSION"
echo "  - revision:  $REV"

docker build \
  -f Dockerfile.client \
  -t "$IMAGE" \
  --label "org.opencontainers.image.version=${TAG}" \
  --label "org.opencontainers.image.revision=${REV}" \
  --label "org.grcai.contracts.version=${CONTRACTS_VERSION}" \
  .

if [[ "$PUSH" == "1" ]]; then
  echo "Pushing: $IMAGE"
  docker push "$IMAGE"
else
  echo "Skipping push (PUSH=$PUSH)"
fi

