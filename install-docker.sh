#!/bin/bash
# gRCAi Client - Docker Installation Script
# Usage: curl -fsSL https://raw.githubusercontent.com/yourorg/grcai-client/main/install-docker.sh | bash

set -e

REPO_URL="https://github.com/yourorg/grcai-client"
REPO_BRANCH="main"
INSTALL_DIR="${PWD}/grcai-client"
IMAGE_NAME="grcai/client:latest"
CONTAINER_NAME="grcai-client-runtime"

echo "========================================="
echo "gRCAi Client - Docker Installation"
echo "========================================="
echo ""

# Check Docker availability
if ! command -v docker &> /dev/null; then
    echo "❌ Error: Docker is not installed or not in PATH"
    echo "Please install Docker first: https://docs.docker.com/get-docker/"
    exit 1
fi

# Check if Docker daemon is running
if ! docker info &> /dev/null; then
    echo "❌ Error: Docker daemon is not running"
    echo "Please start Docker and try again"
    exit 1
fi

echo "✅ Docker found: $(docker --version)"
echo ""

# Download code
echo "Step 1: Downloading client code..."
if command -v git &> /dev/null; then
    echo "Using git to clone repository..."
    if [ -d "$INSTALL_DIR" ]; then
        echo "Directory $INSTALL_DIR exists. Removing..."
        rm -rf "$INSTALL_DIR"
    fi
    git clone -b "$REPO_BRANCH" "$REPO_URL" "$INSTALL_DIR"
else
    echo "Git not found. Downloading archive..."
    if [ -d "$INSTALL_DIR" ]; then
        rm -rf "$INSTALL_DIR"
    fi
    mkdir -p "$INSTALL_DIR"
    cd "$(dirname "$INSTALL_DIR")"
    
    # Try tar.gz first
    if command -v tar &> /dev/null; then
        curl -L "${REPO_URL}/archive/refs/heads/${REPO_BRANCH}.tar.gz" | tar -xz
        mv "grcai-client-${REPO_BRANCH}" "$(basename "$INSTALL_DIR")"
    elif command -v unzip &> /dev/null; then
        curl -L "${REPO_URL}/archive/refs/heads/${REPO_BRANCH}.zip" -o /tmp/repo.zip
        unzip -q /tmp/repo.zip -d /tmp
        mv "/tmp/grcai-client-${REPO_BRANCH}" "$INSTALL_DIR"
        rm /tmp/repo.zip
    else
        echo "❌ Error: Neither tar nor unzip found. Please install one of them."
        exit 1
    fi
fi

cd "$INSTALL_DIR"
echo "✅ Code downloaded to: $INSTALL_DIR"
echo ""

# Interactive configuration
echo "Step 2: Configuration"
echo "----------------------"
read -p "GRCAI_CENTRAL_URL [http://central-service:8000]: " CENTRAL_URL
CENTRAL_URL=${CENTRAL_URL:-http://central-service:8000}

read -sp "OPENAI_API_KEY (required): " API_KEY
echo ""
if [ -z "$API_KEY" ]; then
    echo "❌ Error: OPENAI_API_KEY is required"
    exit 1
fi

read -p "UI Port [8501]: " UI_PORT
UI_PORT=${UI_PORT:-8501}

echo ""
echo "✅ Configuration collected"
echo ""

# Create config template if missing
if [ ! -f "config/initial/initial.yaml" ]; then
    echo "Step 3: Creating initial configuration template..."
    mkdir -p config/initial
    if [ -f "config/template/initial.yaml.template" ]; then
        cp config/template/initial.yaml.template config/initial/initial.yaml
        echo "✅ Config template created at: config/initial/initial.yaml"
        echo "⚠️  Please edit this file with your environment details"
    else
        # Create minimal template
        cat > config/initial/initial.yaml <<EOF
hosts:
  - name: localhost
    type: vm
    address: localhost
    access:
      docker:
        use_local_socket: true
    services:
      os: {}
services:
  os: {}
EOF
        echo "✅ Minimal config created at: config/initial/initial.yaml"
        echo "⚠️  Please edit this file with your environment details"
    fi
    echo ""
fi

# Build Docker image
echo "Step 4: Building Docker image..."
docker build -f Dockerfile.client -t "$IMAGE_NAME" .
echo "✅ Docker image built: $IMAGE_NAME"
echo ""

# Stop existing container if running
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "Stopping existing container..."
    docker stop "$CONTAINER_NAME" || true
    docker rm "$CONTAINER_NAME" || true
fi

# Create directories for volumes
mkdir -p grcai_sessions

# Run container
echo "Step 5: Starting container..."
docker run -d \
  --name "$CONTAINER_NAME" \
  --restart unless-stopped \
  -p "${UI_PORT}:8501" \
  -e GRCAI_CENTRAL_URL="$CENTRAL_URL" \
  -e GRCAI_REASONING_MODE=remote \
  -e OPENAI_API_KEY="$API_KEY" \
  -e STREAMLIT_SERVER_PORT=8501 \
  -e STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
  -e STREAMLIT_SERVER_HEADLESS=true \
  -e STREAMLIT_BROWSER_GATHER_USAGE_STATS=false \
  -v "$(pwd)/grcai_sessions:/grcai/grcai_sessions" \
  -v "$(pwd)/config:/grcai/config:ro" \
  -v /var/run/docker.sock:/var/run/docker.sock:ro \
  "$IMAGE_NAME" \
  streamlit run ui/app.py --server.port=8501 --server.address=0.0.0.0 --server.headless=true

echo ""
echo "✅ Container started: $CONTAINER_NAME"
echo ""
echo "========================================="
echo "Installation Complete!"
echo "========================================="
echo ""
echo "Access UI at: http://localhost:${UI_PORT}"
echo ""
echo "Useful commands:"
echo "  # View logs:"
echo "  docker logs -f $CONTAINER_NAME"
echo ""
echo "  # Run CLI command:"
echo "  docker exec -it $CONTAINER_NAME python main.py collect --environment initial --components os"
echo ""
echo "  # Stop container:"
echo "  docker stop $CONTAINER_NAME"
echo ""
echo "  # Start container:"
echo "  docker start $CONTAINER_NAME"
echo ""
echo "⚠️  Next steps:"
echo "  1. Edit config/initial/initial.yaml with your environment details"
echo "  2. Restart container to apply config changes:"
echo "     docker restart $CONTAINER_NAME"
echo ""