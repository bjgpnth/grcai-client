#!/bin/bash
# gRCAi Client - Docker Installation Script
# Usage: curl -fsSL https://raw.githubusercontent.com/bjgpnth/grcai-client/main/install-docker.sh | bash

set -e

REPO_URL="https://github.com/bjgpnth/grcai-client"
REPO_BRANCH="dev"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
INSTALL_DIR="/tmp/grcai-client-${TIMESTAMP}"
IMAGE_NAME="grcai/client:${TIMESTAMP}"
CONTAINER_NAME="grcai-client-runtime-${TIMESTAMP}"

# Cleanup function to remove temporary directory
cleanup() {
    if [ -d "$INSTALL_DIR" ]; then
        echo ""
        echo "Cleaning up temporary directory: $INSTALL_DIR"
        rm -rf "$INSTALL_DIR"
    fi
}

# Register cleanup function to run on exit
trap cleanup EXIT

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
echo "Step 1: Downloading client code...from $REPO_URL branch $REPO_BRANCH"
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

# Use defaults from Dockerfile (can be overridden via environment variables)
UI_PORT=${UI_PORT:-8501}
CENTRAL_URL=${GRCAI_CENTRAL_URL:-"https://grcai-central-dev.militva.dev"}

# Only prompt for API key (required and unique per installation)
# Check if API key is already set via environment variable
if [ -n "${OPENAI_API_KEY:-}" ]; then
    API_KEY="${OPENAI_API_KEY}"
    echo "Using OPENAI_API_KEY from environment variable"
else
    # Use /dev/tty if stdin is not a TTY (e.g., when piped from curl)
    if [ -t 0 ]; then
        read -sp "OPENAI_API_KEY (required): " API_KEY
        echo ""
    elif [ -c /dev/tty ]; then
        read -sp "OPENAI_API_KEY (required): " API_KEY < /dev/tty
        echo ""
    else
        echo "❌ Error: Cannot read OPENAI_API_KEY interactively"
        echo "   Please set OPENAI_API_KEY environment variable:"
        echo "   export OPENAI_API_KEY='your-api-key'"
        echo "   Then run the installation script again"
        exit 1
    fi
fi

if [ -z "$API_KEY" ]; then
    echo "❌ Error: OPENAI_API_KEY is required"
    exit 1
fi

echo ""
echo "✅ Configuration collected"
echo "  - UI Port: ${UI_PORT}"
echo "  - Central URL: ${CENTRAL_URL}"
echo ""

# Create config from repo sample if missing (in $HOME/config, mounted as /config in container)
if [ ! -f "${HOME}/config/initial/initial.yaml" ]; then
    echo "Step 3: Creating initial configuration from sample..."
    mkdir -p "${HOME}/config/initial"
    if [ -f "config/initial/initial.yaml" ]; then
        cp config/initial/initial.yaml "${HOME}/config/initial/initial.yaml"
        echo "✅ Config sample copied to: ${HOME}/config/initial/initial.yaml"
    else
        cat > "${HOME}/config/initial/initial.yaml" <<EOF
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
        echo "✅ Minimal config created at: ${HOME}/config/initial/initial.yaml"
    fi
    echo "⚠️  Please edit ${HOME}/config/ with your environment details"
    echo ""
fi
if [ ! -f "${HOME}/config/reasoning_budget.yaml" ] && [ -f "config/reasoning_budget.yaml" ]; then
    cp config/reasoning_budget.yaml "${HOME}/config/reasoning_budget.yaml"
    echo "✅ reasoning_budget.yaml copied to ${HOME}/config/"
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
mkdir -p "${HOME}/grcai_sessions"
mkdir -p "${HOME}/config"

# Run container
echo "Step 5: Starting container..."
echo "Using port: ${UI_PORT}"
docker run -d \
  --name "$CONTAINER_NAME" \
  --restart unless-stopped \
  -p "${UI_PORT}:8501" \
  -e GRCAI_CENTRAL_URL="$CENTRAL_URL" \
  -e OPENAI_API_KEY="$API_KEY" \
  -v "${HOME}/grcai_sessions:/grcai/grcai_sessions" \
  -e GRCAI_SESSIONS_HOME=/grcai/grcai_sessions \
  -v "${HOME}/config:/config" \
  -e GRCAI_CONFIG_HOME=/config \
  -v "${HOME}/.ssh:/home/grcai/.ssh:ro" \
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
echo "  1. Edit configuration files in ${HOME}/config/ with your environment details"
echo "     Templates have been copied from config/template/ if the directory was empty"
echo "     Create environment subdirectories (e.g., ${HOME}/config/initial/initial.yaml) as needed"
echo "  2. For SSH to remote hosts: your ${HOME}/.ssh is mounted so the container uses the same keys."
echo "     In env config you can omit key_path (default ~/.ssh/id_rsa) or set key_path: /home/grcai/.ssh/id_rsa"
echo "  3. Restart container to apply config changes:"
echo "     docker restart $CONTAINER_NAME"
echo ""