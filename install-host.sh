#!/bin/bash
# gRCAi Client - Host Installation Script
# Usage: curl -fsSL https://raw.githubusercontent.com/yourorg/grcai-client/main/install-host.sh | bash

set -e

REPO_URL="https://github.com/yourorg/grcai-client"
REPO_BRANCH="main"
INSTALL_DIR="${PWD}/grcai-client"
VENV_DIR="${INSTALL_DIR}/venv"

echo "========================================="
echo "gRCAi Client - Host Installation"
echo "========================================="
echo ""

# Check Python availability
if ! command -v python3 &> /dev/null; then
    echo "❌ Error: python3 is not installed or not in PATH"
    echo "Please install Python 3.12+ first"
    exit 1
fi

PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
PYTHON_MAJOR=$(echo "$PYTHON_VERSION" | cut -d'.' -f1)
PYTHON_MINOR=$(echo "$PYTHON_VERSION" | cut -d'.' -f2)

if [ "$PYTHON_MAJOR" -lt 3 ] || ([ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -lt 12 ]); then
    echo "⚠️  Warning: Python 3.12+ recommended. Found: $PYTHON_VERSION"
    read -p "Continue anyway? (y/N): " CONTINUE
    if [[ ! "$CONTINUE" =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

echo "✅ Python found: $PYTHON_VERSION"
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

# Create virtual environment
echo "Step 3: Creating virtual environment..."
python3 -m venv "$VENV_DIR"
echo "✅ Virtual environment created: $VENV_DIR"
echo ""

# Activate venv and install dependencies
echo "Step 4: Installing dependencies..."
source "$VENV_DIR/bin/activate"
pip install --upgrade pip
pip install -r requirements.txt
echo "✅ Dependencies installed"
echo ""

# Create directories
mkdir -p grcai_sessions

# Create config template if missing
if [ ! -f "config/initial/initial.yaml" ]; then
    echo "Step 5: Creating initial configuration template..."
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

# Create .env file for environment variables
echo "Step 6: Creating .env file..."
cat > .env <<EOF
GRCAI_CENTRAL_URL=$CENTRAL_URL
GRCAI_REASONING_MODE=remote
OPENAI_API_KEY=$API_KEY
STREAMLIT_SERVER_PORT=$UI_PORT
STREAMLIT_SERVER_ADDRESS=0.0.0.0
STREAMLIT_SERVER_HEADLESS=true
STREAMLIT_BROWSER_GATHER_USAGE_STATS=false
EOF
echo "✅ Environment file created: .env"
echo ""

# Create run script
echo "Step 7: Creating run script..."
cat > run-ui.sh <<'RUN_EOF'
#!/bin/bash
# Run gRCAi Client UI
cd "$(dirname "$0")"
source venv/bin/activate
source .env
streamlit run ui/app.py --server.port=${STREAMLIT_SERVER_PORT} --server.address=${STREAMLIT_SERVER_ADDRESS} --server.headless=true
RUN_EOF

chmod +x run-ui.sh
echo "✅ Run script created: run-ui.sh"
echo ""

# Create systemd service file (optional)
read -p "Create systemd service for auto-start? (y/N): " CREATE_SERVICE
if [[ "$CREATE_SERVICE" =~ ^[Yy]$ ]]; then
    SERVICE_FILE="/etc/systemd/system/grcai-client.service"
    echo ""
    echo "Creating systemd service..."
    sudo tee "$SERVICE_FILE" > /dev/null <<SERVICE_EOF
[Unit]
Description=gRCAi Client Runtime
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$INSTALL_DIR
Environment="PATH=$VENV_DIR/bin"
EnvironmentFile=$INSTALL_DIR/.env
ExecStart=$VENV_DIR/bin/streamlit run $INSTALL_DIR/ui/app.py --server.port=\${STREAMLIT_SERVER_PORT} --server.address=\${STREAMLIT_SERVER_ADDRESS} --server.headless=true
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
SERVICE_EOF

    sudo systemctl daemon-reload
    sudo systemctl enable grcai-client
    echo "✅ Systemd service created and enabled"
    echo "  Start: sudo systemctl start grcai-client"
    echo "  Status: sudo systemctl status grcai-client"
    echo "  Logs: sudo journalctl -u grcai-client -f"
    echo ""
fi

echo ""
echo "========================================="
echo "Installation Complete!"
echo "========================================="
echo ""
echo "Installation directory: $INSTALL_DIR"
echo ""
echo "To run the UI:"
echo "  cd $INSTALL_DIR"
echo "  ./run-ui.sh"
echo ""
echo "Or manually:"
echo "  cd $INSTALL_DIR"
echo "  source venv/bin/activate"
echo "  source .env"
echo "  streamlit run ui/app.py --server.port=$UI_PORT --server.address=0.0.0.0"
echo ""
echo "To run CLI:"
echo "  cd $INSTALL_DIR"
echo "  source venv/bin/activate"
echo "  source .env"
echo "  python main.py collect --environment initial --components os"
echo ""
echo "⚠️  Next steps:"
echo "  1. Edit config/initial/initial.yaml with your environment details"
echo "  2. Update .env file if you need to change configuration"
echo ""