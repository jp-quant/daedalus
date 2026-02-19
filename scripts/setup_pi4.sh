#!/bin/bash
# =============================================================================
# Daedalus - Automated Pi4 Setup Script
# =============================================================================
# This script automates the complete setup of Daedalus on Raspberry Pi 4.
# Run this once after cloning the repository.
#
# Usage:
#   chmod +x scripts/setup_pi4.sh
#   ./scripts/setup_pi4.sh
#
# What it does:
#   1. Creates virtual environment and installs dependencies
#   2. Creates required directories
#   3. Installs and enables systemd service
#   4. Configures log rotation
#   5. Sets up swap for memory stability
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
SERVICE_NAME="daedalus"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log() {
    echo -e "${GREEN}[SETUP]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
    exit 1
}

# =============================================================================
# Pre-flight Checks
# =============================================================================

check_requirements() {
    log "Checking requirements..."
    
    # Check if running on Linux (Pi)
    if [[ "$(uname)" != "Linux" ]]; then
        warn "This script is designed for Linux/Raspberry Pi"
        warn "Some features may not work on your system"
    fi
    
    # Check Python
    if ! command -v python3 &> /dev/null; then
        error "Python 3 not found. Install with: sudo apt install python3 python3-venv python3-pip"
    fi
    log "Python: $(python3 --version)"
    
    # Check venv module
    if ! python3 -c "import venv" 2>/dev/null; then
        error "Python venv module not found. Install with: sudo apt install python3-venv"
    fi
}

# =============================================================================
# Virtual Environment Setup
# =============================================================================

setup_venv() {
    log "Setting up virtual environment..."
    
    VENV_DIR="$PROJECT_DIR/venv"
    
    if [ -d "$VENV_DIR" ]; then
        log "Virtual environment already exists"
    else
        log "Creating virtual environment..."
        python3 -m venv "$VENV_DIR"
    fi
    
    log "Activating virtual environment..."
    source "$VENV_DIR/bin/activate"
    
    log "Upgrading pip..."
    pip install --upgrade pip
    
    log "Installing requirements..."
    pip install -r "$PROJECT_DIR/requirements.txt"
    
    log "Virtual environment ready!"
}

# =============================================================================
# Directory Setup
# =============================================================================

setup_directories() {
    log "Creating directories..."
    
    mkdir -p "$PROJECT_DIR/data/raw/active"
    mkdir -p "$PROJECT_DIR/data/raw/ready"
    mkdir -p "$PROJECT_DIR/data/processed"
    mkdir -p "$PROJECT_DIR/logs"
    mkdir -p "$PROJECT_DIR/.pids"
    
    log "Directories created"
}

# =============================================================================
# Systemd Service Setup
# =============================================================================

setup_systemd() {
    log "Setting up systemd service..."
    
    SERVICE_FILE="$SCRIPT_DIR/daedalus.service"
    
    if [ ! -f "$SERVICE_FILE" ]; then
        error "Service file not found: $SERVICE_FILE"
    fi
    
    # Create customized service file
    TEMP_SERVICE="/tmp/daedalus.service"
    sed -e "s|/home/pi/daedalus|$PROJECT_DIR|g" \
        -e "s|User=pi|User=$USER|g" \
        -e "s|Group=pi|Group=$USER|g" \
        "$SERVICE_FILE" > "$TEMP_SERVICE"
    
    # Install service (requires sudo)
    if command -v sudo &> /dev/null; then
        log "Installing systemd service (requires sudo)..."
        sudo cp "$TEMP_SERVICE" /etc/systemd/system/daedalus.service
        sudo systemctl daemon-reload
        
        read -p "Enable service to start on boot? [y/N] " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            sudo systemctl enable daedalus
            log "Service enabled for boot"
        fi
        
        read -p "Start service now? [y/N] " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            sudo systemctl start daedalus
            log "Service started"
            sleep 2
            sudo systemctl status daedalus --no-pager
        fi
    else
        warn "sudo not available - skipping systemd setup"
        warn "Manually copy $TEMP_SERVICE to /etc/systemd/system/"
    fi
    
    rm -f "$TEMP_SERVICE"
}

# =============================================================================
# Log Rotation Setup
# =============================================================================

setup_logrotate() {
    log "Setting up log rotation..."
    
    LOGROTATE_CONF="/tmp/daedalus-logrotate"
    cat > "$LOGROTATE_CONF" << EOF
$PROJECT_DIR/logs/*.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 644 $USER $USER
    dateext
}
EOF
    
    if command -v sudo &> /dev/null && [ -d "/etc/logrotate.d" ]; then
        sudo cp "$LOGROTATE_CONF" /etc/logrotate.d/daedalus
        log "Log rotation configured"
    else
        warn "Could not install logrotate config"
        warn "Manually copy to /etc/logrotate.d/daedalus"
    fi
    
    rm -f "$LOGROTATE_CONF"
}

# =============================================================================
# Swap Configuration (for Pi4 stability)
# =============================================================================

setup_swap() {
    log "Checking swap configuration..."
    
    # Check current swap
    CURRENT_SWAP=$(free -m | grep Swap | awk '{print $2}')
    
    if [ "$CURRENT_SWAP" -lt 1024 ]; then
        warn "Current swap: ${CURRENT_SWAP}MB (recommended: 2GB for Pi4)"
        
        if command -v sudo &> /dev/null && [ -f /etc/dphys-swapfile ]; then
            read -p "Increase swap to 2GB? [y/N] " -n 1 -r
            echo
            if [[ $REPLY =~ ^[Yy]$ ]]; then
                sudo sed -i 's/CONF_SWAPSIZE=.*/CONF_SWAPSIZE=2048/' /etc/dphys-swapfile
                sudo dphys-swapfile setup
                sudo dphys-swapfile swapon
                log "Swap increased to 2GB"
            fi
        else
            warn "Manual swap configuration required"
            echo "  Edit /etc/dphys-swapfile and set CONF_SWAPSIZE=2048"
        fi
    else
        log "Swap: ${CURRENT_SWAP}MB (OK)"
    fi
}

# =============================================================================
# Configuration Check
# =============================================================================

check_config() {
    log "Checking configuration..."
    
    CONFIG_FILE="$PROJECT_DIR/config/config.yaml"
    EXAMPLE_FILE="$PROJECT_DIR/config/config.examples.yaml"
    
    if [ ! -f "$CONFIG_FILE" ]; then
        warn "config.yaml not found!"
        if [ -f "$EXAMPLE_FILE" ]; then
            read -p "Copy from config.examples.yaml? [y/N] " -n 1 -r
            echo
            if [[ $REPLY =~ ^[Yy]$ ]]; then
                cp "$EXAMPLE_FILE" "$CONFIG_FILE"
                log "Config copied - EDIT $CONFIG_FILE with your API keys!"
            fi
        fi
    else
        log "config.yaml found"
    fi
}

# =============================================================================
# Final Summary
# =============================================================================

print_summary() {
    echo ""
    echo "=============================================="
    echo -e "${GREEN}Daedalus Setup Complete!${NC}"
    echo "=============================================="
    echo ""
    echo "Project directory: $PROJECT_DIR"
    echo "Virtual environment: $PROJECT_DIR/venv"
    echo "Logs: $PROJECT_DIR/logs"
    echo ""
    echo "Manual steps:"
    echo "  1. Edit config/config.yaml with your API keys"
    echo "  2. Test: ./scripts/run_pipeline.sh --status"
    echo ""
    echo "Commands:"
    echo "  Start:   ./scripts/run_pipeline.sh"
    echo "  Stop:    ./scripts/run_pipeline.sh --stop"
    echo "  Status:  ./scripts/run_pipeline.sh --status"
    echo ""
    if command -v systemctl &> /dev/null; then
        echo "Systemd commands:"
        echo "  Status:  sudo systemctl status daedalus"
        echo "  Start:   sudo systemctl start daedalus"
        echo "  Stop:    sudo systemctl stop daedalus"
        echo "  Logs:    journalctl -u daedalus -f"
    fi
    echo ""
}

# =============================================================================
# Main
# =============================================================================

main() {
    echo ""
    echo "=============================================="
    echo "Daedalus Pi4 Setup Script"
    echo "=============================================="
    echo ""
    
    check_requirements
    setup_directories
    setup_venv
    check_config
    
    # Only do system-level setup on Linux
    if [[ "$(uname)" == "Linux" ]]; then
        setup_swap
        setup_logrotate
        setup_systemd
    else
        warn "Skipping system setup (not Linux)"
    fi
    
    print_summary
}

main "$@"
