#!/bin/bash
# =============================================================================
# Daedalus Market Data Pipeline - Unified Startup Script
# =============================================================================
# This script manages the complete pipeline startup on Raspberry Pi 4:
# 1. Creates/activates virtual environment
# 2. Installs dependencies if needed
# 3. Starts ingestion and sync in the background with proper logging
# 4. Designed to be run on boot via systemd or cron
#
# Usage:
#   ./scripts/run_pipeline.sh              # Start all services
#   ./scripts/run_pipeline.sh --ingestion  # Start ingestion only
#   ./scripts/run_pipeline.sh --sync       # Start sync only
#   ./scripts/run_pipeline.sh --status     # Check status
#   ./scripts/run_pipeline.sh --stop       # Stop all services
#
# For systemd setup, see: docs/PI4_DEPLOYMENT.md
# =============================================================================

set -e  # Exit on error

# =============================================================================
# Configuration
# =============================================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
VENV_DIR="$PROJECT_DIR/venv"
LOG_DIR="$PROJECT_DIR/logs"
PID_DIR="$PROJECT_DIR/.pids"

# Sync configuration
SYNC_INTERVAL=300          # 5 minutes between syncs
SYNC_INCLUDE_RAW=true      # Sync raw data to S3
SYNC_DELETE_AFTER=false    # Keep local files after sync (set true to save disk)

# Resource limits for Pi4 (prevent OOM)
export MALLOC_ARENA_MAX=2  # Limit glibc memory arenas
export PYTHONMALLOC=malloc # Use system malloc

# =============================================================================
# Helper Functions
# =============================================================================

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $1" >&2
}

ensure_dirs() {
    mkdir -p "$LOG_DIR"
    mkdir -p "$PID_DIR"
}

# =============================================================================
# Virtual Environment Setup
# =============================================================================

setup_venv() {
    log "Checking virtual environment..."
    
    if [ ! -d "$VENV_DIR" ]; then
        log "Creating virtual environment at $VENV_DIR..."
        python3 -m venv "$VENV_DIR"
        
        log "Upgrading pip..."
        "$VENV_DIR/bin/pip" install --upgrade pip
        
        log "Installing requirements..."
        "$VENV_DIR/bin/pip" install -r "$PROJECT_DIR/requirements.txt"
        
        log "Virtual environment created successfully!"
    else
        log "Virtual environment exists at $VENV_DIR"
    fi
    
    # Activate venv
    source "$VENV_DIR/bin/activate"
    log "Activated: $(which python)"
}

# =============================================================================
# Process Management
# =============================================================================

get_pid() {
    local name="$1"
    local pid_file="$PID_DIR/${name}.pid"
    if [ -f "$pid_file" ]; then
        cat "$pid_file"
    fi
}

is_running() {
    local name="$1"
    local pid=$(get_pid "$name")
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        return 0
    fi
    return 1
}

save_pid() {
    local name="$1"
    local pid="$2"
    echo "$pid" > "$PID_DIR/${name}.pid"
}

remove_pid() {
    local name="$1"
    rm -f "$PID_DIR/${name}.pid"
}

# =============================================================================
# Service Start Functions
# =============================================================================

start_ingestion() {
    if is_running "ingestion"; then
        log "Ingestion already running (PID: $(get_pid ingestion))"
        return 0
    fi
    
    log "Starting ingestion pipeline..."
    
    cd "$PROJECT_DIR"
    
    # Run with nohup to survive terminal close
    # Use unbuffered output for real-time logging
    nohup python -u scripts/run_ingestion.py \
        --sources ccxt \
        --stats-interval 60 \
        >> "$LOG_DIR/ingestion.log" 2>&1 &
    
    local pid=$!
    save_pid "ingestion" "$pid"
    
    # Wait a moment and verify it started
    sleep 2
    if is_running "ingestion"; then
        log "Ingestion started (PID: $pid)"
        log "Logs: $LOG_DIR/ingestion.log"
    else
        error "Ingestion failed to start! Check $LOG_DIR/ingestion.log"
        return 1
    fi
}

start_sync() {
    if is_running "sync"; then
        log "Sync already running (PID: $(get_pid sync))"
        return 0
    fi
    
    log "Starting storage sync (interval: ${SYNC_INTERVAL}s)..."
    
    cd "$PROJECT_DIR"
    
    # Build sync arguments
    local sync_args="--continuous --interval $SYNC_INTERVAL"
    if [ "$SYNC_INCLUDE_RAW" = true ]; then
        sync_args="$sync_args --include-raw"
    fi
    if [ "$SYNC_DELETE_AFTER" = true ]; then
        sync_args="$sync_args --delete-after-sync"
    fi
    
    nohup python -u scripts/run_sync.py $sync_args \
        >> "$LOG_DIR/sync.log" 2>&1 &
    
    local pid=$!
    save_pid "sync" "$pid"
    
    sleep 2
    if is_running "sync"; then
        log "Sync started (PID: $pid)"
        log "Logs: $LOG_DIR/sync.log"
    else
        error "Sync failed to start! Check $LOG_DIR/sync.log"
        return 1
    fi
}

# =============================================================================
# Service Stop Functions
# =============================================================================

stop_service() {
    local name="$1"
    local pid=$(get_pid "$name")
    
    if [ -z "$pid" ]; then
        log "$name not running (no PID file)"
        return 0
    fi
    
    if ! kill -0 "$pid" 2>/dev/null; then
        log "$name not running (stale PID: $pid)"
        remove_pid "$name"
        return 0
    fi
    
    log "Stopping $name (PID: $pid)..."
    
    # Send SIGTERM for graceful shutdown
    kill -TERM "$pid" 2>/dev/null || true
    
    # Wait up to 30 seconds for graceful shutdown
    local count=0
    while kill -0 "$pid" 2>/dev/null && [ $count -lt 30 ]; do
        sleep 1
        count=$((count + 1))
    done
    
    # Force kill if still running
    if kill -0 "$pid" 2>/dev/null; then
        log "Force killing $name..."
        kill -9 "$pid" 2>/dev/null || true
    fi
    
    remove_pid "$name"
    log "$name stopped"
}

stop_all() {
    log "Stopping all services..."
    stop_service "sync"
    stop_service "ingestion"
    log "All services stopped"
}

# =============================================================================
# Status Check
# =============================================================================

show_status() {
    echo ""
    echo "=== Daedalus Pipeline Status ==="
    echo ""
    
    # Ingestion status
    if is_running "ingestion"; then
        local pid=$(get_pid "ingestion")
        echo "Ingestion: RUNNING (PID: $pid)"
        # Show memory usage
        if command -v ps &> /dev/null; then
            local mem=$(ps -o rss= -p "$pid" 2>/dev/null | awk '{print $1/1024}')
            echo "  Memory: ${mem:-?} MB"
        fi
    else
        echo "Ingestion: STOPPED"
    fi
    
    # Sync status
    if is_running "sync"; then
        local pid=$(get_pid "sync")
        echo "Sync:      RUNNING (PID: $pid)"
        if command -v ps &> /dev/null; then
            local mem=$(ps -o rss= -p "$pid" 2>/dev/null | awk '{print $1/1024}')
            echo "  Memory: ${mem:-?} MB"
        fi
    else
        echo "Sync:      STOPPED"
    fi
    
    echo ""
    
    # System resources
    if command -v free &> /dev/null; then
        echo "=== System Memory ==="
        free -h
        echo ""
    fi
    
    # Disk usage
    echo "=== Data Directory Size ==="
    if [ -d "$PROJECT_DIR/data" ]; then
        du -sh "$PROJECT_DIR/data"/* 2>/dev/null || echo "No data directories"
    fi
    echo ""
    
    # Recent log entries
    echo "=== Recent Ingestion Logs ==="
    if [ -f "$LOG_DIR/ingestion.log" ]; then
        tail -5 "$LOG_DIR/ingestion.log"
    else
        echo "No ingestion log found"
    fi
    echo ""
}

# =============================================================================
# Log Rotation (prevent disk fill)
# =============================================================================

rotate_logs() {
    log "Rotating logs..."
    
    for logfile in "$LOG_DIR"/*.log; do
        if [ -f "$logfile" ]; then
            local size=$(stat -f%z "$logfile" 2>/dev/null || stat -c%s "$logfile" 2>/dev/null)
            # Rotate if > 100MB
            if [ "${size:-0}" -gt 104857600 ]; then
                mv "$logfile" "${logfile}.1"
                log "Rotated: $logfile"
            fi
        fi
    done
}

# =============================================================================
# Main Entry Point
# =============================================================================

main() {
    ensure_dirs
    
    case "${1:-}" in
        --ingestion|-i)
            setup_venv
            start_ingestion
            ;;
        --sync|-s)
            setup_venv
            start_sync
            ;;
        --status)
            show_status
            ;;
        --stop)
            stop_all
            ;;
        --restart)
            stop_all
            sleep 2
            setup_venv
            rotate_logs
            start_ingestion
            # Wait for ingestion to stabilize before starting sync
            sleep 10
            start_sync
            ;;
        --help|-h)
            echo "Usage: $0 [OPTION]"
            echo ""
            echo "Options:"
            echo "  (none)        Start all services (ingestion + sync)"
            echo "  --ingestion   Start ingestion only"
            echo "  --sync        Start sync only"
            echo "  --status      Show status of all services"
            echo "  --stop        Stop all services"
            echo "  --restart     Restart all services"
            echo "  --help        Show this help"
            echo ""
            echo "Environment:"
            echo "  VENV:     $VENV_DIR"
            echo "  LOGS:     $LOG_DIR"
            echo "  PROJECT:  $PROJECT_DIR"
            ;;
        *)
            # Default: start everything
            setup_venv
            rotate_logs
            start_ingestion
            # Wait for ingestion to stabilize before starting sync
            sleep 10
            start_sync
            log ""
            log "Pipeline started! Use '$0 --status' to check status"
            log "Use '$0 --stop' to stop all services"
            ;;
    esac
}

main "$@"
