# Raspberry Pi 4 Deployment Guide

Complete guide for deploying Daedalus Market Data Pipeline on Raspberry Pi 4 for 24/7 operation.

## Quick Start (One Command)

```bash
# Clone repo (if not done)
cd ~
git clone https://github.com/your-repo/market-data-pipeline.git
cd market-data-pipeline

# Run automated setup
chmod +x scripts/setup_pi4.sh
./scripts/setup_pi4.sh
```

This will:
1. Create virtual environment
2. Install dependencies
3. Set up directories
4. Configure systemd service
5. Set up log rotation
6. Configure swap (recommended 2GB)

---

## Manual Setup

### 1. Virtual Environment

```bash
cd ~/market-data-pipeline

# Create venv
python3 -m venv venv

# Activate
source venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt
```

### 2. Configuration

```bash
# Copy example config
cp config/config.examples.yaml config/config.yaml

# Edit with your API keys
nano config/config.yaml
```

### 3. Test Run

```bash
# Activate venv
source venv/bin/activate

# Test ingestion (Ctrl+C to stop)
python scripts/run_ingestion.py --stats-interval 30

# Test sync
python scripts/run_sync.py --dry-run --include-raw
```

---

## Running as a Service

### Option 1: Using daedalus_supervisor.py (Recommended)

The Python supervisor provides:
- **Health monitoring** - Checks process memory and responsiveness
- **Auto-restart** - Restarts crashed processes automatically  
- **Memory leak protection** - Force-restarts processes exceeding memory limits
- **Rate limiting** - Prevents restart loops
- **Unified logging** - All output to logs/ directory

```bash
# Activate venv
source venv/bin/activate

# Start supervisor (foreground for testing)
python scripts/daedalus_supervisor.py

# Start with options
python scripts/daedalus_supervisor.py --no-sync        # Skip sync
python scripts/daedalus_supervisor.py --memory-limit-mb 1500  # Custom limit
python scripts/daedalus_supervisor.py --dry-run        # Preview without starting

# Check status of running processes
python scripts/daedalus_supervisor.py --status
```

### Option 2: Using run_pipeline.sh (Legacy)

```bash
# Make executable
chmod +x scripts/run_pipeline.sh

# Start all services
./scripts/run_pipeline.sh

# Check status
./scripts/run_pipeline.sh --status

# Stop all services
./scripts/run_pipeline.sh --stop

# Restart
./scripts/run_pipeline.sh --restart
```

### Option 3: Using systemd (Auto-start on Boot - RECOMMENDED)

```bash
# Install service
sudo cp scripts/daedalus.service /etc/systemd/system/

# Edit paths to match your installation
sudo nano /etc/systemd/system/daedalus.service

# Reload systemd
sudo systemctl daemon-reload

# Enable on boot
sudo systemctl enable daedalus

# Start now
sudo systemctl start daedalus

# Check status
sudo systemctl status daedalus

# View logs
journalctl -u daedalus -f

# View process-specific logs
tail -f ~/market-data-pipeline/logs/ingestion.log
tail -f ~/market-data-pipeline/logs/sync.log
```

---

## Memory & Resource Management

### Why Processes Die After a Week

Common causes on Pi4:

1. **Memory Leaks**: Dictionary accumulation over time (now fixed)
2. **OOM Killer**: Linux kills processes when memory/swap exhausted
3. **SSH Disconnect**: Running in SSH terminal without supervision
4. **Swap Exhaustion**: Insufficient swap space for long-running processes

### Solutions Implemented

#### 0. Automatic Memory Management (NEW)

The supervisor and writer now include automatic memory management:

**Supervisor (`daedalus_supervisor.py`):**
- Monitors process memory every 60 seconds
- Warns at 1.2GB, force-restarts at 1.8GB
- Tracks restart rate to prevent crash loops
- Runs GC periodically on itself

**Writer (`StreamingParquetWriter`):**
- **Periodic cleanup** every 5 minutes:
  - Removes stale `hour_counters` entries (older than 2 hours)
  - Cleans empty partition buffers
  - Forces garbage collection
- **Stats tracking** for memory diagnostics:
  - `memory_cleanups`, `gc_collections`
  - `stale_counters_cleaned`, `stale_buffers_cleaned`

Check memory stats in logs:
```
[StreamingParquetWriter] Memory cleanup: counters=12, buffers=8, segments=6, gc_collected=142
```

#### 1. Memory Optimization in Writer

The `StreamingParquetWriter` is designed for low memory usage:

```
┌─────────────────────────────────────────────────────────────────────┐
│                    MEMORY FLOW DIAGRAM                               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  WebSocket    ──►  AsyncQueue    ──►  Per-Partition   ──►  PyArrow  │
│  Message          (max 10,000)       Buffers (deque)      Table     │
│  ~500 bytes       ~5MB max           ~40KB each           ~40KB     │
│                                                                      │
│                                        │                             │
│                                        ▼                             │
│                                   Parquet Writer                     │
│                                   (streaming, not buffered)          │
│                                        │                             │
│                                        ▼                             │
│                                   active/*.parquet                   │
│                                   (on disk immediately)              │
│                                        │                             │
│                                        ▼ (on rotation/shutdown)      │
│                                   ready/*.parquet                    │
│                                   (ready for ETL/sync)               │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘

MEMORY USAGE SUMMARY:
- Queue: ~5 MB max (10,000 records × 500 bytes)
- Buffers: ~1 MB max (20 partitions × 100 records × 500 bytes)
- PyArrow: ~100 KB temporary during conversion
- TOTAL: < 10 MB per writer
```

#### 2. Flush Triggers

Data is written to disk when ANY of these occur:
- **Time**: Every 5 seconds
- **Buffer**: Any partition buffer reaches 100 records
- **Rotation**: Segment file reaches 100 MB

#### 3. File Lifecycle

```
active/                          ready/
  │                                │
  │ 1. Parquet writer streams      │
  │    data directly to file       │
  │                                │
  │ 2. File stays in active/       │
  │    while being written         │
  │                                │
  │ 3. On rotation (100MB or       │
  │    hour change), file is       │
  │    MOVED (not copied) ─────────┼──► ready/
  │                                │
  │ 4. Files in ready/ are         │
  │    NEVER touched again by      │
  │    ingestion (safe for ETL)    │
  │                                │
```

#### 4. Swap Configuration

```bash
# Check current swap
free -h

# Pi4 recommended: 2GB swap
sudo nano /etc/dphys-swapfile
# Set: CONF_SWAPSIZE=2048

# Apply
sudo dphys-swapfile setup
sudo dphys-swapfile swapon
```

#### 5. Resource Limits (systemd)

The service file includes:
```ini
MemoryMax=2G       # Hard limit
MemoryHigh=1.5G    # Soft limit (triggers memory pressure)
```

---

## Troubleshooting

### Orphan Files in active/

**Cause**: Process killed without graceful shutdown

**Solution**: Already implemented! On startup, `StreamingParquetWriter.start()` now migrates orphan files:

```python
async def start(self):
    # Migrate orphan files from previous run
    await asyncio.get_event_loop().run_in_executor(
        None, self._move_active_to_ready
    )
```

Just restart the service and orphans are automatically recovered.

### Check Memory Usage

```bash
# Per-process memory
ps aux --sort=-%mem | head -20

# System memory
free -h

# Check for OOM kills
dmesg | grep -i "killed process"
journalctl -k | grep -i "out of memory"
```

### Check Logs

```bash
# Ingestion logs
tail -f ~/market-data-pipeline/logs/ingestion.log

# Sync logs
tail -f ~/market-data-pipeline/logs/sync.log

# Systemd logs
journalctl -u daedalus -f

# System logs (OOM, etc.)
journalctl -k -f
```

### Process Monitoring

```bash
# Install htop for better monitoring
sudo apt install htop

# Run with process tree
htop -t

# Watch memory specifically
watch -n 5 'free -h; echo; ps aux --sort=-%mem | head -10'
```

---

## Recommended Pi4 Settings

### /boot/config.txt

```ini
# Reduce GPU memory (headless server)
gpu_mem=16

# Enable 64-bit kernel (better memory management)
arm_64bit=1
```

### /etc/sysctl.conf

```ini
# Reduce swappiness (prefer RAM)
vm.swappiness=10

# Allow more open files
fs.file-max=65535
```

Apply: `sudo sysctl -p`

---

## Complete Deployment Checklist

- [ ] Clone repository
- [ ] Run `./scripts/setup_pi4.sh`
- [ ] Edit `config/config.yaml` with API keys
- [ ] Test: `./scripts/run_pipeline.sh --status`
- [ ] Configure swap (2GB recommended)
- [ ] Enable systemd service
- [ ] Verify logs are rotating
- [ ] Set up monitoring/alerts (optional)

---

## Architecture Summary

```
┌─────────────────────────────────────────────────────────────────┐
│                      RASPBERRY PI 4                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────┐     ┌─────────────────┐                    │
│  │ run_ingestion   │     │ run_sync        │                    │
│  │ (background)    │     │ (background)    │                    │
│  └────────┬────────┘     └────────┬────────┘                    │
│           │                       │                             │
│           ▼                       ▼                             │
│  ┌─────────────────────────────────────────┐                    │
│  │           LOCAL STORAGE                 │                    │
│  │                                         │                    │
│  │  data/raw/active/   ──►  data/raw/ready/ ──► S3 (via sync)  │
│  │  (being written)        (ready for ETL)                      │
│  │                                         │                    │
│  └─────────────────────────────────────────┘                    │
│                                                                 │
│  ┌─────────────────┐                                            │
│  │ systemd         │  Manages start/stop/restart                │
│  │ (daedalus.service)                                           │
│  └─────────────────┘                                            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Support

If processes keep dying:

1. Check `dmesg | grep -i oom` for OOM kills
2. Increase swap to 4GB
3. Reduce `queue_maxsize` in config (default 10000 → try 5000)
4. Reduce `batch_size` (default 100 → try 50)
5. Reduce number of symbols being collected
