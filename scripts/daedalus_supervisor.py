#!/usr/bin/env python3
"""
Daedalus Supervisor - Unified process manager for ingestion pipeline.

This supervisor:
1. Starts ingestion and sync processes
2. Monitors their health (memory, responsiveness)
3. Auto-restarts crashed processes
4. Performs periodic memory cleanup
5. Logs health metrics
6. Handles graceful shutdown

Usage:
    python scripts/daedalus_supervisor.py
    python scripts/daedalus_supervisor.py --no-sync
    python scripts/daedalus_supervisor.py --dry-run

For systemd integration, install the service:
    sudo cp scripts/daedalus.service /etc/systemd/system/
    sudo systemctl daemon-reload
    sudo systemctl enable daedalus
    sudo systemctl start daedalus
"""
import argparse
import asyncio
import gc
import json
import logging
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, List

# Add parent directory to path for imports
SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_DIR))

# Configure logging early
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger("daedalus.supervisor")


# =============================================================================
# Configuration
# =============================================================================

@dataclass
class SupervisorConfig:
    """Configuration for the supervisor."""
    # Process settings
    enable_ingestion: bool = True
    enable_sync: bool = True
    ingestion_sources: str = "ccxt"
    sync_interval: int = 900  # 15 minutes
    sync_include_raw: bool = True
    sync_no_compact: bool = True  # Raw data already optimally sized
    
    # Health monitoring
    health_check_interval: int = 60  # seconds
    memory_warning_mb: int = 3000  # Warn at 3.0GB
    memory_critical_mb: int = 3500  # Restart at 3.5GB
    max_restart_attempts: int = 5  # Max restarts in restart_window
    restart_window: int = 3600  # 1 hour window for restart counting
    restart_cooldown: int = 30  # seconds between restarts
    
    # Graceful shutdown timeouts (seconds)
    # Ingestion needs more time to: stop collectors, drain queue, flush buffers,
    # close Parquet writers, move files from active/ to ready/
    ingestion_shutdown_timeout: int = 120  # 2 minutes for ingestion
    sync_shutdown_timeout: int = 60  # 1 minute for sync
    
    # Memory management
    gc_interval: int = 300  # Force GC every 5 minutes
    
    # Paths
    project_dir: Path = field(default_factory=lambda: PROJECT_DIR)
    log_dir: Path = field(default_factory=lambda: PROJECT_DIR / "logs")
    pid_dir: Path = field(default_factory=lambda: PROJECT_DIR / ".pids")
    venv_dir: Path = field(default_factory=lambda: PROJECT_DIR / "venv")
    
    def __post_init__(self):
        # Ensure directories exist
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.pid_dir.mkdir(parents=True, exist_ok=True)


@dataclass
class ProcessInfo:
    """Tracks a managed subprocess."""
    name: str
    process: Optional[subprocess.Popen] = None
    start_time: float = 0
    restart_times: List[float] = field(default_factory=list)
    total_restarts: int = 0
    last_memory_mb: float = 0
    
    @property
    def pid(self) -> Optional[int]:
        return self.process.pid if self.process else None
    
    @property
    def is_running(self) -> bool:
        if self.process is None:
            return False
        return self.process.poll() is None
    
    @property
    def uptime_seconds(self) -> float:
        if not self.is_running or self.start_time == 0:
            return 0
        return time.time() - self.start_time
    
    def get_memory_mb(self) -> float:
        """Get process memory usage in MB."""
        if not self.is_running:
            return 0
        try:
            # Cross-platform memory check
            import psutil
            proc = psutil.Process(self.pid)
            mem_info = proc.memory_info()
            return mem_info.rss / (1024 * 1024)
        except ImportError:
            # Fallback for systems without psutil
            try:
                with open(f"/proc/{self.pid}/status") as f:
                    for line in f:
                        if line.startswith("VmRSS:"):
                            return int(line.split()[1]) / 1024
            except:
                pass
        except:
            pass
        return 0
    
    def recent_restarts(self, window_seconds: int) -> int:
        """Count restarts within the time window."""
        cutoff = time.time() - window_seconds
        return sum(1 for t in self.restart_times if t > cutoff)


# =============================================================================
# Supervisor
# =============================================================================

class DaedalusSupervisor:
    """
    Process supervisor for Daedalus ingestion pipeline.
    
    Manages ingestion and sync processes with:
    - Health monitoring
    - Auto-restart on crash
    - Memory monitoring and forced restart on leak
    - Graceful shutdown handling
    """
    
    def __init__(self, config: SupervisorConfig, dry_run: bool = False):
        self.config = config
        self.dry_run = dry_run
        self.processes: Dict[str, ProcessInfo] = {}
        self._shutdown = asyncio.Event()
        self._last_gc = time.time()
        
        # Get Python executable from venv
        if sys.platform == "win32":
            self.python_exe = config.venv_dir / "Scripts" / "python.exe"
        else:
            self.python_exe = config.venv_dir / "bin" / "python"
        
        # Fallback to current interpreter if venv doesn't exist
        if not self.python_exe.exists():
            self.python_exe = Path(sys.executable)
            logger.warning(f"Venv not found, using: {self.python_exe}")
        
        logger.info(f"Supervisor initialized (dry_run={dry_run})")
        logger.info(f"Python: {self.python_exe}")
        logger.info(f"Project: {config.project_dir}")
    
    def _build_ingestion_cmd(self) -> List[str]:
        """Build command for ingestion process."""
        return [
            str(self.python_exe),
            "-u",  # Unbuffered output
            str(self.config.project_dir / "scripts" / "run_ingestion.py"),
            "--sources", self.config.ingestion_sources,
            "--stats-interval", "60",
        ]
    
    def _build_sync_cmd(self) -> List[str]:
        """Build command for sync process."""
        cmd = [
            str(self.python_exe),
            "-u",
            str(self.config.project_dir / "scripts" / "run_sync.py"),
            "--mode", "continuous",
            "--interval", str(self.config.sync_interval),
            "--source", "ccxt",  # Match ingestion source
        ]
        if self.config.sync_include_raw:
            cmd.append("--include-raw")
        if self.config.sync_no_compact:
            cmd.append("--no-compact")
        return cmd
    
    def start_process(self, name: str, cmd: List[str], log_file: Path) -> bool:
        """Start a managed process."""
        if name in self.processes and self.processes[name].is_running:
            logger.warning(f"{name} already running (PID: {self.processes[name].pid})")
            return True
        
        logger.info(f"Starting {name}...")
        logger.debug(f"Command: {' '.join(cmd)}")
        
        if self.dry_run:
            logger.info(f"[DRY RUN] Would start: {' '.join(cmd)}")
            return True
        
        try:
            # Open log file for output
            log_file.parent.mkdir(parents=True, exist_ok=True)
            log_handle = open(log_file, "a", buffering=1)  # Line buffered
            
            # Write startup marker
            log_handle.write(f"\n{'='*60}\n")
            log_handle.write(f"[SUPERVISOR] Starting {name} at {datetime.now(timezone.utc).isoformat()}\n")
            log_handle.write(f"[SUPERVISOR] Command: {' '.join(cmd)}\n")
            log_handle.write(f"{'='*60}\n\n")
            log_handle.flush()
            
            # Start process
            env = os.environ.copy()
            env["PYTHONUNBUFFERED"] = "1"
            env["MALLOC_ARENA_MAX"] = "2"  # Limit glibc memory arenas
            
            process = subprocess.Popen(
                cmd,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                cwd=str(self.config.project_dir),
                env=env,
                start_new_session=True,  # Prevent SIGINT propagation
            )
            
            # Track process
            if name not in self.processes:
                self.processes[name] = ProcessInfo(name=name)
            
            info = self.processes[name]
            info.process = process
            info.start_time = time.time()
            
            # Save PID file
            pid_file = self.config.pid_dir / f"{name}.pid"
            pid_file.write_text(str(process.pid))
            
            # Wait a moment to verify startup
            time.sleep(2)
            
            if info.is_running:
                logger.info(f"✓ {name} started (PID: {process.pid})")
                return True
            else:
                logger.error(f"✗ {name} failed to start - check {log_file}")
                return False
            
        except Exception as e:
            logger.error(f"Failed to start {name}: {e}")
            return False
    
    def stop_process(self, name: str, timeout: Optional[int] = None) -> bool:
        """
        Stop a managed process gracefully.
        
        Args:
            name: Process name ('ingestion' or 'sync')
            timeout: Override shutdown timeout. If None, uses per-process defaults:
                     - ingestion: 120s (needs time to flush buffers, close writers)
                     - sync: 60s
        """
        if name not in self.processes:
            return True
        
        info = self.processes[name]
        if not info.is_running:
            logger.info(f"{name} not running")
            return True
        
        # Use per-process timeout if not specified
        if timeout is None:
            if name == "ingestion":
                timeout = self.config.ingestion_shutdown_timeout
            elif name == "sync":
                timeout = self.config.sync_shutdown_timeout
            else:
                timeout = 60  # default fallback
        
        pid = info.pid
        logger.info(f"Stopping {name} (PID: {pid}, timeout={timeout}s)...")
        
        if self.dry_run:
            logger.info(f"[DRY RUN] Would stop PID {pid}")
            return True
        
        try:
            # Send SIGTERM for graceful shutdown
            info.process.terminate()
            
            # Wait for graceful shutdown with progress logging
            start_time = time.time()
            try:
                # Check every 10 seconds and log progress
                while True:
                    try:
                        info.process.wait(timeout=10)
                        # Process exited
                        elapsed = time.time() - start_time
                        logger.info(f"✓ {name} stopped gracefully in {elapsed:.1f}s")
                        break
                    except subprocess.TimeoutExpired:
                        elapsed = time.time() - start_time
                        if elapsed >= timeout:
                            raise subprocess.TimeoutExpired(cmd=name, timeout=timeout)
                        logger.info(f"  Waiting for {name} to stop... ({elapsed:.0f}s/{timeout}s)")
            except subprocess.TimeoutExpired:
                logger.warning(f"{name} didn't stop gracefully after {timeout}s, killing...")
                info.process.kill()
                info.process.wait(timeout=5)
            
            # Clean up PID file
            pid_file = self.config.pid_dir / f"{name}.pid"
            if pid_file.exists():
                pid_file.unlink()
            
            return True
            
        except Exception as e:
            logger.error(f"Error stopping {name}: {e}")
            return False
    
    def restart_process(self, name: str) -> bool:
        """Restart a process with cooldown and rate limiting."""
        info = self.processes.get(name)
        
        if info:
            # Check restart rate limit
            recent = info.recent_restarts(self.config.restart_window)
            if recent >= self.config.max_restart_attempts:
                logger.error(
                    f"🚨 {name} has restarted {recent} times in the last "
                    f"{self.config.restart_window}s - NOT restarting to prevent crash loop"
                )
                return False
            
            # Record restart
            info.restart_times.append(time.time())
            info.total_restarts += 1
        
        # Stop existing process
        self.stop_process(name)
        
        # Cooldown
        logger.info(f"Waiting {self.config.restart_cooldown}s before restart...")
        time.sleep(self.config.restart_cooldown)
        
        # Start based on process type
        if name == "ingestion":
            return self.start_process(
                "ingestion",
                self._build_ingestion_cmd(),
                self.config.log_dir / "ingestion.log"
            )
        elif name == "sync":
            return self.start_process(
                "sync",
                self._build_sync_cmd(),
                self.config.log_dir / "sync.log"
            )
        
        return False
    
    def check_process_health(self, name: str) -> bool:
        """
        Check if a process is healthy.
        
        Returns True if healthy, False if needs restart.
        """
        info = self.processes.get(name)
        if not info:
            return False
        
        # Check if running
        if not info.is_running:
            logger.warning(f"⚠️  {name} is not running!")
            return False
        
        # Check memory usage
        mem_mb = info.get_memory_mb()
        info.last_memory_mb = mem_mb
        
        if mem_mb > self.config.memory_critical_mb:
            logger.error(
                f"🚨 {name} memory critical: {mem_mb:.1f}MB > {self.config.memory_critical_mb}MB - forcing restart"
            )
            return False
        elif mem_mb > self.config.memory_warning_mb:
            logger.warning(
                f"⚠️  {name} memory high: {mem_mb:.1f}MB > {self.config.memory_warning_mb}MB"
            )
        
        return True
    
    def log_status(self):
        """Log current status of all processes."""
        logger.info("=" * 60)
        logger.info("Daedalus Supervisor Status")
        logger.info("=" * 60)
        
        for name, info in self.processes.items():
            if info.is_running:
                mem = info.last_memory_mb or info.get_memory_mb()
                uptime = info.uptime_seconds
                uptime_str = f"{uptime/3600:.1f}h" if uptime > 3600 else f"{uptime/60:.1f}m"
                logger.info(
                    f"✓ {name}: PID={info.pid}, mem={mem:.1f}MB, "
                    f"uptime={uptime_str}, restarts={info.total_restarts}"
                )
            else:
                logger.info(f"✗ {name}: STOPPED (restarts={info.total_restarts})")
        
        # System memory (if psutil available)
        try:
            import psutil
            mem = psutil.virtual_memory()
            swap = psutil.swap_memory()
            logger.info(
                f"System: RAM={mem.used/1024/1024:.0f}MB/{mem.total/1024/1024:.0f}MB "
                f"({mem.percent}%), Swap={swap.used/1024/1024:.0f}MB/{swap.total/1024/1024:.0f}MB"
            )
        except ImportError:
            pass
        
        logger.info("=" * 60)
    
    async def run(self):
        """Main supervisor loop."""
        logger.info("Starting Daedalus Supervisor...")
        
        # Start processes
        if self.config.enable_ingestion:
            self.start_process(
                "ingestion",
                self._build_ingestion_cmd(),
                self.config.log_dir / "ingestion.log"
            )
            # Wait for ingestion to stabilize before starting sync
            await asyncio.sleep(10)
        
        if self.config.enable_sync:
            self.start_process(
                "sync",
                self._build_sync_cmd(),
                self.config.log_dir / "sync.log"
            )
        
        # Log initial status
        self.log_status()
        
        # Main monitoring loop
        last_status_log = time.time()
        
        while not self._shutdown.is_set():
            try:
                # Health checks
                for name, info in list(self.processes.items()):
                    if not self.check_process_health(name):
                        logger.warning(f"Restarting unhealthy process: {name}")
                        self.restart_process(name)
                
                # Periodic status logging (every 10 minutes)
                if time.time() - last_status_log > 600:
                    self.log_status()
                    last_status_log = time.time()
                
                # Periodic GC (supervisor itself)
                if time.time() - self._last_gc > self.config.gc_interval:
                    gc.collect()
                    self._last_gc = time.time()
                
                # Wait for next check or shutdown
                try:
                    await asyncio.wait_for(
                        self._shutdown.wait(),
                        timeout=self.config.health_check_interval
                    )
                except asyncio.TimeoutError:
                    pass  # Normal timeout, continue loop
                
            except Exception as e:
                logger.error(f"Error in supervisor loop: {e}", exc_info=True)
                await asyncio.sleep(10)
        
        # Shutdown
        logger.info("Shutting down supervisor...")
        self.log_status()
        
        for name in list(self.processes.keys()):
            self.stop_process(name)
        
        logger.info("Supervisor stopped")
    
    def request_shutdown(self):
        """Signal the supervisor to shut down."""
        logger.info("Shutdown requested")
        self._shutdown.set()


# =============================================================================
# Entry Point
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Daedalus Supervisor - Process manager for ingestion pipeline"
    )
    parser.add_argument(
        "--no-ingestion",
        action="store_true",
        help="Don't start ingestion process"
    )
    parser.add_argument(
        "--no-sync",
        action="store_true",
        help="Don't start sync process"
    )
    parser.add_argument(
        "--sources",
        type=str,
        default="ccxt",
        help="Ingestion sources (default: ccxt)"
    )
    parser.add_argument(
        "--sync-interval",
        type=int,
        default=900,
        help="Sync interval in seconds (default: 900)"
    )
    parser.add_argument(
        "--memory-limit-mb",
        type=int,
        default=3600,
        help="Memory limit in MB before forced restart (default: 3600)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't actually start processes, just log what would happen"
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show status and exit"
    )
    
    args = parser.parse_args()
    
    # Configure based on args
    config = SupervisorConfig(
        enable_ingestion=not args.no_ingestion,
        enable_sync=not args.no_sync,
        ingestion_sources=args.sources,
        sync_interval=args.sync_interval,
        memory_critical_mb=args.memory_limit_mb,
    )
    
    # Status check mode
    if args.status:
        supervisor = DaedalusSupervisor(config, dry_run=True)
        # Check for running processes from PID files
        for name in ["ingestion", "sync"]:
            pid_file = config.pid_dir / f"{name}.pid"
            if pid_file.exists():
                try:
                    pid = int(pid_file.read_text().strip())
                    # Check if process is running
                    try:
                        os.kill(pid, 0)
                        print(f"{name}: RUNNING (PID: {pid})")
                    except OSError:
                        print(f"{name}: STOPPED (stale PID file)")
                except:
                    print(f"{name}: UNKNOWN")
            else:
                print(f"{name}: STOPPED (no PID file)")
        return
    
    # Create supervisor
    supervisor = DaedalusSupervisor(config, dry_run=args.dry_run)
    
    # Setup signal handlers
    def signal_handler(sig, frame):
        supervisor.request_shutdown()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Run supervisor
    try:
        asyncio.run(supervisor.run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
