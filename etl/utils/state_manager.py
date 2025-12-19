"""
State Management for ETL Pipelines
==================================

Provides automatic state persistence and recovery for stateful ETL transforms.
Supports graceful shutdown handling and checkpoint/resume functionality.

Key Features:
- Automatic state saving on graceful shutdown (SIGINT, SIGTERM)
- Periodic checkpointing at configurable intervals
- State file rotation to prevent unbounded growth
- Thread-safe state operations
- Cross-platform signal handling

Usage:
    from etl.utils.state_manager import StateManager
    
    manager = StateManager(
        state_dir=Path("data/temp/state"),
        checkpoint_interval=60,
        max_state_files=5,
    )
    
    # Register a transform for state management
    manager.register_transform("orderbook_features", transform)
    
    # Start background checkpointing
    manager.start()
    
    # On shutdown (automatic via signal handlers)
    # Or manual:
    manager.save_all_states()
"""

from __future__ import annotations

import atexit
import json
import logging
import signal
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Protocol

if TYPE_CHECKING:
    from config.config import DaedalusConfig

logger = logging.getLogger(__name__)


class StatefulTransformProtocol(Protocol):
    """Protocol for transforms that support state save/load."""
    
    def get_state(self) -> Dict[str, Any]:
        """Get current state as a dictionary."""
        ...
    
    def set_state(self, state: Dict[str, Any]) -> None:
        """Restore state from a dictionary."""
        ...


class StateManager:
    """
    Manages state persistence for stateful ETL transforms.
    
    Features:
    - Automatic state saving on shutdown (SIGINT, SIGTERM)
    - Periodic checkpoint saving
    - State file rotation
    - Thread-safe operations
    """
    
    def __init__(
        self,
        state_dir: Path,
        checkpoint_interval: int = 60,
        max_state_files: int = 5,
        auto_save_on_shutdown: bool = True,
    ):
        """
        Initialize state manager.
        
        Args:
            state_dir: Directory for state files
            checkpoint_interval: Seconds between checkpoints (0 to disable)
            max_state_files: Max checkpoint files to keep per transform
            auto_save_on_shutdown: Register signal handlers for graceful shutdown
        """
        self.state_dir = Path(state_dir)
        self.checkpoint_interval = checkpoint_interval
        self.max_state_files = max_state_files
        self.auto_save_on_shutdown = auto_save_on_shutdown
        
        # Registered transforms
        self._transforms: Dict[str, StatefulTransformProtocol] = {}
        self._lock = threading.Lock()
        
        # Background checkpoint thread
        self._checkpoint_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        
        # Ensure state directory exists
        self.state_dir.mkdir(parents=True, exist_ok=True)
        
        # Register shutdown handlers
        if auto_save_on_shutdown:
            self._register_shutdown_handlers()
    
    def _register_shutdown_handlers(self):
        """Register signal handlers for graceful shutdown."""
        # Register atexit handler
        atexit.register(self._shutdown_handler)
        
        # Register signal handlers (cross-platform)
        try:
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
        except (ValueError, OSError) as e:
            # Can't set signal handlers in non-main thread
            logger.debug(f"Could not register signal handlers: {e}")
    
    def _signal_handler(self, signum: int, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, saving state...")
        self.save_all_states()
        self.stop()
    
    def _shutdown_handler(self):
        """Handle process exit."""
        logger.info("Process exiting, saving state...")
        self.save_all_states()
    
    def register_transform(
        self,
        name: str,
        transform: StatefulTransformProtocol,
    ) -> None:
        """
        Register a stateful transform for state management.
        
        Args:
            name: Unique name for this transform instance
            transform: Transform object with get_state/set_state methods
        """
        with self._lock:
            self._transforms[name] = transform
            logger.debug(f"Registered transform: {name}")
    
    def unregister_transform(self, name: str) -> None:
        """Unregister a transform."""
        with self._lock:
            if name in self._transforms:
                del self._transforms[name]
                logger.debug(f"Unregistered transform: {name}")
    
    def save_state(self, name: str) -> Optional[Path]:
        """
        Save state for a specific transform.
        
        Args:
            name: Transform name
        
        Returns:
            Path to saved state file, or None if failed
        """
        with self._lock:
            if name not in self._transforms:
                logger.warning(f"Transform not found: {name}")
                return None
            
            transform = self._transforms[name]
        
        try:
            state = transform.get_state()
            
            # Add metadata
            state_data = {
                "transform_name": name,
                "saved_at": datetime.utcnow().isoformat(),
                "state": state,
            }
            
            # Create timestamped filename
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            state_file = self.state_dir / f"{name}_{timestamp}.json"
            
            # Write state
            with open(state_file, 'w') as f:
                json.dump(state_data, f, indent=2, default=str)
            
            logger.info(f"Saved state for {name}: {state_file}")
            
            # Rotate old state files
            self._rotate_state_files(name)
            
            return state_file
            
        except Exception as e:
            logger.error(f"Failed to save state for {name}: {e}")
            return None
    
    def save_all_states(self) -> Dict[str, Optional[Path]]:
        """
        Save state for all registered transforms.
        
        Returns:
            Dict mapping transform name to saved file path (or None if failed)
        """
        results = {}
        with self._lock:
            names = list(self._transforms.keys())
        
        for name in names:
            results[name] = self.save_state(name)
        
        return results
    
    def load_state(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Load the most recent state for a transform.
        
        Args:
            name: Transform name
        
        Returns:
            State dictionary, or None if not found
        """
        state_files = sorted(
            self.state_dir.glob(f"{name}_*.json"),
            reverse=True,  # Most recent first
        )
        
        if not state_files:
            logger.debug(f"No state files found for {name}")
            return None
        
        latest_file = state_files[0]
        
        try:
            with open(latest_file, 'r') as f:
                state_data = json.load(f)
            
            logger.info(f"Loaded state for {name} from {latest_file}")
            return state_data.get("state", {})
            
        except Exception as e:
            logger.error(f"Failed to load state for {name}: {e}")
            return None
    
    def restore_transform_state(self, name: str) -> bool:
        """
        Load and apply state to a registered transform.
        
        Args:
            name: Transform name
        
        Returns:
            True if state was restored, False otherwise
        """
        with self._lock:
            if name not in self._transforms:
                logger.warning(f"Transform not found: {name}")
                return False
            transform = self._transforms[name]
        
        state = self.load_state(name)
        if state is None:
            return False
        
        try:
            transform.set_state(state)
            logger.info(f"Restored state for {name}")
            return True
        except Exception as e:
            logger.error(f"Failed to restore state for {name}: {e}")
            return False
    
    def _rotate_state_files(self, name: str) -> None:
        """Remove old state files beyond max_state_files."""
        state_files = sorted(
            self.state_dir.glob(f"{name}_*.json"),
            reverse=True,
        )
        
        # Keep only max_state_files
        for old_file in state_files[self.max_state_files:]:
            try:
                old_file.unlink()
                logger.debug(f"Removed old state file: {old_file}")
            except Exception as e:
                logger.warning(f"Failed to remove old state file {old_file}: {e}")
    
    def start(self) -> None:
        """Start background checkpoint thread."""
        if self.checkpoint_interval <= 0:
            logger.debug("Checkpointing disabled (interval <= 0)")
            return
        
        if self._checkpoint_thread is not None and self._checkpoint_thread.is_alive():
            logger.warning("Checkpoint thread already running")
            return
        
        self._stop_event.clear()
        self._checkpoint_thread = threading.Thread(
            target=self._checkpoint_loop,
            daemon=True,
            name="StateManager-Checkpoint",
        )
        self._checkpoint_thread.start()
        logger.info(f"Started checkpoint thread (interval={self.checkpoint_interval}s)")
    
    def stop(self) -> None:
        """Stop background checkpoint thread."""
        self._stop_event.set()
        if self._checkpoint_thread is not None:
            self._checkpoint_thread.join(timeout=5.0)
            self._checkpoint_thread = None
        logger.debug("Stopped checkpoint thread")
    
    def _checkpoint_loop(self) -> None:
        """Background loop for periodic checkpoints."""
        while not self._stop_event.is_set():
            # Wait for interval or stop signal
            if self._stop_event.wait(timeout=self.checkpoint_interval):
                break  # Stop signal received
            
            # Save all states
            logger.debug("Running periodic checkpoint...")
            self.save_all_states()
    
    def get_state_info(self) -> Dict[str, Any]:
        """
        Get information about current state management.
        
        Returns:
            Dict with state info for each transform
        """
        info = {
            "state_dir": str(self.state_dir),
            "checkpoint_interval": self.checkpoint_interval,
            "transforms": {},
        }
        
        with self._lock:
            for name in self._transforms:
                state_files = sorted(
                    self.state_dir.glob(f"{name}_*.json"),
                    reverse=True,
                )
                info["transforms"][name] = {
                    "registered": True,
                    "state_files": len(state_files),
                    "latest": str(state_files[0]) if state_files else None,
                }
        
        return info


def create_state_manager_from_config(config: "DaedalusConfig") -> StateManager:
    """
    Create a StateManager from Daedalus configuration.
    
    Args:
        config: DaedalusConfig instance
    
    Returns:
        Configured StateManager instance
    """
    from config.config import DaedalusConfig  # Avoid circular import
    
    state_config = config.etl.state
    storage_config = config.storage.etl_storage_input
    
    # Determine base path
    if storage_config.backend == "local":
        base_path = Path(storage_config.base_dir)
    else:
        # For S3, use local temp directory for state
        base_path = Path("./data")
    
    state_dir = base_path / state_config.state_dir
    
    return StateManager(
        state_dir=state_dir,
        checkpoint_interval=state_config.checkpoint_interval_seconds,
        max_state_files=state_config.max_state_files,
        auto_save_on_shutdown=state_config.auto_save_on_shutdown,
    )
