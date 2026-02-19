"""
Model export & production deployment interface.

Provides serialization of trained models, strategy configs, and
signal parameters into self-contained deployment bundles that
the production ingestion/ETL pipeline can consume.

Deployment bundle structure:
    bundle/
    ├── model.joblib           # Serialized ML model (if any)
    ├── scaler.joblib          # Feature scaler
    ├── config.yaml            # Strategy + signal parameters
    ├── features.json          # Required feature list
    └── metadata.json          # Version, training dates, metrics
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import numpy as np

from research.lib.signals import BaseSignal
from research.lib.strategies import BaseStrategy
from research.lib.backtest import BacktestResult


class ModelExporter:
    """Export research artifacts for production deployment.

    Bundles a strategy, signal, optional ML model, and evaluation
    metrics into a standardized directory structure that the
    production system can load.

    Parameters
    ----------
    output_dir : str | Path
        Directory where deployment bundles are saved.
    """

    def __init__(self, output_dir: str | Path = "research/deployments") -> None:
        self.output_dir = Path(output_dir)

    def export(
        self,
        *,
        strategy: BaseStrategy,
        signal: BaseSignal,
        backtest_result: Optional[BacktestResult] = None,
        model: Any = None,
        scaler: Any = None,
        feature_cols: Optional[list[str]] = None,
        training_dates: Optional[list[tuple[int, int, int]]] = None,
        test_dates: Optional[list[tuple[int, int, int]]] = None,
        exchange: str = "coinbaseadvanced",
        symbol: str = "BTC-USD",
        bundle_name: Optional[str] = None,
    ) -> Path:
        """Create a deployment bundle.

        Parameters
        ----------
        strategy : BaseStrategy
        signal : BaseSignal
        backtest_result : BacktestResult | None
        model : sklearn/xgboost model | None
        scaler : sklearn scaler | None
        feature_cols : list[str] | None
        training_dates, test_dates : list of tuples | None
        exchange, symbol : str
        bundle_name : str | None
            Custom name; defaults to ``{strategy.name}_{timestamp}``.

        Returns
        -------
        Path
            Path to the created bundle directory.
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        name = bundle_name or f"{strategy.name}_{timestamp}"
        bundle_dir = self.output_dir / name
        bundle_dir.mkdir(parents=True, exist_ok=True)

        # 1. Strategy & signal config
        config = {
            "strategy": {
                "class": type(strategy).__name__,
                "name": strategy.name,
                "version": strategy.version,
                "params": strategy.params(),
            },
            "signal": {
                "class": type(signal).__name__,
                "name": signal.name,
                "params": {
                    k: v
                    for k, v in signal.__dict__.items()
                    if not k.startswith("_")
                },
            },
            "data": {
                "exchange": exchange,
                "symbol": symbol,
            },
        }
        _write_yaml(bundle_dir / "config.yaml", config)

        # 2. Feature list
        if feature_cols:
            (bundle_dir / "features.json").write_text(
                json.dumps(feature_cols, indent=2)
            )

        # 3. Metadata
        metadata: dict[str, Any] = {
            "exported_at": datetime.now().isoformat(),
            "framework_version": "0.1.0",
            "strategy_name": strategy.name,
            "strategy_version": strategy.version,
        }
        if training_dates:
            metadata["training_dates"] = [
                f"{y}-{m:02d}-{d:02d}" for y, m, d in training_dates
            ]
        if test_dates:
            metadata["test_dates"] = [
                f"{y}-{m:02d}-{d:02d}" for y, m, d in test_dates
            ]
        if backtest_result:
            metadata["backtest_metrics"] = backtest_result.summary()

        (bundle_dir / "metadata.json").write_text(
            json.dumps(metadata, indent=2, default=_json_default)
        )

        # 4. ML model & scaler (optional)
        if model is not None or scaler is not None:
            try:
                import joblib  # type: ignore

                if model is not None:
                    joblib.dump(model, bundle_dir / "model.joblib")
                if scaler is not None:
                    joblib.dump(scaler, bundle_dir / "scaler.joblib")
            except ImportError:
                # Fall back to pickle
                import pickle

                if model is not None:
                    with open(bundle_dir / "model.pkl", "wb") as f:
                        pickle.dump(model, f)
                if scaler is not None:
                    with open(bundle_dir / "scaler.pkl", "wb") as f:
                        pickle.dump(scaler, f)

        return bundle_dir

    @staticmethod
    def load_config(bundle_dir: str | Path) -> dict:
        """Load config.yaml from a deployment bundle."""
        import yaml  # type: ignore

        path = Path(bundle_dir) / "config.yaml"
        with open(path) as f:
            return yaml.safe_load(f)

    @staticmethod
    def load_metadata(bundle_dir: str | Path) -> dict:
        """Load metadata.json from a deployment bundle."""
        path = Path(bundle_dir) / "metadata.json"
        return json.loads(path.read_text())

    @staticmethod
    def list_bundles(output_dir: str | Path = "research/deployments") -> list[str]:
        """List available deployment bundles."""
        base = Path(output_dir)
        if not base.exists():
            return []
        return sorted(
            d.name for d in base.iterdir() if d.is_dir() and (d / "config.yaml").exists()
        )


# ── helpers ─────────────────────────────────────────────────────

def _write_yaml(path: Path, data: dict) -> None:
    """Write YAML config, falling back to JSON if PyYAML unavailable."""
    try:
        import yaml  # type: ignore

        with open(path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
    except ImportError:
        # Fallback: write as JSON with .yaml extension
        path.write_text(json.dumps(data, indent=2, default=_json_default))


def _json_default(obj: Any) -> Any:
    """JSON serializer for numpy types."""
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
