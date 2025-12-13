# FluxForge Documentation

**Quick Links**:
- 🚀 [**New Agent Quick Start**](NEW_AGENT_PROMPT.md) - Start here for new Copilot sessions
- 📖 [**Complete System Reference**](SYSTEM_ONBOARDING.md) - Comprehensive documentation
- 🗺️ [**Documentation Index**](INDEX.md) - Navigation hub and quick reference

---

## Documentation Files

### Getting Started
- **[NEW_AGENT_PROMPT.md](NEW_AGENT_PROMPT.md)** - Copy-paste onboarding for new agents (10 min read)
- **[SYSTEM_ONBOARDING.md](SYSTEM_ONBOARDING.md)** - Complete system documentation (30 pages)
- **[INDEX.md](INDEX.md)** - Central navigation, quick reference, commands

### Feature Engineering
- **[PROCESSOR_OPTIONS.md](PROCESSOR_OPTIONS.md)** - Configure StateConfig parameters
- **[RESEARCH_PROMPT_ORDERBOOK_QUANT.md](RESEARCH_PROMPT_ORDERBOOK_QUANT.md)** - Research prompt for optimal configs

### Storage & Infrastructure
- **[UNIFIED_STORAGE_ARCHITECTURE.md](UNIFIED_STORAGE_ARCHITECTURE.md)** - Storage backend design
- **[HYBRID_STORAGE_GUIDE.md](HYBRID_STORAGE_GUIDE.md)** - Local + S3 patterns
- **[PARQUET_OPERATIONS.md](PARQUET_OPERATIONS.md)** - Parquet best practices

### ETL & Processing
- **[CCXT_ETL_ARCHITECTURE.md](CCXT_ETL_ARCHITECTURE.md)** - CCXT pipeline design
- **[FILENAME_PATTERNS.md](FILENAME_PATTERNS.md)** - Segment naming conventions

### Strategy
- **[US_CRYPTO_STRATEGY.md](US_CRYPTO_STRATEGY.md)** - US regulatory considerations

### Project Management
- **[CLEANUP_SUMMARY.md](CLEANUP_SUMMARY.md)** - Summary of December 2025 cleanup session

---

## Recommended Reading Order

### For New Developers / Agents
1. [NEW_AGENT_PROMPT.md](NEW_AGENT_PROMPT.md) - Quick onboarding
2. [SYSTEM_ONBOARDING.md](SYSTEM_ONBOARDING.md) - Deep dive
3. [INDEX.md](INDEX.md) - Bookmark for quick reference

### For Configuration Changes
1. [PROCESSOR_OPTIONS.md](PROCESSOR_OPTIONS.md) - Feature engineering params
2. [SYSTEM_ONBOARDING.md](SYSTEM_ONBOARDING.md) - Configuration flow section

### For Storage Setup
1. [UNIFIED_STORAGE_ARCHITECTURE.md](UNIFIED_STORAGE_ARCHITECTURE.md) - Architecture
2. [HYBRID_STORAGE_GUIDE.md](HYBRID_STORAGE_GUIDE.md) - Implementation patterns

### For Research
1. [RESEARCH_PROMPT_ORDERBOOK_QUANT.md](RESEARCH_PROMPT_ORDERBOOK_QUANT.md) - Research questions

---

## Quick Command Reference

```bash
# Test configuration flow
python scripts/test_config_flow.py

# Check system health
python scripts/check_health.py

# Start ingestion
python scripts/run_ingestion.py --sources ccxt

# Start ETL
python scripts/run_etl_watcher.py --poll-interval 30
```

---

**Last Updated**: December 12, 2025
