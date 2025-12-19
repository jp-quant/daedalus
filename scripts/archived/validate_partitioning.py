"""
Validation script to verify partitioning implementation is correct.

Run this after implementing the new partitioning approach to ensure:
1. Partition columns are in the data
2. No hive_partitioning flags in code
3. Filename patterns are consistent
4. No file collisions exist
"""
import sys
from pathlib import Path
import re
from collections import Counter

sys.path.insert(0, str(Path(__file__).parent.parent))


def check_partition_columns_in_data(dataset_dir: str) -> bool:
    """
    Verify partition columns are actually in the Parquet data.
    
    Args:
        dataset_dir: Path to processed dataset (e.g., F:/processed/coinbase/ticker)
    
    Returns:
        True if partition columns found in data
    """
    try:
        import polars as pl
        
        print(f"\n{'='*80}")
        print("TEST 1: Verify Partition Columns in Data")
        print(f"{'='*80}")
        
        # Find first parquet file
        parquet_files = list(Path(dataset_dir).rglob("*.parquet"))
        if not parquet_files:
            print(f"❌ No parquet files found in {dataset_dir}")
            return False
        
        # Read first file
        df = pl.read_parquet(parquet_files[0])
        columns = df.columns
        
        print(f"Dataset: {dataset_dir}")
        print(f"Sample file: {parquet_files[0].name}")
        print(f"Columns: {columns}")
        
        # Check for common partition columns
        partition_cols = ['product_id', 'date', 'hour', 'year', 'month', 'day']
        found_partitions = [col for col in partition_cols if col in columns]
        
        if found_partitions:
            print(f"✅ Found partition columns in data: {found_partitions}")
            return True
        else:
            print(f"❌ No partition columns found in data")
            print(f"   Expected at least one of: {partition_cols}")
            return False
    
    except Exception as e:
        print(f"❌ Error checking partition columns: {e}")
        return False


def check_filename_patterns(dataset_dir: str) -> bool:
    """
    Verify all parquet files follow standard naming pattern.
    
    Expected: part_YYYYMMDDTHH_<uuid8>.parquet
    
    Args:
        dataset_dir: Path to processed dataset
    
    Returns:
        True if all files follow pattern
    """
    print(f"\n{'='*80}")
    print("TEST 2: Verify Filename Patterns")
    print(f"{'='*80}")
    
    pattern = re.compile(r'part_\d{8}T\d{2}_[a-f0-9]{8}\.parquet')
    
    all_files = list(Path(dataset_dir).rglob("*.parquet"))
    bad_files = [f for f in all_files if not pattern.match(f.name)]
    
    print(f"Total files: {len(all_files)}")
    print(f"Standard pattern: part_YYYYMMDDTHH_<uuid8>.parquet")
    
    if bad_files:
        print(f"❌ Found {len(bad_files)} files with non-standard names:")
        for f in bad_files[:10]:  # Show first 10
            print(f"   {f.name}")
        if len(bad_files) > 10:
            print(f"   ... and {len(bad_files) - 10} more")
        return False
    else:
        print(f"✅ All {len(all_files)} files follow standard naming pattern")
        return True


def check_for_collisions(dataset_dir: str) -> bool:
    """
    Check for duplicate filenames across all partitions.
    
    Args:
        dataset_dir: Path to processed dataset
    
    Returns:
        True if no collisions found
    """
    print(f"\n{'='*80}")
    print("TEST 3: Check for Filename Collisions")
    print(f"{'='*80}")
    
    all_files = [f.name for f in Path(dataset_dir).rglob("*.parquet")]
    duplicates = {k: v for k, v in Counter(all_files).items() if v > 1}
    
    print(f"Total files: {len(all_files)}")
    print(f"Unique filenames: {len(set(all_files))}")
    
    if duplicates:
        print(f"❌ Found {len(duplicates)} duplicate filenames:")
        for name, count in list(duplicates.items())[:10]:
            print(f"   {name}: {count} occurrences")
        return False
    else:
        print(f"✅ No duplicate filenames found")
        return True


def check_code_for_hive_partitioning() -> bool:
    """
    Scan codebase for remaining hive_partitioning=True usage.
    
    Returns:
        True if no problematic usage found
    """
    print(f"\n{'='*80}")
    print("TEST 4: Check for hive_partitioning in Code")
    print(f"{'='*80}")
    
    # Directories to check
    code_dirs = ['etl', 'ingestion', 'scripts', 'examples']
    
    # Files to exclude (documentation is OK)
    exclude_patterns = ['docs/', 'PARTITIONING_CHANGES.md', 'FILENAME_PATTERNS.md']
    
    bad_files = []
    
    for code_dir in code_dirs:
        dir_path = Path(__file__).parent.parent / code_dir
        if not dir_path.exists():
            continue
        
        for py_file in dir_path.rglob("*.py"):
            # Skip excluded files
            if any(pattern in str(py_file) for pattern in exclude_patterns):
                continue
            
            content = py_file.read_text()
            
            # Check for hive_partitioning=True
            if 'hive_partitioning=True' in content:
                bad_files.append(py_file)
    
    if bad_files:
        print(f"❌ Found hive_partitioning=True in {len(bad_files)} files:")
        for f in bad_files:
            print(f"   {f.relative_to(Path(__file__).parent.parent)}")
        return False
    else:
        print(f"✅ No hive_partitioning=True found in code files")
        return True


def check_query_compatibility(dataset_dir: str) -> bool:
    """
    Verify queries work without hive_partitioning flag.
    
    Args:
        dataset_dir: Path to processed dataset
    
    Returns:
        True if queries work correctly
    """
    try:
        import polars as pl
        
        print(f"\n{'='*80}")
        print("TEST 5: Query Compatibility")
        print(f"{'='*80}")
        
        # Test 1: Basic scan
        df = pl.scan_parquet(str(Path(dataset_dir) / "**/*.parquet"))
        columns = df.collect_schema().names()
        
        print(f"✅ Basic scan successful")
        print(f"   Columns available: {columns[:10]}...")
        
        # Test 2: Filter on partition column
        partition_col = None
        for col in ['product_id', 'date']:
            if col in columns:
                partition_col = col
                break
        
        if partition_col:
            filtered = df.filter(pl.col(partition_col).is_not_null())
            count = filtered.select(pl.count()).collect().item()
            print(f"✅ Filtering on partition column '{partition_col}' works")
            print(f"   Rows: {count:,}")
        else:
            print(f"⚠️  No common partition columns found for testing")
        
        return True
    
    except Exception as e:
        print(f"❌ Query compatibility test failed: {e}")
        return False


def run_all_tests(dataset_dir: str = None):
    """
    Run all validation tests.
    
    Args:
        dataset_dir: Path to processed dataset. If None, skips data tests.
    """
    print("\n" + "="*80)
    print("DAEDALUS PARTITIONING VALIDATION")
    print("="*80)
    
    results = {}
    
    # Code-only tests (don't require data)
    results['code_check'] = check_code_for_hive_partitioning()
    
    # Data tests (require dataset path)
    if dataset_dir:
        dataset_path = Path(dataset_dir)
        if dataset_path.exists():
            results['partition_cols'] = check_partition_columns_in_data(dataset_dir)
            results['filename_patterns'] = check_filename_patterns(dataset_dir)
            results['collisions'] = check_for_collisions(dataset_dir)
            results['query_compat'] = check_query_compatibility(dataset_dir)
        else:
            print(f"\n⚠️  Dataset directory not found: {dataset_dir}")
            print("   Skipping data validation tests")
    else:
        print("\n⚠️  No dataset directory provided")
        print("   Skipping data validation tests")
        print("   Usage: python scripts/validate_partitioning.py F:/processed/coinbase/ticker")
    
    # Summary
    print(f"\n{'='*80}")
    print("VALIDATION SUMMARY")
    print(f"{'='*80}")
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for test, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {test}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All validation tests passed!")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) failed")
        return 1


if __name__ == "__main__":
    import sys
    
    # Get dataset directory from command line or use default
    if len(sys.argv) > 1:
        dataset_dir = sys.argv[1]
    else:
        # Try common location
        dataset_dir = "F:/processed/coinbase/ticker"
        if not Path(dataset_dir).exists():
            dataset_dir = None
    
    exit_code = run_all_tests(dataset_dir)
    sys.exit(exit_code)
