#!/usr/bin/env python3
"""
Script to find where the CSV file is actually being created.
"""

import os
from pathlib import Path


def find_csv_files():
    """Find all CSV files that might be our output."""
    print("Searching for CSV files...")
    print("=" * 40)
    
    # Search in current directory
    current_dir = Path.cwd()
    print(f"Current directory: {current_dir}")
    
    csv_files = list(current_dir.glob("*.csv"))
    print(f"CSV files in current directory: {len(csv_files)}")
    for f in csv_files:
        stat = f.stat()
        print(f"  - {f.name} ({stat.st_size} bytes, modified: {stat.st_mtime})")
    
    # Search for files with our expected names
    expected_names = ["latest-file.csv", "latest-file", "new.csv", "repo_analysis*.csv"]
    
    print(f"\nSearching for expected filenames...")
    for pattern in expected_names:
        matches = list(current_dir.glob(pattern))
        if matches:
            print(f"  Found {pattern}: {[f.name for f in matches]}")
        else:
            print(f"  Not found: {pattern}")
    
    # Search in subdirectories
    print(f"\nSearching in subdirectories...")
    for subdir in ["api", "Cloned_repo", "api/core/repo_analysis"]:
        subdir_path = current_dir / subdir
        if subdir_path.exists():
            csv_files = list(subdir_path.glob("*.csv"))
            if csv_files:
                print(f"  {subdir}: {[f.name for f in csv_files]}")
            else:
                print(f"  {subdir}: no CSV files")
        else:
            print(f"  {subdir}: directory doesn't exist")
    
    # Search recursively for any CSV files modified in the last hour
    print(f"\nRecent CSV files (modified in last hour):")
    import time
    one_hour_ago = time.time() - 3600
    
    for csv_file in current_dir.rglob("*.csv"):
        if csv_file.stat().st_mtime > one_hour_ago:
            rel_path = csv_file.relative_to(current_dir)
            print(f"  - {rel_path} ({csv_file.stat().st_size} bytes)")


if __name__ == "__main__":
    find_csv_files()