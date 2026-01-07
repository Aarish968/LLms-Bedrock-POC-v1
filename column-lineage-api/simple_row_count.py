#!/usr/bin/env python3
"""
Simple script to count CSV rows vs expected database rows.
"""

import csv
from pathlib import Path

def count_csv_rows():
    """Count rows in the most recent CSV file."""
    
    print("📊 Counting CSV rows...")
    print("=" * 40)
    
    # Find the most recent CSV file
    repo_analyze_dir = Path("Repo_Analyze")
    if not repo_analyze_dir.exists():
        print("❌ Repo_Analyze directory not found")
        return 0, 0
    
    csv_files = list(repo_analyze_dir.glob("*.csv"))
    if not csv_files:
        print("❌ No CSV files found")
        return 0, 0
    
    # Get the most recent CSV file
    latest_csv = max(csv_files, key=lambda x: x.stat().st_mtime)
    print(f"📄 Analyzing: {latest_csv}")
    
    try:
        with open(latest_csv, 'r', encoding='utf-8') as csvfile:
            # Count total lines
            lines = csvfile.readlines()
            total_lines = len(lines)
            
            # Count non-empty lines
            non_empty_lines = sum(1 for line in lines if line.strip())
            
            # Count data rows (excluding header)
            csvfile.seek(0)
            reader = csv.DictReader(csvfile)
            data_rows = 0
            empty_rows = 0
            problematic_rows = []
            
            for row_num, row in enumerate(reader, 1):
                data_rows += 1
                
                # Check if row is essentially empty
                if all(not value or value.strip() == '' for value in row.values()):
                    empty_rows += 1
                    problematic_rows.append(f"Row {row_num}: Completely empty")
                    continue
                
                # Check for missing essential fields
                frontend_file = row.get('Frontend_File', '').strip()
                frontend_function = row.get('Frontend_Function', '').strip()
                
                if not frontend_file and not frontend_function:
                    problematic_rows.append(f"Row {row_num}: Missing frontend_file and frontend_function")
                elif not frontend_file:
                    problematic_rows.append(f"Row {row_num}: Missing frontend_file")
                elif not frontend_function:
                    problematic_rows.append(f"Row {row_num}: Missing frontend_function")
            
            print(f"📈 Total lines in file: {total_lines}")
            print(f"📊 Non-empty lines: {non_empty_lines}")
            print(f"📋 Data rows (excluding header): {data_rows}")
            print(f"🗑️ Empty rows: {empty_rows}")
            print(f"⚠️ Problematic rows: {len(problematic_rows)}")
            
            if problematic_rows:
                print("\n🚨 Problematic rows details:")
                for issue in problematic_rows[:10]:  # Show first 10
                    print(f"  {issue}")
                if len(problematic_rows) > 10:
                    print(f"  ... and {len(problematic_rows) - 10} more")
            
            expected_insertions = data_rows - len(problematic_rows)
            print(f"\n🎯 Expected successful insertions: {expected_insertions}")
            
            return data_rows, expected_insertions
            
    except Exception as e:
        print(f"❌ Error analyzing CSV: {e}")
        return 0, 0

def main():
    """Main function."""
    print("🔍 CSV ROW ANALYSIS")
    print("=" * 50)
    
    total_rows, expected_insertions = count_csv_rows()
    
    print(f"\n📊 SUMMARY:")
    print(f"   Total CSV data rows: {total_rows}")
    print(f"   Expected insertions: {expected_insertions}")
    print(f"   You reported DB rows: 167")
    
    if expected_insertions > 0:
        missing = expected_insertions - 167
        print(f"   Missing rows: {missing}")
        
        if missing > 0:
            print(f"\n💡 {missing} rows are likely failing during insertion")
            print("   Check the application logs for specific error messages")
        elif missing == 0:
            print(f"\n✅ Row counts match! All processable rows were inserted")
        else:
            print(f"\n🤔 More rows in DB than expected - possible duplicates?")

if __name__ == "__main__":
    main()