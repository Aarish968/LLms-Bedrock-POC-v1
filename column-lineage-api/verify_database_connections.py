"""
Verification script to check database connection consistency across all analysis modules.
This script verifies that all modules use the same database infrastructure pattern.
"""

import sys
import ast
import os
from pathlib import Path


def check_file_imports(file_path: str) -> dict:
    """Check if file uses correct database imports."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        tree = ast.parse(content)
        
        imports = {
            'has_get_database_engine': False,
            'has_DatabaseManager': False,
            'has_old_sec_import': False,
            'has_get_sf_connection_engine_method': False,
            'has_get_database_manager_method': False
        }
        
        # Check imports
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                if node.module == 'api.dependencies.database':
                    for alias in node.names:
                        if alias.name == 'get_database_engine':
                            imports['has_get_database_engine'] = True
                        if alias.name == 'DatabaseManager':
                            imports['has_DatabaseManager'] = True
                
                if node.module == 'common':
                    for alias in node.names:
                        if alias.name == 'sec':
                            imports['has_old_sec_import'] = True
            
            # Check for method definitions
            if isinstance(node, ast.FunctionDef):
                if node.name == 'get_sf_connection_engine':
                    imports['has_get_sf_connection_engine_method'] = True
                if node.name == 'get_database_manager':
                    imports['has_get_database_manager_method'] = True
        
        return imports
    
    except Exception as e:
        print(f"Error checking {file_path}: {e}")
        return None


def verify_module(module_name: str, file_path: str) -> bool:
    """Verify a single module."""
    print(f"\n{'='*60}")
    print(f"Checking: {module_name}")
    print(f"File: {file_path}")
    print(f"{'='*60}")
    
    if not os.path.exists(file_path):
        print(f"❌ File not found!")
        return False
    
    imports = check_file_imports(file_path)
    
    if imports is None:
        print(f"❌ Failed to parse file!")
        return False
    
    # Check for correct pattern
    has_correct_imports = imports['has_get_database_engine'] and imports['has_DatabaseManager']
    has_old_pattern = imports['has_old_sec_import']
    has_methods = imports['has_get_sf_connection_engine_method'] or imports['has_get_database_manager_method']
    
    print(f"\n📋 Import Analysis:")
    print(f"  ✅ get_database_engine imported: {imports['has_get_database_engine']}")
    print(f"  ✅ DatabaseManager imported: {imports['has_DatabaseManager']}")
    print(f"  ⚠️  Old 'sec' import found: {imports['has_old_sec_import']}")
    
    print(f"\n📋 Method Analysis:")
    print(f"  ✅ get_sf_connection_engine() method: {imports['has_get_sf_connection_engine_method']}")
    print(f"  ✅ get_database_manager() method: {imports['has_get_database_manager_method']}")
    
    print(f"\n📊 Overall Status:")
    
    if has_correct_imports and not has_old_pattern:
        print(f"  ✅ PASS - Uses correct database infrastructure")
        return True
    elif has_correct_imports and has_old_pattern:
        print(f"  ⚠️  WARNING - Has correct imports but also old 'sec' import")
        print(f"     (This might be okay if 'sec' is used for other purposes)")
        return True
    else:
        print(f"  ❌ FAIL - Missing correct database imports")
        return False


def main():
    """Main verification function."""
    print("\n" + "="*60)
    print("🔍 Database Connection Pattern Verification")
    print("="*60)
    print("\nVerifying that all analysis modules use the same database infrastructure...")
    
    modules_to_check = [
        {
            'name': 'SP Analyzer',
            'path': 'column-lineage-api/api/core/sp_analysis/sp_analyzer.py'
        },
        {
            'name': 'ThoughtSpot Analysis',
            'path': 'column-lineage-api/api/core/toughtspot_to_table/thoughtspot_to_table_analysis.py'
        },
        {
            'name': 'SP Analysis Service',
            'path': 'column-lineage-api/api/v1/services/sp_analysis_service.py'
        },
        {
            'name': 'ThoughtSpot Analysis Service',
            'path': 'column-lineage-api/api/v1/services/thoughtspot_analysis_service.py'
        }
    ]
    
    results = []
    
    for module in modules_to_check:
        result = verify_module(module['name'], module['path'])
        results.append({
            'name': module['name'],
            'passed': result
        })
    
    # Print summary
    print("\n" + "="*60)
    print("📊 VERIFICATION SUMMARY")
    print("="*60)
    
    passed_count = sum(1 for r in results if r['passed'])
    total_count = len(results)
    
    for result in results:
        status = "✅ PASS" if result['passed'] else "❌ FAIL"
        print(f"  {status} - {result['name']}")
    
    print(f"\n{'='*60}")
    print(f"Results: {passed_count}/{total_count} modules passed")
    print(f"{'='*60}")
    
    if passed_count == total_count:
        print("\n🎉 All modules use consistent database infrastructure!")
        print("✅ Verification PASSED")
        return 0
    else:
        print("\n⚠️  Some modules need updates")
        print("❌ Verification FAILED")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
