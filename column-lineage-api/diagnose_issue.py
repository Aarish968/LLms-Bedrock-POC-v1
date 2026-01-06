#!/usr/bin/env python3
"""
Diagnostic script to identify the issue with main.py execution.
"""

import subprocess
import sys
import os
from pathlib import Path


def run_diagnostic():
    """Run diagnostic checks."""
    print("Repository Analysis Diagnostic")
    print("=" * 50)
    
    # Check current directory
    print(f"Current working directory: {Path.cwd()}")
    
    # Check if repositories exist
    frontend_path = Path("Cloned_repo/Frontend/guided-workflow")
    backend_path = Path("Cloned_repo/Backend/guided-workflow-backend")
    
    print(f"\nRepository paths:")
    print(f"  Frontend: {frontend_path} (exists: {frontend_path.exists()})")
    print(f"  Backend: {backend_path} (exists: {backend_path.exists()})")
    
    if frontend_path.exists():
        print(f"  Frontend absolute: {frontend_path.absolute()}")
        # List some files in frontend
        try:
            files = list(frontend_path.rglob("*.ts"))[:5]
            print(f"  Sample TS files: {[f.name for f in files]}")
        except Exception as e:
            print(f"  Error listing TS files: {e}")
    
    if backend_path.exists():
        print(f"  Backend absolute: {backend_path.absolute()}")
        # List some files in backend
        try:
            files = list(backend_path.rglob("*.py"))[:5]
            print(f"  Sample PY files: {[f.name for f in files]}")
        except Exception as e:
            print(f"  Error listing PY files: {e}")
    
    # Check main.py script
    main_script = Path("api/core/repo_analysis/main.py")
    print(f"\nMain script:")
    print(f"  Path: {main_script} (exists: {main_script.exists()})")
    if main_script.exists():
        print(f"  Absolute: {main_script.absolute()}")
    
    # Check action_to_table.py
    action_script = Path("api/core/repo_analysis/action_to_table.py")
    print(f"  action_to_table.py: {action_script} (exists: {action_script.exists()})")
    
    # Test Python import
    print(f"\nTesting Python imports:")
    try:
        repo_analysis_path = Path("api/core/repo_analysis")
        sys.path.insert(0, str(repo_analysis_path))
        
        from action_to_table import CompleteFrontendAnalyzer
        print("  ✅ action_to_table import successful")
        
        from main import EnhancedCSVGenerator
        print("  ✅ main.py import successful")
        
    except Exception as e:
        print(f"  ❌ Import error: {e}")
    
    # Test running main.py with minimal args
    if frontend_path.exists() and backend_path.exists() and main_script.exists():
        print(f"\nTesting main.py execution:")
        
        cmd = [
            sys.executable, str(main_script.absolute()),
            "--frontend", str(frontend_path.absolute()),
            "--backend", str(backend_path.absolute()),
            "--output", "diagnostic_test"
        ]
        
        print(f"Command: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=60,  # 1 minute timeout for diagnostic
                cwd=Path.cwd()
            )
            
            print(f"Return code: {result.returncode}")
            
            if result.stdout:
                print(f"STDOUT (first 500 chars):")
                print(result.stdout[:500])
            
            if result.stderr:
                print(f"STDERR (first 500 chars):")
                print(result.stderr[:500])
            
            # Check if file was created
            output_file = Path("diagnostic_test.csv")
            if output_file.exists():
                print(f"✅ Output file created: {output_file}")
                print(f"   Size: {output_file.stat().st_size} bytes")
            else:
                print(f"❌ Output file not created: {output_file}")
                
        except subprocess.TimeoutExpired:
            print("❌ Script timed out")
        except Exception as e:
            print(f"❌ Error running script: {e}")
    else:
        print(f"\n⚠️  Skipping script test - missing requirements")


if __name__ == "__main__":
    run_diagnostic()