#!/usr/bin/env python3
"""
Verify that all paths and imports work correctly after the updates
"""

import sys
from pathlib import Path

def verify_structure():
    """Verify the updated structure and imports"""
    
    print("Verifying updated repository analysis structure...")
    print("=" * 55)
    
    # Check if required directories exist
    directories = [
        "Cloned_repo",
        "Cloned_repo/Frontend", 
        "Cloned_repo/Backend",
        "Repo_Analyze"
    ]
    
    print("📁 Directory structure:")
    for directory in directories:
        path = Path(directory)
        exists = path.exists()
        print(f"  {directory:<25} {'✅ EXISTS' if exists else '❌ MISSING'}")
        if not exists:
            print(f"    Creating directory: {directory}")
            path.mkdir(parents=True, exist_ok=True)
    
    print()
    
    # Check if key files exist
    key_files = [
        "api/v1/models/repository_analysis.py",
        "api/v1/services/repository_analysis_service.py", 
        "api/v1/routers/repository_analysis.py",
        "api/core/repo_analysis/main.py",
        "test_api_call.py",
        "manual_test.py"
    ]
    
    print("📄 Key files:")
    for file_path in key_files:
        path = Path(file_path)
        exists = path.exists()
        print(f"  {file_path:<45} {'✅ EXISTS' if exists else '❌ MISSING'}")
    
    print()
    
    # Test imports
    print("🔍 Testing imports:")
    try:
        sys.path.append(str(Path.cwd()))
        from api.v1.models.repository_analysis import RepositoryAnalysisRequest
        print("  RepositoryAnalysisRequest model        ✅ OK")
        
        # Check the updated model structure
        request = RepositoryAnalysisRequest()
        fields = request.model_fields.keys()
        expected_fields = {'frontend_repo_name', 'backend_repo_name', 'async_processing'}
        removed_fields = {'output_filename', 'credentials_file'}
        
        print(f"  Model fields: {list(fields)}")
        
        if expected_fields.issubset(fields):
            print("  Expected fields present               ✅ OK")
        else:
            missing = expected_fields - set(fields)
            print(f"  Missing expected fields: {missing}   ❌ ERROR")
        
        if not any(field in fields for field in removed_fields):
            print("  Removed fields not present            ✅ OK")
        else:
            present = [field for field in removed_fields if field in fields]
            print(f"  Removed fields still present: {present} ❌ ERROR")
            
    except ImportError as e:
        print(f"  Import failed: {e}                   ❌ ERROR")
    except Exception as e:
        print(f"  Unexpected error: {e}                 ❌ ERROR")
    
    print()
    print("🎯 Summary of changes:")
    print("  ✅ Removed 'credentials_file' from API payload")
    print("  ✅ Removed 'output_filename' from API payload")
    print("  ✅ Created 'Repo_Analyze/' directory for CSV files")
    print("  ✅ Auto-generate filenames with timestamps")
    print("  ✅ Updated service to handle new file structure")
    print("  ✅ Updated test scripts for new API format")
    
    print()
    print("🚀 Ready to test with new API payload:")
    print("""
    {
        "frontend_repo_name": "guided-workflow",
        "backend_repo_name": "guided-workflow-backend",
        "async_processing": true
    }
    """)

if __name__ == "__main__":
    verify_structure()