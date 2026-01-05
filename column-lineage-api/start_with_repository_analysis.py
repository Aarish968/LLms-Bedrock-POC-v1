#!/usr/bin/env python3
"""
Startup script for Column Lineage API with Repository Analysis
This script helps start the API server with repository analysis enabled.
"""

import os
import sys
from pathlib import Path

def check_dependencies():
    """Check if required dependencies are available."""
    required_modules = [
        'fastapi',
        'uvicorn',
        'pydantic',
        'pandas',
        'boto3',
        'structlog'
    ]
    
    missing = []
    for module in required_modules:
        try:
            __import__(module)
        except ImportError:
            missing.append(module)
    
    if missing:
        print(f"❌ Missing dependencies: {', '.join(missing)}")
        print("Please install them with: pip install -r requirements.txt")
        print("Or if using uv: uv sync")
        return False
    
    print("✅ All required dependencies are available")
    return True


def check_repository_analysis_files():
    """Check if repository analysis files are present."""
    required_files = [
        'api/v1/models/repository_analysis.py',
        'api/v1/services/repository_analysis_service.py',
        'api/v1/routers/repository_analysis.py',
        'smart_analyzer.py'
    ]
    
    missing = []
    for file_path in required_files:
        if not Path(file_path).exists():
            missing.append(file_path)
    
    if missing:
        print(f"❌ Missing repository analysis files: {', '.join(missing)}")
        return False
    
    print("✅ All repository analysis files are present")
    return True


def check_credentials():
    """Check if AWS credentials are configured."""
    credentials_file = Path("credentials.txt")
    
    if not credentials_file.exists():
        print("⚠️  credentials.txt not found")
        print("Repository analysis will not be able to access AWS CodeCommit")
        print("Create credentials.txt with:")
        print("username=your-codecommit-username")
        print("password=your-codecommit-password")
        return False
    
    print("✅ Credentials file found")
    return True


def start_server():
    """Start the FastAPI server."""
    try:
        import uvicorn
        from api.main import app
        
        print("🚀 Starting Column Lineage API with Repository Analysis...")
        print("📊 Repository Analysis endpoints will be available at:")
        print("   - POST /api/v1/repository-analysis/analyze")
        print("   - GET  /api/v1/repository-analysis/jobs/{job_id}")
        print("   - GET  /api/v1/repository-analysis/jobs/{job_id}/progress")
        print("   - GET  /api/v1/repository-analysis/jobs/{job_id}/results")
        print("   - GET  /api/v1/repository-analysis/discover-repositories")
        print("📖 API documentation: http://localhost:8000/docs")
        print()
        
        uvicorn.run(
            app,
            host="0.0.0.0",
            port=8000,
            reload=True,
            log_level="info"
        )
        
    except ImportError as e:
        print(f"❌ Failed to import required modules: {e}")
        print("Make sure you're in the correct virtual environment")
        return False
    except Exception as e:
        print(f"❌ Failed to start server: {e}")
        return False


def main():
    """Main startup function."""
    print("Column Lineage API with Repository Analysis")
    print("=" * 50)
    
    # Check dependencies
    if not check_dependencies():
        return False
    
    # Check repository analysis files
    if not check_repository_analysis_files():
        return False
    
    # Check credentials (warning only)
    check_credentials()
    
    print("\n🎯 All checks passed! Starting server...")
    print("=" * 50)
    
    # Start server
    return start_server()


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n👋 Server stopped by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1)