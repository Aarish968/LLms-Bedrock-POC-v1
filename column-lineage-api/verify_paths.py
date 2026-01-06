#!/usr/bin/env python3
"""
Verification script to show the exact paths used in repository analysis.
"""

import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from api.v1.services.repository_analysis_service import RepositoryAnalysisService
from api.v1.models.repository_analysis import RepositoryAnalysisRequest


def main():
    """Show the paths that will be used for repository analysis."""
    print("Repository Analysis Path Verification")
    print("=" * 50)
    
    # Create service instance
    service = RepositoryAnalysisService()
    
    # Show base directories
    print(f"Base clone directory: {service.base_clone_dir}")
    print(f"Frontend clone directory: {service.frontend_clone_dir}")
    print(f"Backend clone directory: {service.backend_clone_dir}")
    
    # Show example paths for default repositories
    frontend_repo = "guided-workflow"
    backend_repo = "guided-workflow-backend"
    
    frontend_path = service.frontend_clone_dir / frontend_repo
    backend_path = service.backend_clone_dir / backend_repo
    
    print(f"\nFor repositories '{frontend_repo}' and '{backend_repo}':")
    print(f"Frontend will be cloned to: {frontend_path}")
    print(f"Backend will be cloned to: {backend_path}")
    
    # Show the main.py script path
    main_script_path = Path(__file__).parent / "api" / "core" / "repo_analysis" / "main.py"
    print(f"\nMain analysis script: {main_script_path}")
    print(f"Script exists: {main_script_path.exists()}")
    
    # Show what command would be executed
    output_file = "example_analysis.csv"
    output_base = output_file[:-4] if output_file.endswith('.csv') else output_file
    
    cmd_args = [
        "python", str(main_script_path),
        "--frontend", str(frontend_path),
        "--backend", str(backend_path),
        "--output", output_base,
    ]
    
    print(f"\nCommand that would be executed:")
    print(f"  {' '.join(cmd_args)}")
    
    # Check if directories exist
    print(f"\nDirectory status:")
    print(f"  Base clone dir exists: {service.base_clone_dir.exists()}")
    print(f"  Frontend clone dir exists: {service.frontend_clone_dir.exists()}")
    print(f"  Backend clone dir exists: {service.backend_clone_dir.exists()}")
    print(f"  Frontend repo exists: {frontend_path.exists()}")
    print(f"  Backend repo exists: {backend_path.exists()}")
    
    # Show current working directory
    print(f"\nCurrent working directory: {Path.cwd()}")
    print(f"Output file would be created at: {Path.cwd() / f'{output_base}.csv'}")


if __name__ == "__main__":
    main()