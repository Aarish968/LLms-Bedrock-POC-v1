#!/usr/bin/env python3
"""
Smart Frontend-Backend Analyzer with Auto-Cloning
This script intelligently handles repository discovery and analysis:
1. Checks if frontend/backend paths exist locally
2. If missing, automatically clones from AWS CodeCommit
3. Performs comprehensive analysis with table-column mapping

Usage:
uv run python smart_analyzer.py
uv run python smart_analyzer.py --frontend my-frontend --backend my-backend
uv run python smart_analyzer.py --auto-discover --credentials credentials.txt
"""

import os
import sys
import logging
import argparse
from pathlib import Path
from typing import Optional, Tuple, List, Dict
import subprocess
import tempfile
import shutil

# Import existing functionality
from main import EnhancedCSVGenerator
from clone_prefect_repos import PrefectRepoCloner
from action_to_table import CompleteFrontendAnalyzer, CompleteBackendAnalyzer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SmartAnalyzer:
    """
    Smart analyzer that handles repository discovery, cloning, and analysis
    """
    
    def __init__(self, credentials_file: str = "credentials.txt", 
                 clone_dir: str = "auto_cloned_repos"):
        self.credentials_file = credentials_file
        self.clone_dir = clone_dir
        self.cloner = PrefectRepoCloner(credentials_file)
        
        # Repository patterns for auto-discovery
        self.frontend_patterns = [
            r'.*-frontend$',
            r'.*-ui$', 
            r'.*-web$',
            r'.*-client$',
            r'frontend-.*',
            r'ui-.*',
            r'web-.*',
            r'client-.*',
            r'.*workflow.*frontend.*',
            r'.*workflow.*ui.*',
            # Add patterns for workflow repos without explicit frontend/ui
            r'^guided-workflow$',
            r'.*workflow$',
            r'.*-workflow$'
        ]
        
        self.backend_patterns = [
            r'.*-backend$',
            r'.*-api$',
            r'.*-server$',
            r'.*-service$',
            r'backend-.*',
            r'api-.*',
            r'server-.*',
            r'service-.*',
            r'.*workflow.*backend.*',
            r'.*workflow.*api.*',
            # Add patterns for your specific repos
            r'guided-workflow-backend$',
            r'.*-workflow-backend$',
            r'action-to-table.*'
        ]
    
    def check_local_paths(self, frontend_path: str, backend_path: str) -> Tuple[bool, bool]:
        """Check if frontend and backend paths exist locally"""
        frontend_exists = Path(frontend_path).exists()
        backend_exists = Path(backend_path).exists()
        
        logger.info(f"Local path check:")
        logger.info(f"  Frontend ({frontend_path}): {'EXISTS' if frontend_exists else 'MISSING'}")
        logger.info(f"  Backend ({backend_path}): {'EXISTS' if backend_exists else 'MISSING'}")
        
        return frontend_exists, backend_exists
    
    def discover_repositories(self) -> Dict[str, List[str]]:
        """Discover frontend and backend repositories from CodeCommit"""
        logger.info("Discovering repositories from AWS CodeCommit...")
        
        # Setup credentials
        self.cloner.setup_aws_credentials()
        
        # Get all repositories
        all_repos = self.cloner.get_all_repositories_via_boto3()
        if not all_repos:
            logger.error("Could not retrieve repository list")
            return {'frontend': [], 'backend': []}
        
        frontend_repos = []
        backend_repos = []
        
        # Categorize repositories
        for repo in all_repos:
            repo_name = repo['repositoryName']
            
            # Check frontend patterns
            for pattern in self.frontend_patterns:
                import re
                if re.match(pattern, repo_name, re.IGNORECASE):
                    frontend_repos.append(repo_name)
                    break
            
            # Check backend patterns  
            for pattern in self.backend_patterns:
                import re
                if re.match(pattern, repo_name, re.IGNORECASE):
                    backend_repos.append(repo_name)
                    break
        
        logger.info(f"Discovered repositories:")
        logger.info(f"  Frontend candidates: {len(frontend_repos)}")
        logger.info(f"  Backend candidates: {len(backend_repos)}")
        
        return {
            'frontend': frontend_repos,
            'backend': backend_repos
        }
    
    def clone_repository_smart(self, repo_name: str, repo_type: str) -> Optional[str]:
        """Clone a repository and return the local path"""
        target_path = Path(self.clone_dir) / repo_name
        
        if target_path.exists():
            logger.info(f"Repository {repo_name} already exists locally, updating...")
            try:
                # Try to pull latest changes
                result = subprocess.run(
                    ['git', 'pull'],
                    cwd=str(target_path),
                    capture_output=True,
                    text=True,
                    timeout=120
                )
                if result.returncode == 0:
                    logger.info(f"Successfully updated {repo_name}")
                else:
                    logger.warning(f"Could not update {repo_name}, using existing version")
                return str(target_path)
            except Exception as e:
                logger.warning(f"Could not update {repo_name}: {e}, using existing version")
                return str(target_path)
        
        # Clone the repository
        logger.info(f"Cloning {repo_type} repository: {repo_name}")
        success = self.cloner.clone_repository(repo_name, self.clone_dir)
        
        if success:
            logger.info(f"Successfully cloned {repo_name}")
            return str(target_path)
        else:
            logger.error(f"Failed to clone {repo_name}")
            return None
    
    def find_nested_projects(self, repo_path: str) -> Tuple[Optional[str], Optional[str]]:
        """Find frontend and backend projects inside a cloned repository"""
        repo_path_obj = Path(repo_path)
        
        frontend_candidates = []
        backend_candidates = []
        
        # Look for subdirectories that match frontend/backend patterns
        if repo_path_obj.exists():
            for item in repo_path_obj.iterdir():
                if item.is_dir():
                    item_name = item.name.lower()
                    
                    # Check for frontend patterns
                    if any(pattern in item_name for pattern in ['frontend', 'ui', 'web', 'client', 'workflow']):
                        if not any(pattern in item_name for pattern in ['backend', 'api', 'server']):
                            frontend_candidates.append(str(item))
                    
                    # Check for backend patterns
                    if any(pattern in item_name for pattern in ['backend', 'api', 'server', 'service']):
                        backend_candidates.append(str(item))
        
        # Select best candidates
        frontend_path = frontend_candidates[0] if frontend_candidates else None
        backend_path = backend_candidates[0] if backend_candidates else None
        
        logger.info(f"Found nested projects in {repo_path}:")
        logger.info(f"  Frontend candidates: {[Path(p).name for p in frontend_candidates]}")
        logger.info(f"  Backend candidates: {[Path(p).name for p in backend_candidates]}")
        logger.info(f"  Selected frontend: {Path(frontend_path).name if frontend_path else 'None'}")
        logger.info(f"  Selected backend: {Path(backend_path).name if backend_path else 'None'}")
        
        return frontend_path, backend_path

    def auto_resolve_paths(self, frontend_path: Optional[str] = None, 
                          backend_path: Optional[str] = None) -> Tuple[Optional[str], Optional[str]]:
        """Automatically resolve frontend and backend paths"""
        
        # If both paths provided and exist, use them
        if frontend_path and backend_path:
            fe_exists, be_exists = self.check_local_paths(frontend_path, backend_path)
            if fe_exists and be_exists:
                return frontend_path, backend_path
        
        # Discover repositories
        discovered = self.discover_repositories()
        
        resolved_frontend = frontend_path
        resolved_backend = backend_path
        
        # Handle the case where we need to clone and look inside repositories
        if discovered['backend']:
            # Try to find the best backend repository (which might contain both frontend and backend)
            best_repo = self.select_best_repository(discovered['backend'], 'backend')
            if best_repo:
                cloned_path = self.clone_repository_smart(best_repo, 'repository')
                if cloned_path:
                    # Look for nested frontend and backend projects
                    nested_frontend, nested_backend = self.find_nested_projects(cloned_path)
                    
                    if nested_frontend and not resolved_frontend:
                        resolved_frontend = nested_frontend
                    if nested_backend and not resolved_backend:
                        resolved_backend = nested_backend
        
        # If we still don't have frontend, try frontend repositories
        if not resolved_frontend and discovered['frontend']:
            best_frontend = self.select_best_repository(discovered['frontend'], 'frontend')
            if best_frontend:
                cloned_path = self.clone_repository_smart(best_frontend, 'frontend')
                if cloned_path:
                    resolved_frontend = cloned_path
        
        # If we still don't have backend, try backend repositories
        if not resolved_backend and discovered['backend']:
            best_backend = self.select_best_repository(discovered['backend'], 'backend')
            if best_backend:
                cloned_path = self.clone_repository_smart(best_backend, 'backend')
                if cloned_path:
                    resolved_backend = cloned_path
        
        return resolved_frontend, resolved_backend
    
    def select_best_repository(self, repos: List[str], repo_type: str) -> Optional[str]:
        """Select the best repository from candidates"""
        if not repos:
            return None
        
        # Priority keywords for better matching
        priority_keywords = {
            'frontend': ['workflow', 'guided', 'main', 'primary'],
            'backend': ['workflow', 'guided', 'main', 'primary', 'api']
        }
        
        keywords = priority_keywords.get(repo_type, [])
        
        # Score repositories based on keywords
        scored_repos = []
        for repo in repos:
            score = 0
            repo_lower = repo.lower()
            
            for keyword in keywords:
                if keyword in repo_lower:
                    score += 1
            
            scored_repos.append((score, repo))
        
        # Sort by score (highest first)
        scored_repos.sort(key=lambda x: x[0], reverse=True)
        
        # If we have a clear winner, use it
        if scored_repos and scored_repos[0][0] > 0:
            selected = scored_repos[0][1]
            logger.info(f"Auto-selected {repo_type} repository: {selected}")
            return selected
        
        # Otherwise, let user choose
        logger.info(f"Multiple {repo_type} repositories found:")
        for i, repo in enumerate(repos, 1):
            logger.info(f"  {i}. {repo}")
        
        # For automation, just pick the first one
        selected = repos[0]
        logger.info(f"Auto-selecting first {repo_type} repository: {selected}")
        return selected
    
    def validate_repository_structure(self, path: str, repo_type: str) -> bool:
        """Validate that the repository has the expected structure"""
        path_obj = Path(path)
        
        if repo_type == 'frontend':
            # Check for typical frontend files
            indicators = [
                'package.json',
                'src',
                'tsconfig.json',
                'vite.config.ts',
                'webpack.config.js'
            ]
        else:  # backend
            # Check for typical backend files
            indicators = [
                'requirements.txt',
                'pyproject.toml',
                'api',
                'main.py',
                'app.py'
            ]
        
        found_indicators = []
        for indicator in indicators:
            if (path_obj / indicator).exists():
                found_indicators.append(indicator)
        
        is_valid = len(found_indicators) > 0
        logger.info(f"Repository validation for {path} ({repo_type}):")
        logger.info(f"  Found indicators: {found_indicators}")
        logger.info(f"  Valid: {'YES' if is_valid else 'NO'}")
        
        return is_valid
    
    def analyze(self, frontend_path: Optional[str] = None, 
                backend_path: Optional[str] = None,
                output_file: str = "smart_analysis_output.csv",
                auto_discover: bool = False) -> bool:
        """Main analysis method"""
        
        logger.info("Smart Frontend-Backend Analysis Starting...")
        logger.info("=" * 60)
        
        # Step 1: Resolve paths
        if auto_discover or not frontend_path or not backend_path:
            logger.info("Step 1: Auto-resolving repository paths...")
            frontend_path, backend_path = self.auto_resolve_paths(frontend_path, backend_path)
        else:
            logger.info("Step 1: Checking provided paths...")
            fe_exists, be_exists = self.check_local_paths(frontend_path, backend_path)
            if not fe_exists or not be_exists:
                logger.info("Some paths missing, attempting auto-resolution...")
                frontend_path, backend_path = self.auto_resolve_paths(frontend_path, backend_path)
        
        # Step 2: Validate paths
        if not frontend_path or not backend_path:
            logger.error("Could not resolve both frontend and backend paths")
            return False
        
        if not Path(frontend_path).exists() or not Path(backend_path).exists():
            logger.error("Resolved paths do not exist")
            return False
        
        # Step 3: Validate repository structure
        logger.info("Step 2: Validating repository structures...")
        fe_valid = self.validate_repository_structure(frontend_path, 'frontend')
        be_valid = self.validate_repository_structure(backend_path, 'backend')
        
        if not fe_valid:
            logger.warning(f"Frontend repository structure seems invalid: {frontend_path}")
        if not be_valid:
            logger.warning(f"Backend repository structure seems invalid: {backend_path}")
        
        # Step 4: Perform analysis
        logger.info("Step 3: Performing comprehensive analysis...")
        logger.info(f"  Frontend: {frontend_path}")
        logger.info(f"  Backend: {backend_path}")
        logger.info(f"  Output: {output_file}")
        
        try:
            generator = EnhancedCSVGenerator(frontend_path, backend_path)
            generator.generate_enhanced_csv(output_file)
            
            logger.info("SUCCESS: Analysis completed successfully!")
            logger.info(f"SUCCESS: Results saved to: {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Analysis failed: {e}")
            return False


def main():
    """Main function with comprehensive argument parsing"""
    parser = argparse.ArgumentParser(
        description="Smart Frontend-Backend Analyzer with Auto-Cloning",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use local paths if they exist, otherwise auto-discover and clone
  python smart_analyzer.py --frontend guided-workflow --backend guided-workflow-backend
  
  # Fully automatic discovery and analysis
  python smart_analyzer.py --auto-discover
  
  # Custom credentials and output
  python smart_analyzer.py --auto-discover --credentials my_creds.txt --output my_analysis.csv
  
  # Force re-cloning even if repos exist locally
  python smart_analyzer.py --auto-discover --force-clone
        """
    )
    
    parser.add_argument("--frontend", 
                       help="Frontend project path (local or repository name)")
    parser.add_argument("--backend", 
                       help="Backend project path (local or repository name)")
    parser.add_argument("--output", default="smart_analysis_output.csv",
                       help="Output CSV file (default: smart_analysis_output.csv)")
    parser.add_argument("--auto-discover", action="store_true",
                       help="Automatically discover repositories from CodeCommit")
    parser.add_argument("--credentials", default="credentials.txt",
                       help="AWS credentials file (default: credentials.txt)")
    parser.add_argument("--clone-dir", default="auto_cloned_repos",
                       help="Directory for cloned repositories (default: auto_cloned_repos)")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], 
                       default="INFO", help="Logging level")
    parser.add_argument("--force-clone", action="store_true",
                       help="Force re-cloning even if repositories exist locally")
    
    args = parser.parse_args()
    
    # Set logging level
    logger.setLevel(getattr(logging, args.log_level))
    
    # Create analyzer
    analyzer = SmartAnalyzer(
        credentials_file=args.credentials,
        clone_dir=args.clone_dir
    )
    
    # Handle force clone
    if args.force_clone and Path(args.clone_dir).exists():
        logger.info(f"Force clone requested, removing existing directory: {args.clone_dir}")
        shutil.rmtree(args.clone_dir)
    
    # Run analysis
    success = analyzer.analyze(
        frontend_path=args.frontend,
        backend_path=args.backend,
        output_file=args.output,
        auto_discover=args.auto_discover
    )
    
    if success:
        logger.info("🎉 Smart analysis completed successfully!")
        sys.exit(0)
    else:
        logger.error("❌ Smart analysis failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()