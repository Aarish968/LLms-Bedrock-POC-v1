"""Repository cloning and discovery services."""

import os
import sys
import subprocess
import tempfile
import shutil
from pathlib import Path
from typing import List, Optional, Dict, Set
import re
from urllib.parse import quote
import boto3

from api.core.logging import LoggerMixin
from api.core.config import get_settings


class RepositoryCloningService(LoggerMixin):
    """Service for cloning repositories from AWS CodeCommit."""
    
    def __init__(self):
        self.settings = get_settings()
        self.credentials = self._load_credentials_from_env()
        
    def _load_credentials_from_env(self) -> Dict[str, str]:
        """Load AWS CodeCommit credentials from environment variables."""
        credentials = {
            'username': self.settings.AWS_CODECOMMIT_USERNAME,
            'password': self.settings.AWS_CODECOMMIT_PASSWORD,
            'region': self.settings.AWS_CODECOMMIT_REGION
        }
        
        if not credentials['username'] or not credentials['password']:
            self.logger.error("AWS CodeCommit credentials not found in environment variables")
            self.logger.error("Please set AWS_CODECOMMIT_USERNAME and AWS_CODECOMMIT_PASSWORD")
            raise ValueError("Missing AWS CodeCommit credentials in environment variables")
        
        self.logger.info("AWS CodeCommit credentials loaded from environment variables")
        return credentials
    
    def setup_aws_credentials(self):
        """Setup AWS credentials from environment variables."""
        # Credentials are already loaded in __init__
        self.logger.info("AWS CodeCommit credentials ready")
    
    def get_clone_url_with_auth(self, repo_name: str, region: str = None) -> str:
        """Get the clone URL with properly encoded authentication."""
        if region is None:
            region = self.credentials.get('region', 'us-east-1')
            
        username = self.credentials.get('username', '')
        password = self.credentials.get('password', '')
        
        # Handle special characters in username and password
        encoded_username = quote(username, safe='')
        encoded_password = quote(password, safe='')
        
        clone_url_https = f"https://git-codecommit.{region}.amazonaws.com/v1/repos/{repo_name}"
        auth_url = clone_url_https.replace('https://', f'https://{encoded_username}:{encoded_password}@')
        
        return auth_url
    
    def _is_git_repository(self, path: str) -> bool:
        """Check if a directory is a valid git repository."""
        try:
            result = subprocess.run(
                ['git', 'rev-parse', '--git-dir'],
                cwd=path,
                capture_output=True,
                text=True,
                timeout=10
            )
            return result.returncode == 0
        except Exception:
            return False
    
    def _pull_latest_changes(self, repo_path: str, repo_name: str) -> bool:
        """Pull latest changes from the remote repository."""
        try:
            self.logger.info(f"Repository {repo_name} exists locally, pulling latest changes...")
            
            # First, fetch the latest changes
            fetch_result = subprocess.run(
                ['git', 'fetch', 'origin'],
                cwd=repo_path,
                capture_output=True,
                text=True,
                timeout=120
            )
            
            if fetch_result.returncode != 0:
                self.logger.warning(f"Failed to fetch from origin for {repo_name}: {fetch_result.stderr}")
                return False
            
            # Check if we're on a branch and get current branch name
            branch_result = subprocess.run(
                ['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
                cwd=repo_path,
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if branch_result.returncode != 0:
                self.logger.warning(f"Could not determine current branch for {repo_name}")
                current_branch = "main"  # Default fallback
            else:
                current_branch = branch_result.stdout.strip()
            
            # Pull the latest changes
            pull_result = subprocess.run(
                ['git', 'pull', 'origin', current_branch],
                cwd=repo_path,
                capture_output=True,
                text=True,
                timeout=120
            )
            
            if pull_result.returncode == 0:
                self.logger.info(f"Successfully updated {repo_name} from branch '{current_branch}'")
                return True
            else:
                self.logger.warning(f"Failed to pull latest changes for {repo_name}: {pull_result.stderr}")
                # Even if pull fails, we can still use the existing repository
                self.logger.info(f"Using existing version of {repo_name}")
                return True
                
        except subprocess.TimeoutExpired:
            self.logger.warning(f"Timeout while updating {repo_name}, using existing version")
            return True
        except Exception as e:
            self.logger.warning(f"Error updating {repo_name}: {e}, using existing version")
            return True
    
    def clone_repository(self, repo_name: str, target_dir: str = "cloned_repos", region: str = None) -> bool:
        """Clone a repository to the target directory or pull latest if it exists."""
        try:
            # Ensure target directory exists
            os.makedirs(target_dir, exist_ok=True)
            repo_path = os.path.join(target_dir, repo_name)
            
            # Check if repository already exists locally
            if os.path.exists(repo_path):
                if self._is_git_repository(repo_path):
                    # Repository exists and is a valid git repo - pull latest changes
                    return self._pull_latest_changes(repo_path, repo_name)
                else:
                    # Directory exists but is not a git repository - remove and clone fresh
                    self.logger.warning(f"Directory {repo_path} exists but is not a git repository, removing...")
                    shutil.rmtree(repo_path)
            
            # Clone the repository
            self.logger.info(f"Cloning repository {repo_name} to {repo_path}...")
            clone_url = self.get_clone_url_with_auth(repo_name, region)
            
            cmd = ['git', 'clone', clone_url, repo_path]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            
            if result.returncode == 0:
                self.logger.info(f"Successfully cloned {repo_name}")
                return True
            else:
                error_msg = result.stderr.strip()
                self.logger.error(f"Error cloning {repo_name}: {error_msg}")
                
                # Clean up failed clone attempt
                if os.path.exists(repo_path):
                    shutil.rmtree(repo_path)
                
                return False
            
        except subprocess.TimeoutExpired:
            self.logger.error(f"Timeout cloning {repo_name}")
            # Clean up failed clone attempt
            repo_path = os.path.join(target_dir, repo_name)
            if os.path.exists(repo_path):
                shutil.rmtree(repo_path)
            return False
        except Exception as e:
            self.logger.error(f"Error cloning {repo_name}: {e}")
            # Clean up failed clone attempt
            repo_path = os.path.join(target_dir, repo_name)
            if os.path.exists(repo_path):
                shutil.rmtree(repo_path)
            return False
    
    def clone_if_missing(self, path: str) -> bool:
        """Clone repository if the path doesn't exist locally, otherwise pull latest."""
        if Path(path).exists():
            if self._is_git_repository(path):
                # Repository exists - pull latest changes
                repo_name = Path(path).name
                return self._pull_latest_changes(path, repo_name)
            else:
                self.logger.warning(f"Path {path} exists but is not a git repository")
                return False
        
        # Extract repository name and target directory from path
        repo_name = Path(path).name
        target_dir = str(Path(path).parent)
        
        return self.clone_repository(repo_name, target_dir)


class RepositoryDiscoveryService(LoggerMixin):
    """Service for discovering repositories from AWS CodeCommit."""
    
    def __init__(self, cloning_service: RepositoryCloningService):
        self.cloning_service = cloning_service
        self.settings = get_settings()
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
            r'guided-workflow-backend$',
            r'.*-workflow-backend$',
            r'action-to-table.*'
        ]
    
    def get_all_repositories_via_boto3(self) -> List[Dict]:
        """Get repositories using boto3."""
        try:
            self.logger.info("Getting repository list via boto3...")
            
            session = boto3.Session(region_name=self.settings.AWS_CODECOMMIT_REGION)
            codecommit = session.client('codecommit')
            
            response = codecommit.list_repositories()
            repositories = response.get('repositories', [])
            
            self.logger.info(f"Found {len(repositories)} total repositories")
            return repositories
            
        except Exception as e:
            self.logger.error(f"Error getting repositories via boto3: {e}")
            return self.get_all_repositories_via_cli()
    
    def get_all_repositories_via_cli(self) -> List[Dict]:
        """Get repositories using AWS CLI (fallback method)."""
        try:
            self.logger.info("Getting repository list via AWS CLI...")
            result = subprocess.run(['aws', 'codecommit', 'list-repositories'], 
                                  capture_output=True, text=True, check=True)
            import json
            data = json.loads(result.stdout)
            repositories = data.get('repositories', [])
            self.logger.info(f"Found {len(repositories)} total repositories")
            return repositories
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Error running AWS CLI: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Error parsing AWS CLI output: {e}")
            return []
    
    def discover_repositories(self) -> Dict[str, List[str]]:
        """Discover frontend and backend repositories from CodeCommit."""
        self.logger.info("Discovering repositories from AWS CodeCommit...")
        
        # Setup credentials
        self.cloning_service.setup_aws_credentials()
        
        # Get all repositories
        all_repos = self.get_all_repositories_via_boto3()
        if not all_repos:
            self.logger.error("Could not retrieve repository list")
            return {'frontend': [], 'backend': []}
        
        frontend_repos = []
        backend_repos = []
        
        # Categorize repositories
        for repo in all_repos:
            repo_name = repo['repositoryName']
            
            # Check frontend patterns
            for pattern in self.frontend_patterns:
                if re.match(pattern, repo_name, re.IGNORECASE):
                    frontend_repos.append(repo_name)
                    break
            
            # Check backend patterns  
            for pattern in self.backend_patterns:
                if re.match(pattern, repo_name, re.IGNORECASE):
                    backend_repos.append(repo_name)
                    break
        
        self.logger.info(f"Discovered repositories:")
        self.logger.info(f"  Frontend candidates: {len(frontend_repos)}")
        self.logger.info(f"  Backend candidates: {len(backend_repos)}")
        
        return {
            'frontend': frontend_repos,
            'backend': backend_repos
        }