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


class RepositoryCloningService(LoggerMixin):
    """Service for cloning repositories from AWS CodeCommit."""
    
    def __init__(self, credentials_file: str = "credentials.txt"):
        self.credentials_file = credentials_file
        self.credentials = {}
        
    def _load_credentials(self) -> Dict[str, str]:
        """Load AWS CodeCommit credentials from file."""
        try:
            credentials = {}
            with open(self.credentials_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip().strip('"')
                        value = value.strip().strip('"')
                        credentials[key.lower()] = value
            
            self.logger.info(f"Loaded credentials from {self.credentials_file}")
            return credentials
        except FileNotFoundError:
            self.logger.error(f"Credentials file {self.credentials_file} not found!")
            return {}
        except Exception as e:
            self.logger.error(f"Error loading credentials: {e}")
            return {}
    
    def setup_aws_credentials(self):
        """Setup AWS credentials from credentials.txt file."""
        self.credentials = self._load_credentials()
        
        if not self.credentials.get('username') or not self.credentials.get('password'):
            self.logger.error("Username or Password not found in credentials file")
            raise ValueError("Invalid credentials")
        
        self.logger.info("Git credentials loaded successfully")
    
    def get_clone_url_with_auth(self, repo_name: str, region: str = "us-east-1") -> str:
        """Get the clone URL with properly encoded authentication."""
        username = self.credentials.get('username', '')
        password = self.credentials.get('password', '')
        
        # Handle special characters in username and password
        encoded_username = quote(username, safe='')
        encoded_password = quote(password, safe='')
        
        clone_url_https = f"https://git-codecommit.{region}.amazonaws.com/v1/repos/{repo_name}"
        auth_url = clone_url_https.replace('https://', f'https://{encoded_username}:{encoded_password}@')
        
        return auth_url
    
    def clone_repository(self, repo_name: str, target_dir: str = "cloned_repos", region: str = "us-east-1") -> bool:
        """Clone a repository to the target directory."""
        try:
            os.makedirs(target_dir, exist_ok=True)
            repo_path = os.path.join(target_dir, repo_name)
            
            if os.path.exists(repo_path):
                self.logger.info(f"Repository {repo_name} already exists, pulling latest changes...")
                try:
                    result = subprocess.run(
                        ['git', 'pull'],
                        cwd=repo_path,
                        capture_output=True,
                        text=True,
                        timeout=120
                    )
                    if result.returncode == 0:
                        self.logger.info(f"Successfully updated {repo_name}")
                    else:
                        self.logger.warning(f"Could not update {repo_name}, using existing version")
                    return True
                except Exception as e:
                    self.logger.warning(f"Could not update {repo_name}: {e}, using existing version")
                    return True
            
            self.logger.info(f"Cloning {repo_name}...")
            clone_url = self.get_clone_url_with_auth(repo_name, region)
            
            cmd = ['git', 'clone', clone_url, repo_path]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            
            if result.returncode == 0:
                self.logger.info(f"Successfully cloned {repo_name}")
                return True
            else:
                self.logger.error(f"Error cloning {repo_name}: {result.stderr.strip()}")
                return False
            
        except subprocess.TimeoutExpired:
            self.logger.error(f"Timeout cloning {repo_name}")
            return False
        except Exception as e:
            self.logger.error(f"Error cloning {repo_name}: {e}")
            return False
    
    def clone_if_missing(self, path: str, credentials_file: str) -> bool:
        """Clone repository if the path doesn't exist locally."""
        if Path(path).exists():
            self.logger.info(f"Path already exists: {path}")
            return True
        
        # Update credentials file
        self.credentials_file = credentials_file
        self.setup_aws_credentials()
        
        # Extract repository name from path
        repo_name = Path(path).name
        target_dir = str(Path(path).parent)
        
        return self.clone_repository(repo_name, target_dir)


class RepositoryDiscoveryService(LoggerMixin):
    """Service for discovering repositories from AWS CodeCommit."""
    
    def __init__(self, cloning_service: RepositoryCloningService):
        self.cloning_service = cloning_service
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
            
            session = boto3.Session(region_name='us-east-1')
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