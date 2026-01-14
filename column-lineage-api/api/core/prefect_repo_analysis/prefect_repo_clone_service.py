#!/usr/bin/env python3
"""
Script to clone only Prefect flow repositories from AWS CodeCommit.
Uses environment variables for AWS authentication.
"""

import os
import sys
import subprocess
import tempfile
import shutil
from pathlib import Path
from typing import List, Dict, Set
import re
from urllib.parse import quote
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from api.core.logging import LoggerMixin
from api.core.config import get_settings


class PrefectRepoCloner(LoggerMixin):
    def __init__(self):
        self.settings = get_settings()
        self.credentials = self._load_credentials_from_env()
        self.prefect_patterns = {
            'files': [
                'prefect.yaml',
                'prefect.dev.yaml', 
                'prefect.prod.yaml',
                'prefect.staging.yaml'
            ],
            'code_patterns': [
                r'from prefect import',
                r'import prefect',
                r'@flow\b',
                r'@task\b',
                r'deployment_tags',
                r'flow_service',
                r'FlowService',
                r'prefect\.flow',
                r'prefect\.task',
                r'common_prefect_next',
                r'entrypoint.*flow'
            ],
            'repo_name_patterns': [
                r'.*-flows$',
                r'.*-flow$',
                r'_flows$',
                r'_flow$',
                r'flow-.*',
                r'flow_.*',
                r'.*flows.*',
                r'prefect-.*',
                r'.*-prefect.*'
            ]
        }
        
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
    
    def get_all_repositories_via_boto3(self) -> List[Dict]:
        """Get repositories using boto3."""
        try:
            self.logger.info("Getting repository list via boto3...")
            
            # Initialize boto3 client for CodeCommit
            session = boto3.Session(region_name=self.credentials.get('region', 'us-east-1'))
            codecommit = session.client('codecommit')
            
            # List repositories
            response = codecommit.list_repositories()
            repositories = response.get('repositories', [])
            
            self.logger.info(f"Found {len(repositories)} total repositories")
            
            # Debug: Check for suspicious repository names
            for repo in repositories:
                repo_name = repo.get('repositoryName', 'UNKNOWN')
                if 'string' in repo_name.lower():
                    self.logger.warning(f"Found repository with 'string' in name: {repo}")
            
            return repositories
            
        except Exception as e:
            self.logger.error(f"Error getting repositories via boto3: {e}")
            self.logger.info("Falling back to AWS CLI...")
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
            self.logger.error("Make sure AWS CLI is installed and configured")
            return []
        except Exception as e:
            self.logger.error(f"Error parsing AWS CLI output: {e}")
            return []
    
    def filter_by_naming_convention(self, repositories: List[Dict]) -> Set[str]:
        """Filter repositories by naming patterns."""
        matching_repos = set()
        
        # Skip repositories with suspicious names
        suspicious_names = {'string', 'str', 'name', 'test', 'temp', 'tmp'}
        
        for repo in repositories:
            repo_name = repo['repositoryName']
            
            # Skip suspicious repository names
            if repo_name.lower() in suspicious_names:
                self.logger.warning(f"Skipping suspicious repository name: {repo_name}")
                continue
                
            for pattern in self.prefect_patterns['repo_name_patterns']:
                if re.match(pattern, repo_name, re.IGNORECASE):
                    matching_repos.add(repo_name)
                    self.logger.info(f"Found by naming pattern: {repo_name}")
                    break
        
        return matching_repos
    
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
    
    
    def shallow_clone_and_check(self, repo_name: str, region: str = None) -> bool:
        """Perform shallow clone and check for Prefect patterns."""
        if region is None:
            region = self.credentials.get('region', 'us-east-1')
            
        temp_dir = None
        try:
            temp_dir = tempfile.mkdtemp(prefix=f"prefect_check_{repo_name}_")
            clone_url = self.get_clone_url_with_auth(repo_name, region)
            
            self.logger.info(f"Checking repository: {repo_name}")
            self.logger.info(f"  Cloning (timeout: 30s)...")  # Reduced timeout for faster parallel processing
            
            # Use git command with proper error handling and shorter timeout
            cmd = ['git', 'clone', '--depth', '1', clone_url, temp_dir]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)  # Reduced from 60s
            
            if result.returncode != 0:
                self.logger.warning(f"  Could not clone {repo_name}: {result.stderr.strip()}")
                return False
            
            self.logger.info(f"  Clone successful, checking for Prefect patterns...")
            
            # Check for Prefect patterns
            if self._check_prefect_files(temp_dir):
                return True
            
            if self._check_prefect_code_patterns(temp_dir):
                return True
            
            self.logger.info(f"  No Prefect patterns found in {repo_name}")
            return False
            
        except subprocess.TimeoutExpired:
            self.logger.warning(f"  Timeout (30s) checking {repo_name} - skipping")
            return False
        except Exception as e:
            self.logger.error(f"  Error checking {repo_name}: {e}")
            return False
        finally:
            if temp_dir and os.path.exists(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)

    def _check_prefect_files(self, repo_path: str) -> bool:
        """Check for Prefect configuration files."""
        for file_pattern in self.prefect_patterns['files']:
            if list(Path(repo_path).rglob(file_pattern)):
                self.logger.info(f"  Found Prefect config file: {file_pattern}")
                return True
        return False
    
    def _check_prefect_code_patterns(self, repo_path: str) -> bool:
        """Check for Prefect code patterns in Python files."""
        python_files = list(Path(repo_path).rglob("*.py"))
        
        # Limit to first 50 Python files to avoid excessive checking
        for py_file in python_files[:50]:
            try:
                with open(py_file, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                
                for pattern in self.prefect_patterns['code_patterns']:
                    if re.search(pattern, content, re.MULTILINE | re.IGNORECASE):
                        self.logger.info(f"  Found Prefect pattern '{pattern}' in {py_file.name}")
                        return True
            except Exception:
                continue
        
        return False
    
    def _clone_single_repository(self, repo_name: str, target_dir: str, region: str = None) -> Dict[str, any]:
        """Clone a single repository (used for parallel execution)."""
        if region is None:
            region = self.credentials.get('region', 'us-east-1')
            
        result = {
            'repo_name': repo_name,
            'success': False,
            'action': 'none',
            'message': ''
        }
        
        try:
            os.makedirs(target_dir, exist_ok=True)
            repo_path = os.path.join(target_dir, repo_name)
            
            # Check if repository already exists locally
            if os.path.exists(repo_path):
                if self._is_git_repository(repo_path):
                    # Repository exists and is a valid git repo - pull latest changes
                    if self._pull_latest_changes(repo_path, repo_name):
                        result['success'] = True
                        result['action'] = 'updated'
                        result['message'] = 'Successfully updated existing repository'
                    else:
                        result['success'] = True  # Still consider success even if pull fails
                        result['action'] = 'existing'
                        result['message'] = 'Using existing repository (update failed)'
                    return result
                else:
                    # Directory exists but is not a git repository - remove and clone fresh
                    self.logger.warning(f"Directory {repo_path} exists but is not a git repository, removing...")
                    shutil.rmtree(repo_path)
            
            # Clone the repository
            self.logger.info(f"Cloning repository {repo_name}...")
            clone_url = self.get_clone_url_with_auth(repo_name, region)
            
            cmd = ['git', 'clone', clone_url, repo_path]
            clone_result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            
            if clone_result.returncode == 0:
                result['success'] = True
                result['action'] = 'cloned'
                result['message'] = 'Successfully cloned repository'
                self.logger.info(f"Successfully cloned {repo_name}")
            else:
                result['message'] = f"Clone failed: {clone_result.stderr.strip()}"
                self.logger.error(f"Error cloning {repo_name}: {clone_result.stderr.strip()}")
                
                # Clean up failed clone attempt
                if os.path.exists(repo_path):
                    shutil.rmtree(repo_path)
            
            return result
            
        except subprocess.TimeoutExpired:
            result['message'] = 'Clone timeout (300s)'
            self.logger.error(f"Timeout cloning {repo_name}")
            # Clean up failed clone attempt
            repo_path = os.path.join(target_dir, repo_name)
            if os.path.exists(repo_path):
                shutil.rmtree(repo_path)
            return result
        except Exception as e:
            result['message'] = f'Clone error: {str(e)}'
            self.logger.error(f"Error cloning {repo_name}: {e}")
            # Clean up failed clone attempt
            repo_path = os.path.join(target_dir, repo_name)
            if os.path.exists(repo_path):
                shutil.rmtree(repo_path)
            return result
    
    def clone_repositories_parallel(self, repo_names: List[str], target_dir: str = "prefect_repos", 
                                  region: str = None, max_workers: int = 5) -> Dict[str, any]:
        """Clone multiple repositories in parallel."""
        if region is None:
            region = self.credentials.get('region', 'us-east-1')
        
        self.logger.info(f"Starting parallel clone of {len(repo_names)} repositories with {max_workers} workers...")
        self.logger.info(f"Target directory: {target_dir}")
        self.logger.info(f"Repositories to clone: {repo_names}")
        
        results = {
            'total': len(repo_names),
            'successful': 0,
            'failed': 0,
            'updated': 0,
            'cloned': 0,
            'existing': 0,
            'details': []
        }
        
        # Use ThreadPoolExecutor for parallel cloning
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all clone tasks
            future_to_repo = {
                executor.submit(self._clone_single_repository, repo_name, target_dir, region): repo_name
                for repo_name in repo_names
            }
            
            # Process completed tasks
            for future in as_completed(future_to_repo):
                repo_name = future_to_repo[future]
                try:
                    result = future.result(timeout=10)  # 10 second timeout for getting result
                    results['details'].append(result)
                    
                    if result['success']:
                        results['successful'] += 1
                        if result['action'] == 'cloned':
                            results['cloned'] += 1
                        elif result['action'] == 'updated':
                            results['updated'] += 1
                        elif result['action'] == 'existing':
                            results['existing'] += 1
                    else:
                        results['failed'] += 1
                        
                    # Log progress
                    completed = results['successful'] + results['failed']
                    self.logger.info(f"Progress: {completed}/{results['total']} - {repo_name}: {result['action']}")
                    
                except Exception as e:
                    results['failed'] += 1
                    results['details'].append({
                        'repo_name': repo_name,
                        'success': False,
                        'action': 'error',
                        'message': f'Future execution error: {str(e)}'
                    })
                    self.logger.error(f"Error processing {repo_name}: {e}")
        
        # Log summary
        self.logger.info(f"Parallel clone completed:")
        self.logger.info(f"  Total repositories: {results['total']}")
        self.logger.info(f"  Successful: {results['successful']}")
        self.logger.info(f"  Failed: {results['failed']}")
        self.logger.info(f"  Newly cloned: {results['cloned']}")
        self.logger.info(f"  Updated existing: {results['updated']}")
        self.logger.info(f"  Used existing: {results['existing']}")
        
        # Ensure results structure is valid before returning
        if not results.get('details'):
            self.logger.warning("No details in results, creating empty list")
            results['details'] = []
        
        return results
        
    def check_repositories_for_prefect_parallel(self, repo_names: List[str], region: str = None, max_workers: int = 8) -> Set[str]:
        """Check multiple repositories for Prefect patterns in parallel."""
        if region is None:
            region = self.credentials.get('region', 'us-east-1')
        
        self.logger.info(f"Checking {len(repo_names)} repositories for Prefect patterns in parallel with {max_workers} workers...")
        
        prefect_repos = set()
        
        # Use ThreadPoolExecutor for parallel checking
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all check tasks
            future_to_repo = {
                executor.submit(self.shallow_clone_and_check, repo_name, region): repo_name
                for repo_name in repo_names
            }
            
            # Process completed tasks
            completed = 0
            for future in as_completed(future_to_repo):
                repo_name = future_to_repo[future]
                completed += 1
                try:
                    is_prefect_repo = future.result(timeout=5)  # 5 second timeout for getting result
                    if is_prefect_repo:
                        prefect_repos.add(repo_name)
                        self.logger.info(f"[{completed}/{len(repo_names)}] ✓ {repo_name} contains Prefect flows")
                    else:
                        self.logger.info(f"[{completed}/{len(repo_names)}] ✗ {repo_name} - no Prefect patterns")
                        
                except Exception as e:
                    self.logger.error(f"[{completed}/{len(repo_names)}] ✗ {repo_name} - error: {e}")
        
        self.logger.info(f"Parallel Prefect pattern check completed: {len(prefect_repos)} repositories contain Prefect flows")
        return prefect_repos
    
    def check_cloned_repositories_for_prefect(self, repo_names: List[str], target_dir: str) -> Set[str]:
        """Check already-cloned repositories for Prefect patterns (much faster than cloning)."""
        self.logger.info(f"Checking {len(repo_names)} cloned repositories for Prefect patterns...")
        self.logger.info(f"Target directory: {target_dir}")
        self.logger.info(f"Repositories to check: {repo_names}")
        
        if not repo_names:
            self.logger.warning("No repository names provided for checking")
            return set()
        
        prefect_repos = set()
        
        for i, repo_name in enumerate(repo_names, 1):
            try:
                self.logger.info(f"[{i}/{len(repo_names)}] Checking {repo_name}")
                repo_path = os.path.join(target_dir, repo_name)
                
                if not os.path.exists(repo_path):
                    self.logger.warning(f"✗ {repo_name} - Repository path does not exist: {repo_path}")
                    continue
                
                if not self._is_git_repository(repo_path):
                    self.logger.warning(f"✗ {repo_name} - Not a valid git repository: {repo_path}")
                    continue
                
                # Check for Prefect patterns in the cloned repository
                if self._check_prefect_files(repo_path):
                    prefect_repos.add(repo_name)
                    self.logger.info(f"✓ {repo_name} - Found Prefect config files")
                    continue
                
                if self._check_prefect_code_patterns(repo_path):
                    prefect_repos.add(repo_name)
                    self.logger.info(f"✓ {repo_name} - Found Prefect code patterns")
                    continue
                
                self.logger.info(f"✗ {repo_name} - No Prefect patterns found")
                
            except Exception as e:
                self.logger.error(f"✗ {repo_name} - Error checking: {e}")
                import traceback
                self.logger.error(f"Traceback: {traceback.format_exc()}")
        
        self.logger.info(f"Found {len(prefect_repos)} repositories with Prefect patterns out of {len(repo_names)} checked")
        return prefect_repos
    
    def clone_repository(self, repo_name: str, target_dir: str = "prefect_repos", region: str = None):
        """Clone a repository to the target directory."""
        if region is None:
            region = self.credentials.get('region', 'us-east-1')
            
        try:
            os.makedirs(target_dir, exist_ok=True)
            repo_path = os.path.join(target_dir, repo_name)
            
            if os.path.exists(repo_path):
                self.logger.info(f"Repository {repo_name} already exists, pulling latest changes...")
                try:
                    # Try to pull latest changes
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
    
    def run(self, target_dir: str = "prefect_repos", skip_naming_check: bool = False, 
            region: str = None, repo_list: List[str] = None, max_workers: int = 5):
        """Main execution method."""
        if region is None:
            region = self.credentials.get('region', 'us-east-1')
            
        self.logger.info("Starting Prefect repository discovery and cloning...")
        
        self.setup_aws_credentials()
        
        if repo_list:
            all_repos = [{'repositoryName': name} for name in repo_list]
            self.logger.info(f"Using provided repository list: {len(all_repos)} repositories")
        else:
            # Try boto3 first, then fall back to CLI
            all_repos = self.get_all_repositories_via_boto3()
            if not all_repos:
                self.logger.error("No repositories found. Please check your AWS configuration.")
                return
        
        prefect_repos = set()
        
        # Step 1: Filter by naming convention
        if not skip_naming_check:
            self.logger.info("\nStep 1: Filtering by naming conventions...")
            prefect_repos.update(self.filter_by_naming_convention(all_repos))
        
        # Step 2: Check remaining repositories by content in parallel
        self.logger.info("\nStep 2: Checking repositories for Prefect patterns in parallel...")
        
        remaining_repos = [r for r in all_repos if r['repositoryName'] not in prefect_repos]
        
        if remaining_repos:
            # Filter out suspicious names before checking
            remaining_repo_names = []
            suspicious_names = {'string', 'str', 'name', 'test', 'temp', 'tmp'}
            
            for repo in remaining_repos:
                repo_name = repo['repositoryName']
                if repo_name.lower() not in suspicious_names:
                    remaining_repo_names.append(repo_name)
                else:
                    self.logger.warning(f"Skipping suspicious repository name: {repo_name}")
            
            if remaining_repo_names:
                # Use parallel checking
                check_workers = min(max_workers, 10)  # Use same workers as cloning, but cap at 10
                additional_prefect_repos = self.check_repositories_for_prefect_parallel(
                    remaining_repo_names, 
                    region=region,
                    max_workers=check_workers
                )
                prefect_repos.update(additional_prefect_repos)
        
        # Step 3: Clone identified Prefect repositories in parallel
        if prefect_repos:
            self.logger.info(f"\nStep 3: Cloning {len(prefect_repos)} Prefect repositories in parallel...")
            
            # Use the provided max_workers, but cap it at 10 for safety
            effective_max_workers = min(max_workers, 10, max(1, len(prefect_repos)))
            
            clone_results = self.clone_repositories_parallel(
                list(prefect_repos), 
                target_dir, 
                region, 
                max_workers=effective_max_workers
            )
            
            successful_clones = clone_results['successful']
        else:
            self.logger.info(f"\nStep 3: No Prefect repositories found to clone")
            successful_clones = 0
            clone_results = {
                'total': 0,
                'successful': 0,
                'failed': 0,
                'updated': 0,
                'cloned': 0,
                'existing': 0,
                'details': []
            }
        
        # Summary
        self.logger.info(f"\nSummary:")
        self.logger.info(f"  Total repositories checked: {len(all_repos)}")
        self.logger.info(f"  Prefect repositories found: {len(prefect_repos)}")
        self.logger.info(f"  Successfully processed: {successful_clones}")
        if clone_results['total'] > 0:
            self.logger.info(f"  Newly cloned: {clone_results['cloned']}")
            self.logger.info(f"  Updated existing: {clone_results['updated']}")
            self.logger.info(f"  Used existing: {clone_results['existing']}")
            self.logger.info(f"  Failed: {clone_results['failed']}")
        self.logger.info(f"  Target directory: {os.path.abspath(target_dir)}")
        
        if prefect_repos:
            self.logger.info(f"\nPrefect repositories:")
            for repo in sorted(prefect_repos):
                # Find the result for this repo to show its status
                repo_result = next((r for r in clone_results['details'] if r['repo_name'] == repo), None)
                status = repo_result['action'] if repo_result else 'unknown'
                self.logger.info(f"  - {repo} ({status})")
        
        # Save list of Prefect repositories for future reference
        prefect_list_file = os.path.join(target_dir, "prefect_repositories.txt")
        try:
            with open(prefect_list_file, 'w') as f:
                for repo in sorted(prefect_repos):
                    f.write(f"{repo}\n")
            self.logger.info(f"\nPrefect repository list saved to: {prefect_list_file}")
        except Exception as e:
            self.logger.error(f"Could not save repository list: {e}")
        
        return clone_results


def main():
    """Main function with command line argument support."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Clone Prefect flow repositories from AWS CodeCommit")
    parser.add_argument("--target-dir", default="prefect_repos",
                       help="Target directory for cloned repositories (default: prefect_repos)")
    parser.add_argument("--region", default=None,
                       help="AWS region (default: from environment variables)")
    parser.add_argument("--skip-naming-check", action="store_true",
                       help="Skip naming convention filtering (check all repos)")
    parser.add_argument("--repo-list", nargs="+",
                       help="Specific repository names to check (space-separated)")
    parser.add_argument("--max-workers", type=int, default=5,
                       help="Maximum number of parallel workers for cloning (default: 5)")
    parser.add_argument("--dry-run", action="store_true",
                       help="Only identify repositories, don't clone them")
    
    args = parser.parse_args()
    
    cloner = PrefectRepoCloner()
    
    if args.dry_run:
        print("DRY RUN MODE - Only identifying repositories...")
        # In dry run mode, we'll just identify repos without cloning
        original_clone_parallel = cloner.clone_repositories_parallel
        def dry_run_clone_parallel(repo_names, target_dir, region, max_workers):
            print(f"[DRY RUN] Would clone {len(repo_names)} repositories in parallel:")
            for repo in repo_names:
                print(f"  - {repo}")
            return {
                'total': len(repo_names),
                'successful': len(repo_names),
                'failed': 0,
                'updated': 0,
                'cloned': len(repo_names),
                'existing': 0,
                'details': [{'repo_name': repo, 'success': True, 'action': 'dry_run', 'message': 'Dry run'} for repo in repo_names]
            }
        cloner.clone_repositories_parallel = dry_run_clone_parallel
    
    # Add max_workers to the run method call
    result = cloner.run(
        target_dir=args.target_dir, 
        skip_naming_check=args.skip_naming_check,
        region=args.region,
        repo_list=args.repo_list,
        max_workers=args.max_workers
    )


if __name__ == "__main__":
    main()