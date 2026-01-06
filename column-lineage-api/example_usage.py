#!/usr/bin/env python3
"""
Example usage of the Repository Analysis API.
This script demonstrates how to use the API to analyze repositories.
"""

import requests
import time
import json
from typing import Dict, Any


def start_analysis(base_url: str = "http://localhost:8000") -> Dict[str, Any]:
    """Start a repository analysis job."""
    print("Starting repository analysis...")
    
    response = requests.post(
        f"{base_url}/api/v1/repo-analysis/public/analyze",
        json={
            "frontend_repo_name": "guided-workflow",
            "backend_repo_name": "guided-workflow-backend",
            "output_filename": "example_analysis.csv"
        }
    )
    
    if response.status_code == 200:
        data = response.json()
        print(f"✅ Analysis started successfully!")
        print(f"   Job ID: {data['job_id']}")
        print(f"   Status: {data['status']}")
        print(f"   Message: {data['message']}")
        return data
    else:
        print(f"❌ Failed to start analysis: {response.status_code}")
        print(f"   Error: {response.text}")
        return {}


def check_status(job_id: str, base_url: str = "http://localhost:8000") -> Dict[str, Any]:
    """Check the status of an analysis job."""
    response = requests.get(f"{base_url}/api/v1/repo-analysis/public/status/{job_id}")
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"❌ Failed to get status: {response.status_code}")
        return {}


def wait_for_completion(job_id: str, base_url: str = "http://localhost:8000", timeout: int = 600) -> Dict[str, Any]:
    """Wait for analysis to complete."""
    print(f"Waiting for analysis to complete (timeout: {timeout}s)...")
    
    start_time = time.time()
    while time.time() - start_time < timeout:
        status_data = check_status(job_id, base_url)
        
        if not status_data:
            break
        
        current_status = status_data.get('status', 'unknown')
        print(f"   Status: {current_status} - {status_data.get('message', '')}")
        
        if current_status in ["completed", "failed", "cancelled"]:
            return status_data
        
        time.sleep(10)  # Wait 10 seconds before checking again
    
    print("⚠️  Timeout waiting for completion")
    return {}


def list_jobs(base_url: str = "http://localhost:8000") -> None:
    """List all analysis jobs."""
    print("Listing all analysis jobs...")
    
    response = requests.get(f"{base_url}/api/v1/repo-analysis/public/jobs?limit=10")
    
    if response.status_code == 200:
        jobs = response.json()
        print(f"Found {len(jobs)} jobs:")
        
        for job in jobs:
            print(f"  Job {job['job_id'][:8]}...")
            print(f"    Status: {job['status']}")
            print(f"    Started: {job['started_at']}")
            if job.get('completed_at'):
                print(f"    Completed: {job['completed_at']}")
            if job.get('output_file'):
                print(f"    Output: {job['output_file']}")
            print()
    else:
        print(f"❌ Failed to list jobs: {response.status_code}")


def main():
    """Main example function."""
    print("Repository Analysis API Example")
    print("=" * 40)
    
    base_url = "http://localhost:8000"
    
    # Check if API is running
    try:
        response = requests.get(f"{base_url}/health")
        if response.status_code != 200:
            print(f"❌ API not responding at {base_url}")
            print("   Make sure the API server is running")
            return
    except requests.exceptions.ConnectionError:
        print(f"❌ Cannot connect to API at {base_url}")
        print("   Make sure the API server is running")
        return
    
    print(f"✅ API is running at {base_url}")
    
    # Start analysis
    job_data = start_analysis(base_url)
    if not job_data:
        return
    
    job_id = job_data.get('job_id')
    if not job_id:
        print("❌ No job ID returned")
        return
    
    # Wait for completion
    final_status = wait_for_completion(job_id, base_url)
    
    if final_status:
        status = final_status.get('status')
        if status == 'completed':
            print("🎉 Analysis completed successfully!")
            output_file = final_status.get('output_file')
            if output_file:
                print(f"   Output file: {output_file}")
                print(f"   Frontend repo: {final_status.get('frontend_repo_name')}")
                print(f"   Backend repo: {final_status.get('backend_repo_name')}")
        elif status == 'failed':
            print("❌ Analysis failed")
            error_msg = final_status.get('error_message')
            if error_msg:
                print(f"   Error: {error_msg}")
        else:
            print(f"⚠️  Analysis ended with status: {status}")
    
    # List all jobs
    print("\n" + "=" * 40)
    list_jobs(base_url)


if __name__ == "__main__":
    main()