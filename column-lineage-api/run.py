#!/usr/bin/env python3
"""Run script for Column Lineage API."""

import os
import sys
import subprocess
from pathlib import Path

def check_env_file():
    """Check if .env file exists."""
    if not os.path.exists('.env'):
        print("⚠️  .env file not found!")
        if os.path.exists('.env.example'):
            response = input("Copy .env.example to .env? (y/N): ")
            if response.lower() == 'y':
                import shutil
                shutil.copy('.env.example', '.env')
                print("✅ .env file created. Please edit it with your configuration.")
                return False
        else:
            print("❌ No .env.example file found. Please create a .env file manually.")
            return False
    return True

def main():
    """Main run function."""
    print("🚀 Starting Column Lineage API...")
    
    # Add current directory to Python path
    current_dir = Path(__file__).parent.absolute()
    sys.path.insert(0, str(current_dir))
    
    # Check environment file
    if not check_env_file():
        print("Please configure your .env file and try again.")
        return
    
    # Set environment variables
    os.environ.setdefault('PYTHONPATH', str(current_dir))
    
    # Run the application
    try:
        cmd = [
            sys.executable, "-m", "uvicorn", 
            "api.main:app", 
            "--host", "0.0.0.0", 
            "--port", "8000", 
            "--reload"
        ]
        
        print("🌐 Starting server at http://localhost:8000")
        print("📚 API docs available at http://localhost:8000/docs")
        print("🔍 Health check at http://localhost:8000/health")
        print("\nPress Ctrl+C to stop the server")
        
        subprocess.run(cmd)
        
    except KeyboardInterrupt:
        print("\n👋 Server stopped.")
    except Exception as e:
        print(f"❌ Error starting server: {e}")

if __name__ == "__main__":
    main()