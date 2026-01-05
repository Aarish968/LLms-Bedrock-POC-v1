#!/usr/bin/env python3
"""Installation script for Column Lineage API."""

import subprocess
import sys
import os

def run_command(command, description):
    """Run a command and handle errors."""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        return result
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed:")
        print(f"Command: {command}")
        print(f"Error: {e.stderr}")
        return None

def main():
    """Main installation function."""
    print("🚀 Installing Column Lineage API dependencies...")
    
    # Check if we're in a virtual environment
    if not os.environ.get('VIRTUAL_ENV'):
        print("⚠️  Warning: Not in a virtual environment. Consider activating one first.")
        response = input("Continue anyway? (y/N): ")
        if response.lower() != 'y':
            print("Installation cancelled.")
            return
    
    # Install dependencies using pip with pyproject.toml
    if not run_command("pip install -e .", "Installing main dependencies"):
        return
    
    # Install development dependencies if requested
    response = input("Install development dependencies? (y/N): ")
    if response.lower() == 'y':
        if not run_command("pip install -e .[dev]", "Installing development dependencies"):
            return
    
    # Install pre-commit hooks if available
    if os.path.exists('.pre-commit-config.yaml'):
        response = input("Install pre-commit hooks? (y/N): ")
        if response.lower() == 'y':
            run_command("pre-commit install", "Installing pre-commit hooks")
    
    print("\n🎉 Installation completed!")
    print("\n📋 Next steps:")
    print("1. Copy .env.example to .env and configure your settings")
    print("2. Run: python -m uvicorn api.main:app --reload")
    print("3. Visit: http://localhost:8000/docs")

if __name__ == "__main__":
    main()