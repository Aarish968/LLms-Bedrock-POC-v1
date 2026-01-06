#!/usr/bin/env python3
"""
Simple wrapper for the Smart Analyzer
This provides the easiest way to run the analysis with sensible defaults.

Usage:
  python run_analysis.py                    # Auto-discover everything
  python run_analysis.py --local-only      # Only use local paths
  python run_analysis.py --help            # Show all options
"""

import sys
import os
from pathlib import Path
import argparse

# Add current directory to path so we can import our modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from smart_analyzer import SmartAnalyzer

def main():
    parser = argparse.ArgumentParser(
        description="Easy Frontend-Backend Analysis Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
🚀 QUICK START EXAMPLES:

1. FULLY AUTOMATIC (Recommended):
   python run_analysis.py
   
2. USE SPECIFIC LOCAL PATHS:
   python run_analysis.py --frontend path/to/frontend --backend path/to/backend
   
3. LOCAL ONLY (No cloning):
   python run_analysis.py --local-only --frontend path/to/frontend --backend path/to/backend
   
4. CUSTOM OUTPUT:
   python run_analysis.py --output my_results.csv

📁 The tool will:
   ✓ Auto-discover repositories from CodeCommit
   ✓ Clone repositories if needed
   ✓ Look inside cloned repos for frontend/backend projects
   ✓ Generate comprehensive CSV with table-column mappings
   ✓ Show detailed progress and results
        """
    )
    
    # Simple options
    parser.add_argument("--frontend", 
                       help="Frontend path (will auto-discover if not provided)")
    parser.add_argument("--backend", 
                       help="Backend path (will auto-discover if not provided)")
    parser.add_argument("--output", default="analysis_results.csv",
                       help="Output CSV file")
    parser.add_argument("--local-only", action="store_true",
                       help="Only use local paths, don't clone from CodeCommit")
    parser.add_argument("--verbose", action="store_true",
                       help="Show detailed logging")
    
    args = parser.parse_args()
    
    # Use provided paths or let smart analyzer auto-discover
    frontend_path = args.frontend
    backend_path = args.backend
    
    print("🔍 Smart Frontend-Backend Analyzer")
    print("=" * 50)
    
    if args.local_only:
        print("📁 LOCAL-ONLY MODE: Will only use existing local paths")
        
        if not frontend_path or not backend_path:
            print("ERROR: In local-only mode, you must specify both --frontend and --backend paths")
            return False
        
        # Check if paths exist
        fe_exists = Path(frontend_path).exists()
        be_exists = Path(backend_path).exists()
        
        if not fe_exists:
            print(f"ERROR: Frontend path not found: {frontend_path}")
            return False
            
        if not be_exists:
            print(f"ERROR: Backend path not found: {backend_path}")
            return False
            
        print(f"SUCCESS: Frontend found: {frontend_path}")
        print(f"SUCCESS: Backend found: {backend_path}")
        
        # Import and run the enhanced CSV generator directly
        from main import EnhancedCSVGenerator
        
        print(f"🔄 Analyzing...")
        generator = EnhancedCSVGenerator(frontend_path, backend_path)
        generator.generate_enhanced_csv(args.output)
        print(f"SUCCESS: Analysis complete! Results saved to: {args.output}")
        
    else:
        print("🤖 SMART MODE: Will auto-discover and clone if needed")
        
        # Create smart analyzer
        analyzer = SmartAnalyzer()
        
        # Set log level
        if args.verbose:
            import logging
            logging.getLogger().setLevel(logging.DEBUG)
        
        # Run smart analysis
        success = analyzer.analyze(
            frontend_path=frontend_path,
            backend_path=backend_path,
            output_file=args.output,
            auto_discover=True
        )
        
        if success:
            print(f"SUCCESS! Analysis complete!")
            print(f"Results saved to: {args.output}")
        else:
            print(f"FAILED! Check the logs above for details.")
            return False
    
    return True

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n⏹️  Analysis cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1)