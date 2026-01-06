#!/usr/bin/env python3
"""
Smart Frontend-Backend Analyzer with Auto-Cloning
This script intelligently handles repository discovery and analysis.
"""

import os
import sys
import logging
from pathlib import Path
from typing import Optional, Tuple, List, Dict

# Import existing functionality
from main import EnhancedCSVGenerator
from api.v1.services.repository_cloning_service import RepositoryCloningService, RepositoryDiscoveryService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SmartAnalyzer:
    """Smart analyzer that handles repository discovery, cloning, and analysis"""
    
    def __init__(self, credentials_file: str = "credentials.txt", 
                 clone_dir: str = "cloned-repo"):
        self.credentials_file = credentials_file
        self.clone_dir = clone_dir
        self.cloning_service = RepositoryCloningService(credentials_file)
        self.discovery_service = RepositoryDiscoveryService(self.cloning_service)
    
    def analyze(self, frontend_path: Optional[str] = None, 
                backend_path: Optional[str] = None,
                output_file: str = "smart_analysis_output.csv",
                auto_discover: bool = False) -> bool:
        """Main analysis method"""
        
        logger.info("Smart Frontend-Backend Analysis Starting...")
        
        try:
            # Use provided paths or defaults
            if not frontend_path:
                frontend_path = os.path.join(self.clone_dir, "frontend")
            if not backend_path:
                backend_path = os.path.join(self.clone_dir, "backend")
            
            # Check if paths exist
            if not Path(frontend_path).exists() or not Path(backend_path).exists():
                logger.error("Frontend or backend paths do not exist")
                return False
            
            # Perform analysis
            logger.info(f"Analyzing: {frontend_path} and {backend_path}")
            generator = EnhancedCSVGenerator(frontend_path, backend_path)
            generator.generate_enhanced_csv(output_file)
            
            logger.info("SUCCESS: Analysis completed successfully!")
            return True
            
        except Exception as e:
            logger.error(f"Analysis failed: {e}")
            return False


def main():
    """Main function"""
    analyzer = SmartAnalyzer()
    success = analyzer.analyze()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()