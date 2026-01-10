"""Direct analysis runner - alternative to subprocess approach."""

import sys
import os
from pathlib import Path
from uuid import UUID
from api.core.logging import get_logger

logger = get_logger(__name__)

class DirectAnalysisRunner:
    """Run analysis directly by importing and calling functions."""
    
    def __init__(self):
        # Add the action_to_endpoint_analysis directory to Python path
        action_to_endpoint_analysis_path = Path(__file__).parent.parent.parent / "core" / "action_to_endpoint_analysis"
        if str(action_to_endpoint_analysis_path) not in sys.path:
            sys.path.insert(0, str(action_to_endpoint_analysis_path))
    
    async def run_analysis(
        self, 
        frontend_path: str, 
        backend_path: str, 
        output_file: str, 
        job_id: UUID
    ) -> bool:
        """Run analysis directly by importing the main module."""
        try:
            logger.info(f"Running analysis directly (no subprocess)", job_id=str(job_id))
            
            # Import the main module
            from main import EnhancedCSVGenerator
            
            # Parse output file
            output_path = Path(output_file)
            output_base = output_path.stem
            
            # Create generator and run analysis
            generator = EnhancedCSVGenerator(frontend_path, backend_path)
            generator.generate_enhanced_csv(output_base)
            
            # Check if file was created
            possible_locations = [
                Path(output_base),
                Path(f"{output_base}.csv"),
                Path.cwd() / output_base,
                Path.cwd() / f"{output_base}.csv",
            ]
            
            csv_path = None
            for location in possible_locations:
                if location.exists():
                    csv_path = location
                    break
            
            if not csv_path:
                logger.error(f"Output file not created by direct analysis")
                return False
            
            # Move to final location
            final_path = output_path
            final_path.parent.mkdir(parents=True, exist_ok=True)
            
            if csv_path.resolve() != final_path.resolve():
                import shutil
                shutil.copy2(str(csv_path), str(final_path))
                csv_path.unlink()  # Clean up
            
            logger.info(f"✅ Direct analysis completed successfully: {final_path}")
            return True
            
        except Exception as e:
            logger.error(f"Direct analysis failed: {e}", job_id=str(job_id))
            return False