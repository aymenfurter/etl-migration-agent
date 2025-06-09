"""Tool for analyzing row-level differences between Python and legacy ETL outputs."""
import logging
import os
from typing import Union, Dict, Any
from fastmcp import Context
from .config import LegacyEtlMcpConfig
from .services.csv_comparison_agent_service import CSVComparisonAgentService
from .services.prompts import CSV_DIFFERENCE_ANALYSIS_INSTRUCTIONS

logger = logging.getLogger(__name__)


class RowLevelAnalyzerError(Exception):
    """Custom exception for row-level analysis errors."""
    pass


class RowlevelAnalyzerAgentTool:
    """Tool for analyzing row-level differences between CSV outputs."""
    
    ERROR_INDICATORS = ["Error:", "Run failed:"]

    def __init__(
        self, 
        csv_comparison_service: CSVComparisonAgentService, 
        config: LegacyEtlMcpConfig
    ):
        """Initialize the row-level analyzer tool.
        
        Args:
            csv_comparison_service: Instance of CSVComparisonAgentService
            config: The application configuration
        """
        self.csv_comparison_service = csv_comparison_service
        self.config = config

    async def rowlevel_analyzer_agent(
        self, 
        ctx: Context, 
        *, 
        data_path: str, 
        legacy_etl_output_file: str,
        python_output_file: str
    ) -> Union[str, Dict[str, Any]]:
        """Analyze row-level differences between Python and legacy ETL outputs.
        
        Args:
            ctx: The FastMCP context
            data_path: Path to the data directory
            legacy_etl_output_file: Name of the legacy ETL-generated output file
            python_output_file: Name of the Python-generated output file
            
        Returns:
            Analysis results as string or error dictionary
        """
        await ctx.info(f"Analyzing row differences for {data_path}")
        
        try:
            legacy_path = self._build_file_path(data_path, legacy_etl_output_file)
            python_path = self._build_file_path(data_path, python_output_file)
            
            await ctx.info(f"Running CSV difference analysis on Python output: {python_path}")
            
            analysis_result = await self._analyze_csv_differences(
                python_path,
                legacy_path
            )

            if isinstance(analysis_result, dict) and "error" in analysis_result:
                await ctx.error(f"Analysis failed: {analysis_result['error']}")
            else:
                await ctx.info("Row-level analysis completed successfully")
                
            return analysis_result
            
        except Exception as e:
            error_msg = f"Error analyzing row differences: {e}"
            logger.exception(error_msg)
            await ctx.error(error_msg)
            return {"error": str(e)}
    
    async def _analyze_csv_differences(
        self, 
        python_output_path: str, 
        legacy_output_path: str
    ) -> Union[str, Dict[str, Any]]:
        """Analyze differences between two CSV files using the AI agent.
        
        Args:
            python_output_path: Path to the Python-generated CSV file
            legacy_output_path: Path to the legacy ETL-generated CSV file
            
        Returns:
            Analysis results as string or error dictionary
        """
        logger.info(
            f"Analyzing differences between {python_output_path} "
            f"and {legacy_output_path}"
        )
        
        try:
            self._validate_file_paths(python_output_path, legacy_output_path)
            
            analysis_text = await self.csv_comparison_service.compare_csv_files(
                python_output_path=python_output_path,
                legacy_output_path=legacy_output_path,
                task=CSV_DIFFERENCE_ANALYSIS_INSTRUCTIONS
            )
            
            if self._is_error_response(analysis_text):
                logger.error(f"Agent processing returned an error: {analysis_text}")
                return {"error": analysis_text}
                
            logger.info("AI agent analysis complete")
            return analysis_text
            
        except FileNotFoundError as e:
            return {"error": str(e)}
        except Exception as e:
            logger.exception(f"Error analyzing CSV differences: {e}")
            return {"error": str(e)}
    
    def _build_file_path(self, base_path: str, filename: str) -> str:
        """Build full file path from base path and filename."""
        return os.path.join(base_path, filename)
    
    def _validate_file_paths(self, python_path: str, legacy_path: str) -> None:
        """Validate that both file paths exist.
        
        Raises:
            FileNotFoundError: If either file doesn't exist
        """
        if not os.path.exists(python_path):
            raise FileNotFoundError(f"File not found: {python_path}")
        if not os.path.exists(legacy_path):
            raise FileNotFoundError(f"File not found: {legacy_path}")
    
    def _is_error_response(self, response: str) -> bool:
        """Check if the response indicates an error."""
        return any(indicator in response for indicator in self.ERROR_INDICATORS)
