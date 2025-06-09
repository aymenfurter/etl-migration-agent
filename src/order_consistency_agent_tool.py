"""Tool for ensuring row order consistency between source and output files."""
import logging
import os
from typing import Tuple
from fastmcp import Context
from .services.reorder_service import ReorderService, ReorderServiceError
from .services.openai_service import OpenAIService

logger = logging.getLogger(__name__)


class OrderConsistencyAgentTool:
    """Tool for ensuring row order consistency in ETL outputs."""
    
    def __init__(self, openai_service: OpenAIService):
        """Initialize the order consistency tool.
        
        Args:
            openai_service: Instance of OpenAIService
        """
        self.reorder_service = ReorderService(openai_service)
    
    async def order_consistency_agent(
        self, 
        ctx: Context, 
        *,
        data_path: str,
        source_file: str,
        legacy_etl_output_file: str
    ) -> str:
        """Ensure row order consistency between source and legacy ETL output.
        
        This tool uses AI agents to intelligently match and reorder rows from the 
        legacy ETL output to match the sequence in the source file, without 
        modifying any data values.
        
        Args:
            ctx: The FastMCP context
            data_path: Path to the data directory
            source_file: Name of the source CSV file (original input)
            legacy_etl_output_file: Name of the legacy ETL output file to reorder
            
        Returns:
            A string describing the result of the reordering operation
        """
        await ctx.info(f"Ensuring order consistency for {legacy_etl_output_file}")
        
        try:
            source_path, legacy_path = self._build_file_paths(
                data_path, 
                source_file, 
                legacy_etl_output_file
            )
            
            self._validate_file_paths(source_path, legacy_path)
            
            await ctx.info("Running reordering with multiple AI models...")
            
            _, selected_model = await self.reorder_service.reorder_csv_files(
                source_path, 
                legacy_path
            )
            
            await ctx.info(f"Successfully reordered using model: {selected_model}")
            
            return self._format_success_message(legacy_etl_output_file, selected_model)
            
        except FileNotFoundError as e:
            error_msg = str(e)
            await ctx.error(error_msg)
            return f"Error: {error_msg}"
        except ReorderServiceError as e:
            error_msg = f"Reordering failed: {e}"
            logger.exception(error_msg)
            await ctx.error(error_msg)
            return f"Error: {error_msg}"
        except Exception as e:
            error_msg = f"Unexpected error during reordering: {e}"
            logger.exception(error_msg)
            await ctx.error(error_msg)
            return f"Error: {str(e)}"
    
    def _build_file_paths(
        self, 
        base_path: str, 
        source_file: str, 
        legacy_file: str
    ) -> Tuple[str, str]:
        """Build full file paths from base path and file names."""
        source_path = os.path.join(base_path, source_file)
        legacy_path = os.path.join(base_path, legacy_file)
        return source_path, legacy_path
    
    def _validate_file_paths(self, source_path: str, legacy_path: str) -> None:
        """Validate that both files exist.
        
        Raises:
            FileNotFoundError: If either file doesn't exist
        """
        if not os.path.exists(source_path):
            raise FileNotFoundError(f"Source file not found: {source_path}")
        
        if not os.path.exists(legacy_path):
            raise FileNotFoundError(f"Legacy ETL output file not found: {legacy_path}")
    
    def _format_success_message(self, output_file: str, model_name: str) -> str:
        """Format the success message for the operation."""
        return f"""Order consistency ensured successfully! 🎯

✅ Reordered {output_file} to match source row order
✅ Saved as: 'source_data_reordered.csv' and 'legacy_etl_output_reordered.csv'
✅ Best ordering selected from model: {model_name}
✅ Data integrity preserved - only row order was changed

The reordered files are now ready for accurate comparison."""
