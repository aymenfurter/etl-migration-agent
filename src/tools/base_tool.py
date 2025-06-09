"""Base class for MCP tools."""
import logging
import os
from typing import Dict, List, Any, Optional
from abc import ABC, abstractmethod
from fastmcp import Context
from ..utils.file_utils import scan_directory_for_files, read_file_safely
from ..config import LegacyEtlMcpConfig

logger = logging.getLogger(__name__)


class BaseTool(ABC):
    """Base class for MCP tools with common functionality."""
    
    async def validate_directory(self, ctx: Context, directory: str) -> Optional[str]:
        """Validate that directory exists.
        
        Returns:
            Error message if validation fails, None otherwise
        """
        if not os.path.isdir(directory):
            error_msg = f"Directory not found: {directory}"
            await ctx.error(error_msg)
            return error_msg
        return None
    
    def get_legacy_extension(self, config: LegacyEtlMcpConfig) -> str:
        """Get the legacy ETL file extension from configuration."""
        description = config.legacy_etl_code_description.lower()
        return f".{description}"
    
    async def gather_legacy_files(
        self, 
        directory: str, 
        legacy_extension: str,
        ctx: Optional[Context] = None
    ) -> Dict[str, Any]:
        """Gather and read all legacy ETL files."""
        legacy_files = []
        legacy_contents = []
        
        files = scan_directory_for_files(directory, legacy_extension)[legacy_extension]
        
        for file_path in files:
            content = read_file_safely(file_path)
            if content:
                legacy_files.append(os.path.basename(file_path))
                legacy_contents.append(content)
            elif ctx:
                await ctx.warning(f"Error reading {file_path}")
        
        return {
            "files": legacy_files,
            "contents": legacy_contents,
            "full_code": self.format_file_contents(legacy_files, legacy_contents)
        }
    
    def format_file_contents(self, filenames: List[str], contents: List[str]) -> str:
        """Format multiple files into a single string."""
        if not filenames:
            return ""
        
        parts = []
        for filename, content in zip(filenames, contents):
            parts.append(f"--- File: {filename} ---\n{content}")
        
        return "\n\n--- End of File ---\n\n".join(parts)
    
    def build_error_response(self, error: str) -> Dict[str, Any]:
        """Build a standard error response."""
        return {"error": error}
    
    @abstractmethod
    async def execute(self, ctx: Context, **kwargs) -> Dict[str, Any]:
        """Execute the tool's main functionality."""
        pass
