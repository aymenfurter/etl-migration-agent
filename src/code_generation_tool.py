"""Tool for generating initial Python code from legacy ETL files with LLM and CSV context."""
import logging
import os
import pickle
import tempfile
import functools
from typing import Dict, List, Any, Optional, Tuple
from fastmcp import Context
from .services.openai_service import OpenAIService
from .services.prompts import LEGACY_ETL_MIGRATION_SYSTEM_PROMPT
from .tools.base_tool import BaseTool
from .utils.file_utils import scan_directory_for_files, build_csv_context

logger = logging.getLogger(__name__)


def mcp_tool_cache(func):
    """Decorator for caching MCP tool results."""
    @functools.wraps(func)
    async def wrapper(self, ctx: Context, *args, **kwargs):
        # 'self' is the tool instance (e.g., CodeGenerationTool)
        # args will be empty because all tool parameters are keyword-only after ctx
        
        # Create a stable cache key from function name and sorted kwargs
        # Exclude ctx from the cache key logic as it's not a parameter defining the call's uniqueness for caching
        cache_key = (func.__name__, tuple(sorted(kwargs.items())))

        if cache_key in self.cache:
            await ctx.info(f"Returning cached result for {func.__name__}")
            return self.cache[cache_key]
        
        result = await func(self, ctx, **kwargs)
        
        # Cache successful results
        if self._is_cacheable_result(result):
            self.cache[cache_key] = result
            await self._save_cache()
            await ctx.info(f"Cached result for {func.__name__}")
            
        return result
    return wrapper


class CodeGenerationTool(BaseTool):
    """Tool for generating Python code from legacy ETL files using LLM."""

    CACHE_FILE_NAME = "code_generation_tool_cache.pkl"

    def __init__(self, openai_service: OpenAIService):
        """Initialize the code generation tool."""
        self.openai_service = openai_service
        self.cache_file = os.path.join(tempfile.gettempdir(), self.CACHE_FILE_NAME)
        self.cache: Dict[Any, Any] = self._load_cache()
        self.system_prompt = LEGACY_ETL_MIGRATION_SYSTEM_PROMPT

    def _load_cache(self) -> Dict[Any, Any]:
        """Load cache from disk if it exists."""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'rb') as f:
                    cache = pickle.load(f)
                    logger.info(f"Loaded cache with {len(cache)} entries")
                    return cache
        except Exception as e:
            logger.warning(f"Failed to load cache: {e}")
        
        logger.info("Starting with empty cache")
        return {}

    async def _save_cache(self) -> None:
        """Save cache to disk."""
        try:
            with open(self.cache_file, 'wb') as f:
                pickle.dump(self.cache, f)
            logger.debug(f"Saved cache with {len(self.cache)} entries")
        except Exception as e:
            logger.warning(f"Failed to save cache: {e}")

    def _is_cacheable_result(self, result: Any) -> bool:
        """Check if a result should be cached."""
        if isinstance(result, dict):
            return "error" not in result
        return True

    @mcp_tool_cache
    async def code_bootstrap_agent(
        self, 
        ctx: Context, 
        *, 
        input_dir: str
    ) -> Dict[str, Any]:
        """Generate initial Python code from legacy ETL and CSV files."""
        await ctx.info(f"Generating Python code from files in: {input_dir}")

        error = await self.validate_directory(ctx, input_dir)
        if error:
            return self.build_error_response(error)

        try:
            return await self.execute(ctx, input_dir=input_dir)
        except Exception as e:
            error_msg = f"Error generating code: {e}"
            logger.exception(error_msg)
            await ctx.error(error_msg)
            return self.build_error_response(str(e))

    async def execute(self, ctx: Context, **kwargs) -> Dict[str, Any]:
        """Execute code generation."""
        input_dir = kwargs['input_dir']
        
        legacy_extension = self.get_legacy_extension(self.openai_service.config)
        files = scan_directory_for_files(input_dir, legacy_extension, '.csv')
        
        legacy_files = files[legacy_extension]
        csv_files = files['.csv']
        
        if not legacy_files and not csv_files:
            await ctx.info("No files found to analyze")
            return self._build_empty_response()
        
        legacy_data = await self.gather_legacy_files(input_dir, legacy_extension, ctx)
        csv_context = build_csv_context(csv_files)
        
        if not legacy_data["full_code"].strip() and not csv_files:
            return self._build_empty_response()
        
        description = await self.openai_service.analyze_legacy_etl_code(
            legacy_data["full_code"],
            csv_context["context_string"],
            input_dir
        )
        
        await ctx.info("Code generation completed successfully")
        
        return self._build_success_response(
            description,
            legacy_data["files"],
            csv_context
        )

    def _build_empty_response(self) -> Dict[str, Any]:
        """Build response for empty directory."""
        return {
            "description": "No legacy ETL or CSV files found to analyze.",
            "analyzed_legacy_etl_files": [],
            "analyzed_csv_files": []
        }

    def _build_success_response(
        self,
        description: str,
        legacy_files: List[str],
        csv_context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Build successful response."""
        return {
            "description": description,
            "analyzed_legacy_etl_files": legacy_files,
            "analyzed_csv_files": [m['filename'] for m in csv_context["metadata"]]
        }