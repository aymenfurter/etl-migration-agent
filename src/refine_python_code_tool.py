"""Tool for refining Python code to ensure exact output match with legacy ETL."""
import logging
import os
from typing import Dict, Any
from fastmcp import Context
from .services.openai_service import OpenAIService
from .services.prompts import PYTHON_REFINEMENT_SYSTEM_PROMPT, PYTHON_REFINEMENT_PROMPT_TEMPLATE
from .tools.base_tool import BaseTool
from .utils.file_utils import read_file_safely, build_csv_context, scan_directory_for_files

logger = logging.getLogger(__name__)


class CodeRefinementError(Exception):
    """Custom exception for code refinement operations."""
    pass


class RefinePythonCodeTool(BaseTool):
    """Tool for refining Python code to match legacy ETL output exactly."""

    def __init__(self, openai_service: OpenAIService):
        """Initialize the code refinement tool."""
        self.openai_service = openai_service

    async def code_refinement_agent(
        self, 
        ctx: Context, 
        *, 
        data_dir: str,
        python_file: str,
        issues_description: str = ""
    ) -> Dict[str, Any]:
        """Refine Python code to match legacy ETL output exactly."""
        await ctx.info(f"Refining Python code in {python_file}")

        error = await self.validate_directory(ctx, data_dir)
        if error:
            return self.build_error_response(error)

        try:
            return await self.execute(
                ctx, 
                data_dir=data_dir,
                python_file=python_file,
                issues_description=issues_description
            )
        except CodeRefinementError as e:
            await ctx.error(str(e))
            return self.build_error_response(str(e))
        except Exception as e:
            error_msg = f"Error during refinement: {e}"
            logger.exception(error_msg)
            await ctx.error(error_msg)
            return self.build_error_response(str(e))

    async def execute(self, ctx: Context, **kwargs) -> Dict[str, Any]:
        """Execute code refinement."""
        data_dir = kwargs['data_dir']
        python_file = kwargs['python_file']
        issues_description = kwargs.get('issues_description', '')
        
        python_path = os.path.join(data_dir, python_file)
        if not os.path.exists(python_path):
            raise CodeRefinementError(f"Python file not found: {python_path}")

        python_code = read_file_safely(python_path)
        if not python_code:
            raise CodeRefinementError(f"Error reading Python file: {python_path}")

        legacy_extension = self.get_legacy_extension(self.openai_service.config)
        legacy_data = await self.gather_legacy_files(data_dir, legacy_extension)
        
        csv_files = scan_directory_for_files(data_dir, '.csv')['.csv']
        csv_context = build_csv_context(csv_files)
        
        additional_context = await self._read_additional_context(data_dir)
        
        refined_code = await self._refine_code(
            python_code,
            legacy_data,
            csv_context,
            issues_description,
            additional_context
        )
        
        await ctx.info("Python code refinement completed successfully")
        
        return self._build_success_response(
            refined_code,
            python_file,
            legacy_data,
            csv_context
        )

    async def _read_additional_context(self, data_dir: str) -> str:
        """Read additional context from PROMPT.md if it exists."""
        content = read_file_safely(os.path.join(data_dir, "PROMPT.md"))
        return f"\n\nAdditional Context from PROMPT.md:\n{content}" if content else ""

    async def _refine_code(
        self,
        python_code: str,
        legacy_data: Dict[str, Any],
        csv_context: Dict[str, Any],
        issues_description: str,
        additional_context: str
    ) -> str:
        """Refine the Python code using the OpenAI service."""
        refinement_prompt = PYTHON_REFINEMENT_PROMPT_TEMPLATE.format(
            current_python_code=python_code,
            full_legacy_etl_code=legacy_data["full_code"],
            csv_context_str=csv_context["context_string"],
            issues_description=issues_description or "General refinement needed to match legacy ETL output exactly.",
            additional_context=additional_context
        )

        refinement_model = self.openai_service.config.refinement_model
        
        return await self.openai_service.get_completion(
            prompt=refinement_prompt,
            system_prompt=PYTHON_REFINEMENT_SYSTEM_PROMPT,
            model_name=refinement_model
        )

    def _build_success_response(
        self,
        refined_code: str,
        python_file: str,
        legacy_data: Dict[str, Any],
        csv_context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Build the success response dictionary."""
        return {
            "refined_code": refined_code,
            "python_file": python_file,
            "analyzed_legacy_etl_files": legacy_data["files"],
            "analyzed_csv_files": [m['filename'] for m in csv_context["metadata"]],
            "model_used": self.openai_service.config.refinement_model
        }
