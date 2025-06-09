"""Service for interacting with Azure OpenAI."""
import logging
import asyncio
from typing import Optional, List, Dict, Any, Tuple
from openai import AzureOpenAI, OpenAIError
from ..config import LegacyEtlMcpConfig
from .prompts import (
    LEGACY_ETL_MIGRATION_SYSTEM_PROMPT,
    DEFAULT_SYSTEM_PROMPT,
    CSV_OUTPUT_SELECTION_SYSTEM_PROMPT,
    CSV_OUTPUT_SELECTION_USER_PROMPT_TEMPLATE
)
from ..utils.file_utils import read_prompt_md

logger = logging.getLogger(__name__)


class OpenAIServiceError(Exception):
    """Custom exception for OpenAI service operations."""
    pass


class OpenAIService:
    """Service for Azure OpenAI interactions in legacy ETL migration context."""

    DEFAULT_TEMPERATURE = 0.7
    DEFAULT_MAX_TOKENS = 8000
    SELECTION_TEMPERATURE = 0.3
    SELECTION_MAX_TOKENS = 10

    def __init__(self, config: LegacyEtlMcpConfig):
        """Initialize the Azure OpenAI service.

        Args:
            config: The application configuration.
            
        Raises:
            ConnectionError: If Azure OpenAI client initialization fails.
        """
        self.config = config
        self._initialize_client()
        self.system_prompt_template = LEGACY_ETL_MIGRATION_SYSTEM_PROMPT

    def _initialize_client(self) -> None:
        """Initialize the Azure OpenAI client."""
        if not self.config:
            error_msg = "Configuration cannot be None"
            logger.error(error_msg)
            raise ConnectionError(error_msg)
        
        if not self.config.azure_openai_api_key:
            error_msg = "Azure OpenAI API key is required"
            logger.error(error_msg)
            raise ConnectionError(error_msg)
            
        try:
            self.client = AzureOpenAI(
                api_key=self.config.azure_openai_api_key,
                azure_endpoint=self.config.azure_openai_endpoint,
                api_version=self.config.azure_openai_api_version,
            )
            logger.info(
                f"Initialized Azure OpenAI service with model: "
                f"{self.config.azure_openai_deployment_name}"
            )
        except Exception as e:
            error_msg = f"Failed to initialize Azure OpenAI client: {e}"
            logger.error(error_msg)
            raise ConnectionError(error_msg) from e

    async def get_completion(
        self, 
        prompt: str, 
        system_prompt: Optional[str] = None, 
        model_name: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None
    ) -> str:
        """Get a completion from Azure OpenAI.
        
        Args:
            prompt: The user prompt
            system_prompt: Optional system prompt (defaults to general assistant)
            model_name: Optional model name override
            temperature: Optional temperature override
            max_tokens: Optional max tokens override
            
        Returns:
            The completion text
            
        Raises:
            OpenAIServiceError: If the API call fails
        """
        system_prompt = system_prompt or DEFAULT_SYSTEM_PROMPT
        model_name = model_name or self.config.azure_openai_deployment_name
        temperature = temperature if temperature is not None else self.DEFAULT_TEMPERATURE
        max_tokens = max_tokens or self.DEFAULT_MAX_TOKENS
        
        try:
            response = await self._make_api_call(
                model=model_name,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt}
                ],
                temperature=temperature,
                max_completion_tokens=max_tokens
            )
            
            return self._extract_response_content(response)
            
        except Exception as e:
            raise OpenAIServiceError(f"Failed to get completion: {e}") from e

    async def analyze_legacy_etl_code(
        self, 
        legacy_code: str, 
        csv_context: str, 
        directory_path: Optional[str] = None
    ) -> str:
        """Analyze legacy ETL code and generate Python migration.
        
        Args:
            legacy_code: The legacy ETL code to analyze
            csv_context: CSV file context information
            directory_path: Optional directory containing PROMPT.md
            
        Returns:
            Python code and analysis
            
        Raises:
            OpenAIServiceError: If analysis fails
        """
        logger.info(
            f"Analyzing legacy ETL code (length: {len(legacy_code)} chars) "
            f"with CSV context (length: {len(csv_context)} chars)"
        )
        
        user_prompt = self._build_analysis_prompt(legacy_code, csv_context)
        system_prompt = self._build_system_prompt(directory_path)
        
        try:
            response = await self._make_api_call(
                model=self.config.azure_openai_deployment_name,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=1.0,
                max_completion_tokens=self.DEFAULT_MAX_TOKENS
            )
            
            result = self._extract_response_content(response)
            logger.info("Successfully completed legacy ETL code analysis")
            return result
            
        except Exception as e:
            error_msg = f"Failed to analyze legacy ETL code: {e}"
            logger.error(error_msg)
            raise OpenAIServiceError(error_msg) from e

    async def select_best_csv_output(self, model_outputs: Dict[str, str]) -> str:
        """Select the best CSV analysis output from multiple models.
        
        Args:
            model_outputs: Dictionary mapping model names to their outputs
            
        Returns:
            The best output selected by the judge model
        """
        logger.info(f"Selecting best output from {len(model_outputs)} model responses")
        
        if len(model_outputs) == 1:
            return list(model_outputs.values())[0]
        
        numbered_outputs = self._prepare_numbered_outputs(model_outputs)
        selection_prompt = self._build_selection_prompt(numbered_outputs)
        
        try:
            selected_number = await self._get_selection_number(selection_prompt)
            return self._get_selected_output(numbered_outputs, selected_number, model_outputs)
            
        except Exception as e:
            logger.error(f"Error selecting best output: {e}, using first model")
            return list(model_outputs.values())[0]

    def _build_analysis_prompt(self, legacy_code: str, csv_context: str) -> str:
        """Build the user prompt for legacy ETL analysis."""
        code_language = self.config.legacy_etl_code_description.lower()
        return f"""LEGACY ETL CODE TO ANALYZE:
```{code_language}
{legacy_code}
```

CSV CONTEXT:
{csv_context}

Based on the data provided, provide complete migrated python code."""

    def _build_system_prompt(self, directory_path: Optional[str]) -> str:
        """Build the system prompt with optional PROMPT.md content."""
        system_prompt = self.system_prompt_template
        
        if directory_path:
            prompt_md_content = read_prompt_md(directory_path)
            if prompt_md_content:
                system_prompt += f"\n\n## Additional Instructions from PROMPT.md\n\n{prompt_md_content}"
                
        return system_prompt

    def _prepare_numbered_outputs(
        self, 
        model_outputs: Dict[str, str]
    ) -> Dict[int, Tuple[str, str]]:
        """Prepare numbered outputs for selection."""
        model_list = list(model_outputs.items())
        return {
            i + 1: (model, output) 
            for i, (model, output) in enumerate(model_list)
        }

    def _build_selection_prompt(self, numbered_outputs: Dict[int, Tuple[str, str]]) -> str:
        """Build the selection prompt for the judge model."""
        outputs_text = "\n".join(
            f"=== OUTPUT {num} ===\n{output}\n" 
            for num, (_, output) in numbered_outputs.items()
        )
        
        return CSV_OUTPUT_SELECTION_USER_PROMPT_TEMPLATE.format(
            numbered_outputs_str=outputs_text,
            num_outputs=len(numbered_outputs)
        )

    async def _get_selection_number(self, selection_prompt: str) -> int:
        """Get the selection number from the judge model."""
        judge_model = self.config.best_output_selector_model or self.config.azure_openai_deployment_name
        
        response = await self._make_api_call(
            model=judge_model,
            messages=[
                {"role": "system", "content": CSV_OUTPUT_SELECTION_SYSTEM_PROMPT},
                {"role": "user", "content": selection_prompt}
            ],
            temperature=self.SELECTION_TEMPERATURE,
            max_completion_tokens=self.SELECTION_MAX_TOKENS
        )
        
        selection_text = self._extract_response_content(response)
        
        try:
            return int(selection_text)
        except ValueError:
            raise ValueError(f"Invalid selection response: {selection_text}")

    def _get_selected_output(
        self, 
        numbered_outputs: Dict[int, Tuple[str, str]], 
        selected_num: int,
        model_outputs: Dict[str, str]
    ) -> str:
        """Get the selected output based on the selection number."""
        if selected_num in numbered_outputs:
            selected_model, selected_output = numbered_outputs[selected_num]
            logger.info(f"Selected best output from model: {selected_model} (output #{selected_num})")
            return selected_output
        else:
            logger.warning(f"Invalid selection number: {selected_num}. Using first available.")
            return list(model_outputs.values())[0]

    async def _make_api_call(self, **kwargs) -> Any:
        """Make an async API call to Azure OpenAI."""
        try:
            return await asyncio.to_thread(
                self.client.chat.completions.create,
                **kwargs
            )
        except OpenAIError as e:
            logger.error(f"Azure OpenAI API error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during OpenAI call: {e}")
            raise

    def _extract_response_content(self, response: Any) -> str:
        """Extract content from API response."""
        content = response.choices[0].message.content
        if content is None:
            raise OpenAIServiceError("Response content is None")
        return content.strip()
    
    # Maintain backward compatibility
    async def analyze_legacy_etl_code_with_context(
        self, 
        legacy_etl_code: str, 
        csv_context_str: str, 
        directory_path: Optional[str] = None
    ) -> str:
        """Backward compatibility wrapper for analyze_legacy_etl_code."""
        return await self.analyze_legacy_etl_code(
            legacy_etl_code, 
            csv_context_str, 
            directory_path
        )