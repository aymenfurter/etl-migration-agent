"""Configuration module for Legacy ETL to Python MCP Server."""
import os
import logging
from dataclasses import dataclass, field
from dotenv import load_dotenv
from typing import List

logger = logging.getLogger(__name__)


@dataclass
class LegacyEtlMcpConfig:
    """Configuration for the Legacy ETL MCP Server."""
    azure_openai_api_key: str
    azure_openai_endpoint: str
    azure_openai_deployment_name: str
    azure_openai_api_version: str = "2024-12-01-preview"
    legacy_etl_code_description: str = "Legacy ETL Code"
    model_deployments: List[str] = field(default_factory=lambda: ["gpt-4", "gpt-4.1", "gpt-4o"])
    best_output_selector_model: str = "gpt-4.1"
    refinement_model: str = "gpt-4.1"

    @classmethod
    def load_from_env(cls) -> 'LegacyEtlMcpConfig':
        """Load configuration from environment variables or .env file.
        
        Returns:
            LegacyEtlMcpConfig instance
            
        Raises:
            ValueError: If required configuration is missing
        """
        load_dotenv()
        
        api_key = os.getenv('AZURE_OPENAI_API_KEY')
        endpoint = os.getenv('AZURE_OPENAI_ENDPOINT')
        deployment_name = os.getenv('AZURE_OPENAI_DEPLOYMENT_NAME', 'o3')
        api_version = os.getenv('AZURE_OPENAI_API_VERSION', '2024-12-01-preview')
        legacy_etl_description = os.getenv('LEGACY_ETL_CODE_DESCRIPTION', 'SQL')
        refinement_model = os.getenv('REFINEMENT_MODEL', 'gpt-4.1')
        
        model_deployments = cls._parse_model_deployments()
        best_output_selector = os.getenv('BEST_OUTPUT_SELECTOR_MODEL', 'gpt-4.1')

        if not all([api_key, endpoint]):
            error_msg = (
                "Missing Azure OpenAI configuration. "
                "Please set AZURE_OPENAI_API_KEY and AZURE_OPENAI_ENDPOINT."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        return cls(
            azure_openai_api_key=api_key,
            azure_openai_endpoint=endpoint,
            azure_openai_deployment_name=deployment_name,
            azure_openai_api_version=api_version,
            legacy_etl_code_description=legacy_etl_description,
            model_deployments=model_deployments,
            best_output_selector_model=best_output_selector,
            refinement_model=refinement_model
        )
    
    @staticmethod
    def _parse_model_deployments() -> List[str]:
        """Parse model deployments from environment variable."""
        deployments_env = os.getenv('MODEL_DEPLOYMENTS')
        if deployments_env:
            return [model.strip() for model in deployments_env.split(',')]
        return ["gpt-4", "gpt-4o-dz", "gpt-4.1", "gpt-4o"]
