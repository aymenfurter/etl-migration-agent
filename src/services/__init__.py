"""Service modules for Legacy ETL to Python MCP."""
from .base_agent_service import BaseAgentService
from .openai_service import OpenAIService, OpenAIServiceError
from .reorder_service import ReorderService, ReorderServiceError
from .csv_comparison_agent_service import CSVComparisonAgentService, CSVComparisonError
from .prompts import (
    CSV_DIFFERENCE_ANALYSIS_INSTRUCTIONS,
    LEGACY_ETL_MIGRATION_SYSTEM_PROMPT,
    AGENT_DEFAULT_INSTRUCTIONS,
    AGENT_CONTINUATION_PROMPT,
    REORDER_TASK_PROMPT,
    REORDER_SELECTION_PROMPT_TEMPLATE,
    PYTHON_REFINEMENT_SYSTEM_PROMPT,
    DEFAULT_SYSTEM_PROMPT,
    CSV_OUTPUT_SELECTION_SYSTEM_PROMPT,
    CSV_OUTPUT_SELECTION_USER_PROMPT_TEMPLATE
)


__all__ = [
    'BaseAgentService',
    'OpenAIService',
    'OpenAIServiceError', 
    'ReorderService',
    'ReorderServiceError',
    'CSVComparisonAgentService',
    'CSVComparisonError',
    'CSV_DIFFERENCE_ANALYSIS_INSTRUCTIONS',
    'LEGACY_ETL_MIGRATION_SYSTEM_PROMPT',
    'AGENT_DEFAULT_INSTRUCTIONS',
    'AGENT_CONTINUATION_PROMPT',
    'REORDER_TASK_PROMPT',
    'REORDER_SELECTION_PROMPT_TEMPLATE',
    'PYTHON_REFINEMENT_SYSTEM_PROMPT',
    'DEFAULT_SYSTEM_PROMPT',
    'CSV_OUTPUT_SELECTION_SYSTEM_PROMPT',
    'CSV_OUTPUT_SELECTION_USER_PROMPT_TEMPLATE'
]
