"""FastMCP server implementation for Legacy ETL to Python migration."""
import logging
from typing import Optional
from fastmcp import FastMCP
from .config import LegacyEtlMcpConfig
from .services import OpenAIService, CSVComparisonAgentService
from .code_generation_tool import CodeGenerationTool
from .rowlevel_analyzer_agent_tool import RowlevelAnalyzerAgentTool
from .order_consistency_agent_tool import OrderConsistencyAgentTool
from .refine_python_code_tool import RefinePythonCodeTool

logger = logging.getLogger(__name__)


class LegacyEtlMCPServer:
    """
    FastMCP server for Legacy ETL to Python migration assistance.
    Manages initialization, configuration, and server lifecycle.
    """
    
    def __init__(self):
        """Initialize server components."""
        self.mcp = FastMCP(
            name="LegacyEtlToPythonMigrationAssistant",
            instructions="Provides tools to compare CSV files and generate Python code from legacy ETL code using an LLM."
        )
        self.config: Optional[LegacyEtlMcpConfig] = None
        self._reset_services()

    def _reset_services(self) -> None:
        """Reset all service instances to None."""
        self.openai_service: Optional[OpenAIService] = None
        self.csv_comparison_service: Optional[CSVComparisonAgentService] = None
        self.code_generation_tool: Optional[CodeGenerationTool] = None
        self.rowlevel_analyzer_tool: Optional[RowlevelAnalyzerAgentTool] = None
        self.order_consistency_tool: Optional[OrderConsistencyAgentTool] = None
        self.refine_python_code_tool: Optional[RefinePythonCodeTool] = None

    def initialize(self, config: LegacyEtlMcpConfig) -> bool:
        """
        Initialize server with configuration.
        
        Args:
            config: Legacy ETL MCP configuration.
            
        Returns:
            True if initialization successful, False otherwise.
        """
        self.config = config
        try:
            self._initialize_services()
            self._register_tools()
            logger.info("LegacyEtlMCPServer initialized successfully.")
            return True
        except ConnectionError as e:
            logger.critical(f"Failed to initialize server due to connection error: {e}")
            self._reset_services()
            return False
        except Exception as e:
            logger.critical(f"Failed to initialize server: {e}", exc_info=True)
            self._reset_services()
            return False
    
    def _initialize_services(self) -> None:
        """Initialize all services."""
        try:
            self.openai_service = OpenAIService(self.config)
        except ConnectionError:
            logger.error("Failed to initialize OpenAI service")
            self._reset_services()
            raise

        default_model = self._get_default_model()
        self.csv_comparison_service = CSVComparisonAgentService(
            openai_service=self.openai_service,
            model_name=default_model
        )
        
        self.code_generation_tool = CodeGenerationTool(self.openai_service)
        self.rowlevel_analyzer_tool = RowlevelAnalyzerAgentTool(
            csv_comparison_service=self.csv_comparison_service,
            config=self.config
        )
        self.order_consistency_tool = OrderConsistencyAgentTool(self.openai_service)
        self.refine_python_code_tool = RefinePythonCodeTool(self.openai_service)
    
    def _get_default_model(self) -> str:
        """Get the default model name from config."""
        if hasattr(self.config, 'model_deployments') and self.config.model_deployments:
            return self.config.model_deployments[0]
        return "gpt-4"
    
    def _register_tools(self) -> None:
        """Register all tools with the MCP."""
        self.mcp.tool()(self.code_generation_tool.code_bootstrap_agent)
        self.mcp.tool()(self.rowlevel_analyzer_tool.rowlevel_analyzer_agent)
        self.mcp.tool()(self.order_consistency_tool.order_consistency_agent)
        self.mcp.tool()(self.refine_python_code_tool.code_refinement_agent)
            
    def run(self) -> None:
        """Run the MCP server."""
        logger.info(f"Starting FastMCP server '{self.mcp.name}'...")
        try:
            self.mcp.run() 
        except Exception as e:
            logger.critical(f"FastMCP server '{self.mcp.name}' encountered a fatal error: {e}", exc_info=True)
        finally:
            logger.info(f"FastMCP server '{self.mcp.name}' stopped.")
