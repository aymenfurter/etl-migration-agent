"""Legacy ETL to Python Migration Assistant MCP Server."""
from .config import LegacyEtlMcpConfig
from .server import LegacyEtlMCPServer

__version__ = "1.0.0"
__all__ = ['LegacyEtlMcpConfig', 'LegacyEtlMCPServer']
