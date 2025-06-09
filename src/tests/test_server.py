"""Tests for Legacy ETL MCP server."""
import pytest
from src.server import LegacyEtlMCPServer
from src.config import LegacyEtlMcpConfig

def test_server_initialization(test_config):
    """Test server initialization with config."""
    server = LegacyEtlMCPServer()
    assert server.initialize(test_config) == True
    assert server.config == test_config
    assert server.openai_service is not None
    assert server.csv_comparison_service is not None
    assert server.code_generation_tool is not None

def test_server_initialization_failure(test_config):
    """Test server initialization failure handling."""
    server = LegacyEtlMCPServer()
    test_config.azure_openai_api_key = None
    assert server.initialize(test_config) == False
    assert server.openai_service is None

def test_server_reset_services():
    """Test resetting server services."""
    server = LegacyEtlMCPServer()
    server._reset_services()
    assert server.openai_service is None
    assert server.csv_comparison_service is None
    assert server.code_generation_tool is None
