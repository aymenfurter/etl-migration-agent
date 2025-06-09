import os
import pytest
from unittest.mock import Mock, AsyncMock
from fastmcp import Context
from src.config import LegacyEtlMcpConfig
from src.services.openai_service import OpenAIService

@pytest.fixture
def test_config():
    """Test configuration fixture."""
    return LegacyEtlMcpConfig(
        azure_openai_api_key="test_key",
        azure_openai_endpoint="https://test.openai.azure.com",
        azure_openai_deployment_name="test_deployment",
        azure_openai_api_version="2024-12-01-preview",
        model_deployments=["gpt-4", "gpt-4.1"],
        best_output_selector_model="gpt-4.1",
        refinement_model="gpt-4.1"
    )

@pytest.fixture
def mock_openai_service(test_config):
    """Mock OpenAI service fixture."""
    service = Mock(spec=OpenAIService)
    service.config = test_config
    service.get_completion = AsyncMock()
    service.analyze_legacy_etl_code = AsyncMock()
    return service

@pytest.fixture
def mock_context():
    """Mock FastMCP context fixture."""
    context = Mock(spec=Context)
    context.info = AsyncMock()
    context.error = AsyncMock()
    context.warning = AsyncMock()
    return context

@pytest.fixture
def test_data_dir(tmp_path):
    """Create a temporary test data directory with sample files."""
    data_dir = tmp_path / "test_data"
    data_dir.mkdir()
    
    # Create sample CSV files
    (data_dir / "source.csv").write_text("id,name\n1,test\n")
    (data_dir / "legacy.csv").write_text("id,name\n1,test\n")
    
    # Create sample legacy ETL code
    (data_dir / "legacy.sql").write_text("SELECT * FROM test")
    
    return str(data_dir)
