"""Tests for code generation tool."""
import pytest
from src.code_generation_tool import CodeGenerationTool

@pytest.mark.asyncio
async def test_code_bootstrap_agent(mock_context, mock_openai_service, test_data_dir):
    """Test code generation bootstrap agent."""
    tool = CodeGenerationTool(mock_openai_service)
    
    result = await tool.code_bootstrap_agent(mock_context, input_dir=test_data_dir)
    
    assert isinstance(result, dict)
    assert "description" in result
    assert "analyzed_legacy_etl_files" in result
    assert "analyzed_csv_files" in result
    assert mock_context.info.called

@pytest.mark.asyncio 
async def test_code_bootstrap_agent_empty_dir(mock_context, mock_openai_service, tmp_path):
    """Test code generation with empty directory."""
    tool = CodeGenerationTool(mock_openai_service)
    empty_dir = str(tmp_path / "empty")
    # Create the empty directory
    tmp_path.joinpath("empty").mkdir(exist_ok=True)
    
    result = await tool.code_bootstrap_agent(mock_context, input_dir=empty_dir)
    
    assert isinstance(result, dict)
    assert "analyzed_legacy_etl_files" in result
    assert "analyzed_csv_files" in result
    assert result["analyzed_legacy_etl_files"] == []
    assert result["analyzed_csv_files"] == []

@pytest.mark.asyncio
async def test_code_bootstrap_agent_invalid_dir(mock_context, mock_openai_service):
    """Test code generation with invalid directory."""
    tool = CodeGenerationTool(mock_openai_service)
    
    result = await tool.code_bootstrap_agent(mock_context, input_dir="/invalid/path")
    
    assert isinstance(result, dict)
    assert "error" in result
    assert mock_context.error.called
