import pytest
from src.refine_python_code_tool import RefinePythonCodeTool

@pytest.mark.asyncio
async def test_code_refinement_agent(mock_context, mock_openai_service, test_data_dir):
    """Test Python code refinement."""
    tool = RefinePythonCodeTool(mock_openai_service)
    
    result = await tool.code_refinement_agent(
        mock_context,
        data_dir=test_data_dir,
        python_file="test.py",
        issues_description="Fix data types"
    )
    
    assert isinstance(result, dict)
    assert mock_context.info.called

@pytest.mark.asyncio
async def test_code_refinement_agent_missing_file(mock_context, mock_openai_service, test_data_dir):
    """Test refinement with missing Python file."""
    tool = RefinePythonCodeTool(mock_openai_service)
    
    result = await tool.code_refinement_agent(
        mock_context,
        data_dir=test_data_dir,
        python_file="nonexistent.py"
    )
    
    assert isinstance(result, dict)
    assert "error" in result
    assert mock_context.error.called
