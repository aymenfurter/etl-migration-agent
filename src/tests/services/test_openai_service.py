"""Tests for OpenAI service that don't require connectivity."""
import pytest
from src.services.openai_service import OpenAIService, OpenAIServiceError

def test_openai_service_initialization(test_config):
    """Test OpenAI service initialization."""
    service = OpenAIService(test_config)
    assert service.config == test_config
    assert service.client is not None

def test_openai_service_initialization_failure():
    """Test OpenAI service initialization failure."""
    with pytest.raises(ConnectionError):
        OpenAIService(None)
