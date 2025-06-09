"""Base class for Azure AI Agent services."""
import os
import logging
from typing import Optional, List
from abc import ABC
from dataclasses import dataclass, field
from azure.ai.agents import AgentsClient
from azure.identity import DefaultAzureCredential

logger = logging.getLogger(__name__)


@dataclass
class BaseAgentTaskContext:
    """Base context object to hold common agent operation state."""
    model_name: str
    file_ids: List[str] = field(default_factory=list)
    agent_id: Optional[str] = None
    thread_id: Optional[str] = None
    tmp_files: List[str] = field(default_factory=list)


class BaseAgentService(ABC):
    """Base class for services using Azure AI Agents."""
    
    def __init__(self):
        """Initialize base agent service."""
        self._initialize_azure_client()
    
    def _initialize_azure_client(self) -> None:
        """Initialize Azure AI Agents client."""
        project_endpoint = os.environ.get("PROJECT_ENDPOINT")
        if not project_endpoint:
            error_msg = "PROJECT_ENDPOINT environment variable not set."
            logger.error(error_msg)
            raise ValueError(error_msg)
            
        self.agents_client = AgentsClient(
            endpoint=project_endpoint,
            credential=DefaultAzureCredential(),
        )
    
    def _cleanup_agent_resources(
        self, 
        thread_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        file_ids: Optional[List[str]] = None,
        model_name: str = ""
    ) -> None:
        """Clean up all agent resources."""
        if not hasattr(self, 'agents_client') or not self.agents_client:
            return
            
        if thread_id:
            self._cleanup_thread(thread_id, model_name)
        
        if file_ids:
            self._cleanup_files(file_ids, model_name)
            
        if agent_id:
            self._cleanup_agent(agent_id, model_name)
    
    def _cleanup_thread(self, thread_id: str, model_name: str = "") -> None:
        """Clean up agent thread."""
        try:
            self.agents_client.threads.delete(thread_id)
            logger.debug(f"Deleted thread {thread_id}" + (f" for model {model_name}" if model_name else ""))
        except Exception as e:
            logger.error(f"Error deleting thread {thread_id}: {e}")
    
    def _cleanup_files(self, file_ids: List[str], model_name: str = "") -> None:
        """Clean up uploaded files."""
        for file_id in file_ids:
            try:
                self.agents_client.files.delete(file_id)
                logger.debug(f"Deleted agent file {file_id}" + (f" for model {model_name}" if model_name else ""))
            except Exception as e:
                logger.error(f"Error deleting file {file_id}: {e}")
    
    def _cleanup_agent(self, agent_id: str, model_name: str = "") -> None:
        """Clean up the agent."""
        try:
            self.agents_client.delete_agent(agent_id)
            logger.debug(f"Deleted agent {agent_id}" + (f" for model {model_name}" if model_name else ""))
        except Exception as e:
            logger.error(f"Error deleting agent {agent_id}: {e}")
