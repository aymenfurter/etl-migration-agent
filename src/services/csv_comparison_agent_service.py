import os
import logging
import shutil
import asyncio
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from dataclasses import dataclass, field

from azure.ai.agents import AgentsClient
from azure.ai.agents.models import CodeInterpreterTool, FilePurpose, MessageRole
from azure.identity import DefaultAzureCredential

from .openai_service import OpenAIService
from .prompts import (
    AGENT_DEFAULT_INSTRUCTIONS, 
    AGENT_CONTINUATION_PROMPT
)
from .base_agent_service import BaseAgentService, BaseAgentTaskContext
from ..utils.file_utils import copy_files_to_temp, cleanup_temp_files

load_dotenv()
logger = logging.getLogger(__name__)


class CSVComparisonError(Exception):
    """Custom exception for CSV comparison operations."""
    pass


@dataclass
class AgentContext(BaseAgentTaskContext):
    """Context object to hold agent-related state for CSV comparison."""
    # All fields (model_name, file_ids, agent_id, thread_id, tmp_files) are inherited
    pass


class CSVComparisonAgentService(BaseAgentService):
    """Service for comparing CSV files using Azure AI Agents with multiple models."""
    
    DEFAULT_MODEL = "gpt-4"
    MAX_ATTEMPTS = 3
    TASK_COMPLETION_MARKER = "TASK COMPLETED"
    
    def __init__(self, openai_service: OpenAIService, model_name: str = DEFAULT_MODEL):
        """Initialize the CSV comparison agent service."""
        super().__init__()
        self.openai_service = openai_service
        self.model_name = model_name
        self._initialize_model_deployments()
    
    def _initialize_model_deployments(self) -> None:
        """Initialize model deployments and thread pool."""
        self.model_deployments = getattr(
            self.openai_service.config, 
            'model_deployments', 
            [self.model_name]
        )
        self._executor = ThreadPoolExecutor(max_workers=len(self.model_deployments))
    
    async def compare_csv_files(
        self, 
        python_output_path: str, 
        legacy_output_path: str, 
        task: str
    ) -> str:
        """Compare CSV files using multiple models in parallel.
        
        Args:
            python_output_path: Path to Python-generated CSV
            legacy_output_path: Path to legacy ETL-generated CSV
            task: Task description for the agent
            
        Returns:
            Best comparison result from available models
        """
        if self._is_single_model_mode():
            return await self._run_single_comparison(
                python_output_path, 
                legacy_output_path, 
                task, 
                self.model_name
            )
        
        return await self._run_parallel_comparisons(
            python_output_path, 
            legacy_output_path, 
            task
        )
    
    def _is_single_model_mode(self) -> bool:
        """Check if running in single model mode."""
        return (
            not hasattr(self.openai_service.config, 'model_deployments') or 
            len(self.model_deployments) == 1
        )
    
    async def _run_parallel_comparisons(
        self, 
        python_output_path: str, 
        legacy_output_path: str, 
        task: str
    ) -> str:
        """Run comparisons with multiple models in parallel."""
        logger.info(f"Processing files with {len(self.model_deployments)} models in parallel")
        
        coroutines = [
            self._run_single_comparison(python_output_path, legacy_output_path, task, model)
            for model in self.model_deployments
        ]
        
        results = await asyncio.gather(*coroutines, return_exceptions=True)
        model_outputs = self._collect_successful_outputs(results)
        
        if not model_outputs:
            return "Error: All models failed to process the files"
        
        if len(model_outputs) == 1:
            return list(model_outputs.values())[0]
        
        return await self.openai_service.select_best_csv_output(model_outputs)
    
    def _collect_successful_outputs(
        self, 
        results: List[Any]
    ) -> Dict[str, str]:
        """Collect successful outputs from model results."""
        model_outputs = {}
        for model, result in zip(self.model_deployments, results):
            if isinstance(result, Exception):
                logger.error(f"Model {model} failed: {result}")
            elif isinstance(result, str) and not result.startswith("Error:"):
                model_outputs[model] = result
        return model_outputs
    
    async def _run_single_comparison(
        self, 
        python_output_path: str, 
        legacy_output_path: str,
        task: str, 
        model_name: str
    ) -> str:
        """Run a single agent comparison with the specified model."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            self._run_comparison_sync,
            python_output_path,
            legacy_output_path,
            task,
            model_name
        )
    
    def _run_comparison_sync(
        self, 
        python_output_path: str, 
        legacy_output_path: str,
        task: str, 
        model_name: str
    ) -> str:
        """Synchronous comparison execution for thread pool."""
        agent_context = AgentContext(model_name)
        
        try:
            self._prepare_comparison_files(
                python_output_path, 
                legacy_output_path, 
                model_name, 
                agent_context
            )
            self._upload_files_to_agent(agent_context)
            self._create_and_configure_agent(model_name, agent_context)
            result = self._execute_agent_task(task, agent_context)
            return result
            
        except Exception as e:
            logger.exception(f"Error during comparison with agent {model_name}: {e}")
            return f"An error occurred during agent processing with model {model_name}: {str(e)}"
        finally:
            self._cleanup_agent_resources(agent_context)
    
    def _prepare_comparison_files(
        self, 
        python_path: str, 
        legacy_path: str,
        model_name: str, 
        context: 'AgentContext'
    ) -> None:
        """Prepare temporary files for comparison."""
        tmp_python = f"/tmp/ported_python_output_{model_name.replace('.', '_')}.csv"
        tmp_legacy = f"/tmp/correct_legacy_etl_output_{model_name.replace('.', '_')}.csv"
        
        context.tmp_files = copy_files_to_temp([
            (python_path, tmp_python),
            (legacy_path, tmp_legacy)
        ])
    
    def _upload_files_to_agent(self, context: 'AgentContext') -> None:
        """Upload files to Azure AI Agent."""
        for file_path in context.tmp_files:
            file = self.agents_client.files.upload_and_poll(
                file_path=file_path, 
                purpose=FilePurpose.AGENTS
            )
            context.file_ids.append(file.id)
            logger.info(f"Uploaded {file_path}, file ID: {file.id}")
    
    def _create_and_configure_agent(
        self, 
        model_name: str, 
        context: 'AgentContext'
    ) -> None:
        """Create and configure the AI agent."""
        code_interpreter = CodeInterpreterTool(file_ids=context.file_ids)
        
        agent = self.agents_client.create_agent(
            model=model_name,
            name=f"csv-comparison-agent-{model_name.replace('.', '-')}",
            instructions=AGENT_DEFAULT_INSTRUCTIONS,
            tools=code_interpreter.definitions,
            tool_resources=code_interpreter.resources,
        )
        context.agent_id = agent.id
        
        thread = self.agents_client.threads.create()
        context.thread_id = thread.id
    
    def _execute_agent_task(self, task: str, context: 'AgentContext') -> str:
        """Execute the comparison task with the agent."""
        self.agents_client.messages.create(
            thread_id=context.thread_id,
            role="user",
            content=task,
        )
        
        for attempt in range(1, self.MAX_ATTEMPTS + 1):
            run = self.agents_client.runs.create_and_process(
                thread_id=context.thread_id, 
                agent_id=context.agent_id
            )
            
            if run.status == "failed":
                logger.error(f"Run failed for model {context.model_name}: {run.last_error}")
                return f"Run failed: {run.last_error}"
            
            last_message = self._get_last_agent_message(context.thread_id)
            
            if self._is_task_completed(last_message):
                return self._extract_csv_results(context.thread_id, context.model_name)
            
            if attempt < self.MAX_ATTEMPTS:
                self._send_continuation_prompt(context.thread_id)
        
        return f"Maximum attempts reached without task completion for model {context.model_name}"
    
    def _get_last_agent_message(self, thread_id: str) -> Optional[Any]:
        """Get the last message from the agent."""
        return self.agents_client.messages.get_last_message_text_by_role(
            thread_id=thread_id, 
            role=MessageRole.AGENT
        )
    
    def _is_task_completed(self, message: Optional[Any]) -> bool:
        """Check if the task is marked as completed."""
        return (
            message and 
            message.text and 
            self.TASK_COMPLETION_MARKER in message.text.value
        )
    
    def _send_continuation_prompt(self, thread_id: str) -> None:
        """Send continuation prompt to the agent."""
        self.agents_client.messages.create(
            thread_id=thread_id,
            role="user",
            content=AGENT_CONTINUATION_PROMPT,
        )
    
    def _extract_csv_results(self, thread_id: str, model_name: str) -> str:
        """Extract CSV results from agent messages."""
        messages = self.agents_client.messages.list(thread_id=thread_id)
        result_parts = []
        
        for msg in messages:
            for annotation in msg.file_path_annotations:
                csv_content = self._download_and_read_csv(
                    annotation.file_path.file_id, 
                    model_name
                )
                if csv_content:
                    file_name = self._format_csv_name(annotation.text)
                    result_parts.append(f"# {file_name}\n{csv_content}\n\n")
        
        return "".join(result_parts)
    
    def _download_and_read_csv(self, file_id: str, model_name: str) -> Optional[str]:
        """Download and read CSV content from agent."""
        file_name = f"{file_id}_csv_{model_name.replace('.', '_')}"
        try:
            self.agents_client.files.save(file_id=file_id, file_name=file_name)
            
            with open(file_name, 'r', encoding='utf-8') as f:
                content = f.read()
            
            os.remove(file_name)
            return content
            
        except Exception as e:
            logger.error(f"Error reading CSV file {file_id}: {e}")
            return None
    
    def _format_csv_name(self, text: str) -> str:
        """Format CSV file name for display."""
        name = text.split("/")[-1].replace(".csv", "")
        return name.replace("_", " ").title()
    
    def _cleanup_agent_resources(self, context: 'AgentContext') -> None:
        """Clean up all agent resources."""
        super()._cleanup_agent_resources(
            thread_id=context.thread_id,
            agent_id=context.agent_id,
            file_ids=context.file_ids,
            model_name=context.model_name
        )
        cleanup_temp_files(context.tmp_files)
    
    def __del__(self):
        """Clean up the thread pool executor."""
        if hasattr(self, '_executor'):
            self._executor.shutdown(wait=True)
