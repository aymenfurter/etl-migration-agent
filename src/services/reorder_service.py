import os
import logging
import shutil
import asyncio
from typing import List, Optional, Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from azure.ai.agents import AgentsClient
from azure.ai.agents.models import CodeInterpreterTool, FilePurpose, MessageRole
from azure.identity import DefaultAzureCredential

from .openai_service import OpenAIService
from .prompts import REORDER_TASK_PROMPT, REORDER_SELECTION_PROMPT_TEMPLATE
from .base_agent_service import BaseAgentService
from ..utils.file_utils import copy_files_to_temp, cleanup_temp_files, read_file_lines

logger = logging.getLogger(__name__)


class ReorderServiceError(Exception):
    """Custom exception for reordering operations."""
    pass


@dataclass
class ReorderContext:
    """Context object to hold reordering operation state."""
    model_name: str
    model_index: int
    file_ids: List[str] = field(default_factory=list)
    agent_id: Optional[str] = None
    thread_id: Optional[str] = None
    tmp_files: List[str] = field(default_factory=list)


class ReorderService(BaseAgentService):
    """Service for reordering CSV files using AI agents."""
    
    DEFAULT_MODEL = "gpt-4"
    CSV_HEAD_LINES = 20
    CSV_SAMPLE_LINES = 5
    
    def __init__(self, openai_service: OpenAIService):
        """Initialize the reorder service."""
        super().__init__()
        self.openai_service = openai_service
        self._initialize_model_deployments()
    
    def _initialize_model_deployments(self) -> None:
        """Initialize model deployments and thread pool."""
        self.model_deployments = getattr(
            self.openai_service.config, 
            'model_deployments', 
            [self.DEFAULT_MODEL]
        )
        self._executor = ThreadPoolExecutor(max_workers=len(self.model_deployments))
    
    async def reorder_csv_files(
        self, 
        source_file_path: str, 
        legacy_output_path: str
    ) -> Tuple[str, str]:
        """Reorder CSV files to match source order using best model.
        
        Args:
            source_file_path: Path to source CSV file
            legacy_output_path: Path to legacy ETL output CSV
            
        Returns:
            Tuple of (reordered_content, selected_model_name)
            
        Raises:
            ReorderServiceError: If reordering fails
        """
        logger.info(f"Starting reordering with {len(self.model_deployments)} models")
        
        model_outputs = await self._run_parallel_reordering(
            source_file_path, 
            legacy_output_path
        )
        
        if not model_outputs:
            raise ReorderServiceError("All models failed to reorder the files")
        
        if len(model_outputs) == 1:
            model_name = list(model_outputs.keys())[0]
            return model_outputs[model_name], model_name
        
        best_model = await self._select_best_reordering(
            source_file_path, 
            model_outputs
        )
        return model_outputs[best_model], best_model
    
    async def _run_parallel_reordering(
        self, 
        source_path: str, 
        legacy_path: str
    ) -> Dict[str, str]:
        """Run reordering with all models in parallel."""
        coroutines = [
            self._run_single_reorder(source_path, legacy_path, model, idx + 1)
            for idx, model in enumerate(self.model_deployments)
        ]
        
        results = await asyncio.gather(*coroutines, return_exceptions=True)
        
        model_outputs = {}
        for model, result in zip(self.model_deployments, results):
            if isinstance(result, Exception):
                logger.error(f"Model {model} failed during reordering: {result}")
            elif isinstance(result, str) and not result.startswith("Error:"):
                model_outputs[model] = result
        
        return model_outputs
    
    async def _run_single_reorder(
        self, 
        source_path: str, 
        legacy_path: str, 
        model_name: str,
        model_index: int
    ) -> str:
        """Run a single reorder operation with specified model."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            self._run_reorder_sync,
            source_path,
            legacy_path,
            model_name,
            model_index
        )
    
    def _run_reorder_sync(
        self, 
        source_path: str, 
        legacy_path: str, 
        model_name: str,
        model_index: int
    ) -> str:
        """Synchronous reordering operation for thread pool."""
        context = ReorderContext(model_name, model_index)
        
        logger.info(f"Running reorder agent {model_index} with model: {model_name}")
        
        try:
            self._prepare_reorder_files(source_path, legacy_path, context)
            self._upload_files_for_reordering(context)
            self._create_reorder_agent(context)
            result = self._execute_reorder_task(context, legacy_path)
            return result
            
        except Exception as e:
            logger.exception(f"Error during reordering with model {model_name}: {e}")
            return f"Error: {str(e)}"
        finally:
            self._cleanup_reorder_resources(context)
    
    def _prepare_reorder_files(
        self, 
        source_path: str, 
        legacy_path: str, 
        context: ReorderContext
    ) -> None:
        """Prepare temporary files for reordering."""
        tmp_source = f"/tmp/source_data_{context.model_index}.csv"
        tmp_legacy = f"/tmp/legacy_etl_output_{context.model_index}.csv"
        
        context.tmp_files = copy_files_to_temp([
            (source_path, tmp_source),
            (legacy_path, tmp_legacy)
        ])
    
    def _upload_files_for_reordering(self, context: ReorderContext) -> None:
        """Upload files to Azure AI Agent for reordering."""
        for file_path in context.tmp_files:
            file_name = os.path.basename(file_path)
            file = self.agents_client.files.upload_and_poll(
                file_path=file_path,
                filename=file_name,
                purpose=FilePurpose.AGENTS
            )
            context.file_ids.append(file.id)
            logger.info(f"Uploaded {file_path} for reordering, file ID: {file.id}")
    
    def _create_reorder_agent(self, context: ReorderContext) -> None:
        """Create and configure the reordering agent."""
        code_interpreter = CodeInterpreterTool(file_ids=context.file_ids)
        
        agent = self.agents_client.create_agent(
            model=context.model_name,
            name=f"reorder_agent_{context.model_index}",
            instructions="You are a data reordering specialist. Follow the user's instructions precisely.",
            tools=code_interpreter.definitions,
            tool_resources=code_interpreter.resources,
        )
        context.agent_id = agent.id
        
        thread = self.agents_client.threads.create()
        context.thread_id = thread.id
    
    def _execute_reorder_task(self, context: ReorderContext, legacy_path: str) -> str:
        """Execute the reordering task with the agent."""
        self.agents_client.messages.create(
            thread_id=context.thread_id,
            role="user",
            content=REORDER_TASK_PROMPT,
        )
        
        run = self.agents_client.runs.create_and_process(
            thread_id=context.thread_id,
            agent_id=context.agent_id
        )
        
        if run.status == "failed":
            logger.error(f"Reorder run failed for model {context.model_name}: {run.last_error}")
            return f"Error: Run failed: {run.last_error}"
        
        return self._process_reorder_results(context, legacy_path)
    
    def _process_reorder_results(self, context: ReorderContext, legacy_path: str) -> str:
        """Process and save reordered files from agent."""
        messages = self.agents_client.messages.list(thread_id=context.thread_id)
        reordered_content = {}
        target_dir = os.path.dirname(legacy_path)
        
        for msg in messages:
            if msg.role == MessageRole.AGENT and hasattr(msg, 'file_path_annotations'):
                for annotation in msg.file_path_annotations:
                    self._process_single_reordered_file(
                        annotation,
                        context,
                        target_dir,
                        reordered_content
                    )
        
        if not reordered_content:
            return "Error: No reordered output was generated"
        
        return self._format_reorder_results(reordered_content)
    
    def _process_single_reordered_file(
        self,
        annotation: Any,
        context: ReorderContext,
        target_dir: str,
        reordered_content: Dict[str, str]
    ) -> None:
        """Process a single reordered file from agent."""
        file_id = annotation.file_path.file_id
        base_filename = annotation.text.lower().split('/')[-1]
        
        temp_filename = f"model_{context.model_index}_{base_filename}"
        temp_path = os.path.join(target_dir, temp_filename)
        
        self.agents_client.files.save(
            file_id=file_id, 
            file_name=temp_filename, 
            target_dir=target_dir
        )
        
        final_filename = temp_filename
        final_path = os.path.join(target_dir, final_filename)
        
        if temp_path != final_path:
            os.rename(temp_path, final_path)
        
        with open(final_path, 'r', encoding='utf-8') as f:
            reordered_content[final_filename] = f.read()
    
    def _format_reorder_results(self, reordered_content: Dict[str, str]) -> str:
        """Format reordering results for output."""
        result_parts = ["# Reordering Analysis\n"]
        
        for file_name, content in reordered_content.items():
            result_parts.append(f"\n## {file_name}\n")
            result_parts.append(content)
            result_parts.append("\n")
        
        return "".join(result_parts)
    
    async def _select_best_reordering(
        self, 
        source_file_path: str, 
        model_outputs: Dict[str, str]
    ) -> str:
        """Select the best reordering using AI judge."""
        source_head = await self._read_csv_head(source_file_path, self.CSV_HEAD_LINES)
        comparison_data = self._build_comparison_data(source_file_path, source_head, model_outputs)
        
        selection_prompt = REORDER_SELECTION_PROMPT_TEMPLATE.format(
            comparison_data=comparison_data,
            model_examples=", ".join([f"'{model}'" for model in self.model_deployments])
        )
        
        try:
            selected_model = await self._get_model_selection(selection_prompt)
            
            if selected_model in model_outputs:
                await self._cleanup_non_winning_files(source_file_path, selected_model)
                logger.info(f"Selected best reordering from model: {selected_model}")
                return selected_model
            else:
                fallback = list(model_outputs.keys())[0]
                logger.warning(f"Invalid model selection '{selected_model}', using fallback: {fallback}")
                return fallback
                
        except Exception as e:
            logger.error(f"Error selecting best reordering: {e}")
            return list(model_outputs.keys())[0]
    
    def _build_comparison_data(
        self, 
        source_path: str, 
        source_head: str, 
        model_outputs: Dict[str, str]
    ) -> str:
        """Build comparison data for model selection."""
        comparison_parts = [f"ORIGINAL SOURCE FILE (first {self.CSV_HEAD_LINES} lines):\n{source_head}\n"]
        target_dir = os.path.dirname(source_path)
        
        for i, (model, _) in enumerate(model_outputs.items(), 1):
            comparison_parts.append(f"\n=== MODEL {i} ({model}) ===")
            
            source_file = os.path.join(target_dir, f"model_{i}_source_data_reordered.csv")
            legacy_file = os.path.join(target_dir, f"model_{i}_legacy_etl_output_reordered.csv")
            
            try:
                source_sample = self._read_file_sample(source_file, self.CSV_SAMPLE_LINES)
                legacy_sample = self._read_file_sample(legacy_file, self.CSV_SAMPLE_LINES)
                
                comparison_parts.append(f"\nSOURCE REORDERED:\n{source_sample}\n")
                comparison_parts.append(f"LEGACY ETL OUTPUT REORDERED:\n{legacy_sample}\n")
                
            except Exception as e:
                logger.error(f"Error reading reordered files for model {model}: {e}")
                comparison_parts.append(" ERROR READING FILES\n")
        
        return "".join(comparison_parts)
    
    def _read_file_sample(self, file_path: str, num_lines: int) -> str:
        """Read first N lines from a file."""
        lines = read_file_lines(file_path, num_lines)
        return '\n'.join(lines) if lines else f"Error reading file"
    
    async def _get_model_selection(self, selection_prompt: str) -> str:
        """Get model selection from AI judge."""
        response = await asyncio.to_thread(
            self.openai_service.client.chat.completions.create,
            model=self.openai_service.config.best_output_selector_model,
            messages=[
                {"role": "system", "content": "You are an expert at evaluating CSV row ordering. Select the best match."},
                {"role": "user", "content": selection_prompt}
            ],
            temperature=0.3,
            max_completion_tokens=50
        )
        
        return response.choices[0].message.content.strip()
    
    async def _cleanup_non_winning_files(self, source_path: str, winning_model: str) -> None:
        """Clean up files from non-winning models and rename winning files."""
        target_dir = os.path.dirname(source_path)
        model_index = self.model_deployments.index(winning_model) + 1
        
        all_model_files = [
            f for f in os.listdir(target_dir) 
            if f.startswith("model_") and f.endswith(".csv")
        ]
        
        # Remove non-winning files
        for file_name in all_model_files:
            if not file_name.startswith(f"model_{model_index}_"):
                file_path = os.path.join(target_dir, file_name)
                try:
                    os.remove(file_path)
                    logger.info(f"Removed non-winning model file: {file_name}")
                except Exception as e:
                    logger.error(f"Error removing file {file_name}: {e}")
        
        # Rename winning files
        winning_files = [
            f for f in all_model_files 
            if f.startswith(f"model_{model_index}_")
        ]
        
        for file_name in winning_files:
            old_path = os.path.join(target_dir, file_name)
            new_name = file_name.replace(f"model_{model_index}_", "")
            new_path = os.path.join(target_dir, new_name)
            
            if os.path.exists(new_path):
                os.remove(new_path)
                logger.info(f"Removed existing file: {new_name}")
            
            os.rename(old_path, new_path)
            logger.info(f"Renamed winning file from {file_name} to {new_name}")
    
    async def _read_csv_head(self, file_path: str, num_lines: int) -> str:
        """Read the first N lines of a CSV file asynchronously."""
        try:
            return await asyncio.to_thread(self._read_file_sample, file_path, num_lines)
        except Exception as e:
            logger.error(f"Error reading CSV head: {e}")
            return f"Error reading file: {str(e)}"
    
    def _cleanup_reorder_resources(self, context: ReorderContext) -> None:
        """Clean up all resources used in reordering operation."""
        super()._cleanup_agent_resources(
            thread_id=context.thread_id,
            agent_id=context.agent_id,
            file_ids=context.file_ids
        )
        cleanup_temp_files(context.tmp_files)
    
    def __del__(self):
        """Clean up the thread pool executor."""
        if hasattr(self, '_executor'):
            self._executor.shutdown(wait=True)
    
    # Maintain backward compatibility
    async def reorder_with_best_model(
        self, 
        source_file_path: str, 
        legacy_etl_output_path: str
    ) -> Tuple[str, str]:
        """Backward compatibility wrapper."""
        return await self.reorder_csv_files(source_file_path, legacy_etl_output_path)
