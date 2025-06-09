"""Utility functions for file operations."""
import logging
import os
import shutil
import pandas as pd
from typing import Dict, Any, Optional, List, Tuple

logger = logging.getLogger(__name__)

CSV_CONTEXT_SAMPLE_DATA_LINE_LIMIT = 15


def read_csv_metadata(file_path: str, max_rows_sample: int = 10) -> Dict[str, Any]:
    """Reads CSV metadata and a small sample of data."""
    try:
        df = pd.read_csv(file_path, nrows=max_rows_sample, dtype=str, keep_default_na=False)
        return {
            'filename': os.path.basename(file_path),
            'headers': list(df.columns),
            'sample_row_count': len(df),
            'sample_data': df.to_csv(index=False)
        }
    except Exception as e:
        logger.warning(f"Error reading CSV {file_path}: {e}. Skipping this file for context.")
        return {
            'filename': os.path.basename(file_path),
            'error': str(e)
        }


def read_prompt_md(directory_path: str) -> Optional[str]:
    """Read PROMPT.md file from the given directory if it exists."""
    return read_file_safely(os.path.join(directory_path, "PROMPT.md"))


def read_file_safely(file_path: str) -> Optional[str]:
    """Safely read a file's content."""
    if not os.path.exists(file_path):
        return None
        
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        logger.warning(f"Error reading file {file_path}: {e}")
        return None


def read_file_lines(file_path: str, num_lines: int) -> List[str]:
    """Read first N lines from a file."""
    try:
        lines = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for _ in range(num_lines):
                line = f.readline()
                if not line:
                    break
                lines.append(line.strip())
        return lines
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        return []


def validate_file_paths(*file_paths: str) -> None:
    """Validate that all file paths exist.
    
    Raises:
        FileNotFoundError: If any file doesn't exist
    """
    for file_path in file_paths:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")


def scan_directory_for_files(directory: str, *extensions: str) -> Dict[str, List[str]]:
    """Scan directory for files with specific extensions.
    
    Args:
        directory: Directory to scan
        extensions: File extensions to look for (e.g., '.csv', '.sql')
        
    Returns:
        Dictionary mapping extensions to lists of file paths
    """
    results = {ext: [] for ext in extensions}
    
    for item in os.listdir(directory):
        item_path = os.path.join(directory, item)
        if os.path.isfile(item_path):
            for ext in extensions:
                if item.lower().endswith(ext):
                    results[ext].append(item_path)
                    break
    
    return results


def copy_files_to_temp(files: List[Tuple[str, str]]) -> List[str]:
    """Copy files to temporary locations.
    
    Args:
        files: List of (source_path, temp_path) tuples
        
    Returns:
        List of temporary file paths
        
    Raises:
        FileNotFoundError: If source file doesn't exist
    """
    temp_files = []
    for source, temp in files:
        if not os.path.exists(source):
            raise FileNotFoundError(f"Source file not found: {source}")
        shutil.copy2(source, temp)
        temp_files.append(temp)
        logger.debug(f"Copied {source} to {temp}")
    return temp_files


def cleanup_temp_files(file_paths: List[str]) -> None:
    """Clean up temporary files."""
    for file_path in file_paths:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.debug(f"Deleted temporary file {file_path}")
        except Exception as e:
            logger.error(f"Error deleting temporary file {file_path}: {e}")


def build_csv_context(csv_files: List[str]) -> Dict[str, Any]:
    """Build context from CSV files."""
    if not csv_files:
        return {"metadata": [], "context_string": "No CSV files found."}
    
    metadata = []
    context_parts = ["CSV FILES CONTEXT:"]
    
    for csv_path in csv_files:
        file_metadata = read_csv_metadata(csv_path)
        
        if 'error' not in file_metadata:
            metadata.append(file_metadata)
            context_parts.extend([
                f"\n--- CSV File: {file_metadata['filename']} ---",
                f"Headers: {', '.join(file_metadata['headers'])}",
                f"Sample data (first {file_metadata['sample_row_count']} rows):",
                '\n'.join(file_metadata['sample_data'].split('\n')[:CSV_CONTEXT_SAMPLE_DATA_LINE_LIMIT])
            ])
        else:
            context_parts.append(
                f"File: {file_metadata['filename']} - Error: {file_metadata['error']}"
            )
    
    context_parts.append("--- End of CSV Files Context ---")
    
    return {
        "metadata": metadata,
        "context_string": "\n".join(context_parts)
    }
