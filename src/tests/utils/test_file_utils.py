import os
import pytest
import pandas as pd
from src.utils.file_utils import (
    read_csv_metadata,
    read_file_safely,
    scan_directory_for_files,
    build_csv_context
)

def test_read_csv_metadata(tmp_path):
    """Test reading CSV metadata."""
    csv_path = tmp_path / "test.csv"
    pd.DataFrame({
        'col1': ['a', 'b'],
        'col2': [1, 2]
    }).to_csv(csv_path, index=False)
    
    metadata = read_csv_metadata(str(csv_path))
    
    assert metadata['filename'] == "test.csv"
    assert metadata['headers'] == ['col1', 'col2']
    assert metadata['sample_row_count'] == 2

def test_read_file_safely(tmp_path):
    """Test safe file reading."""
    file_path = tmp_path / "test.txt"
    file_path.write_text("test content")
    
    content = read_file_safely(str(file_path))
    assert content == "test content"
    
    # Test nonexistent file
    assert read_file_safely("nonexistent.txt") is None

def test_scan_directory_for_files(test_data_dir):
    """Test directory scanning for files."""
    files = scan_directory_for_files(test_data_dir, '.csv', '.sql')
    
    assert len(files['.csv']) == 2
    assert len(files['.sql']) == 1
    assert all(os.path.exists(f) for f in files['.csv'])
    assert all(os.path.exists(f) for f in files['.sql'])

def test_build_csv_context(test_data_dir):
    """Test building CSV context."""
    csv_files = scan_directory_for_files(test_data_dir, '.csv')['.csv']
    context = build_csv_context(csv_files)
    
    assert 'metadata' in context
    assert 'context_string' in context
    assert len(context['metadata']) == 2
    assert "CSV FILES CONTEXT:" in context['context_string']
