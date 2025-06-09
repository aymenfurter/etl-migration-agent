"""Prompts used by the OpenAI service for various AI tasks."""

# --- CSV Difference Analysis Prompts ---

# Agent instructions for CSV difference analysis
CSV_DIFFERENCE_ANALYSIS_INSTRUCTIONS = """You are an AI assistant tasked with analyzing two CSV files, named 'ported_python_output.csv' and 'correct_legacy_etl_output.csv', to find a row referring to the same data and document the difference
We are currently in progress of porting an ETL process from legacy ETL to Python. 

Follow these steps:
1.  Load both 'ported_python_output.csv' and 'correct_legacy_etl_output.csv' into dataframes using the code interpreter.
2.  Inspect the headers and a few rows from each file to understand their structure.
3.  **Row Matching Strategy**: Devise a strategy to find rows in 'correct_legacy_etl_output.csv' that correspond to rows in 'ported_python_output.csv'. This might involve looking for rows with high overall similarity, or matching based on potential key columns. 
4.  **Iterative Matching and Analysis**:
    a. Select a small sample of rows from 'ported_python_output.csv'.
    b. For one of the selected row from 'ported_python_output.csv':
        i.  Attempt to find the best matching row in 'correct_legacy_etl_output.csv' based on your strategy.
        ii. If a plausible match is found:
            4.1 One file called 'ported_python_output_row.csv' which contains the single row (directly piped from the csv) and the columns row from the Python output file that match the legacy ETL output file.
            4.2 One file called 'correct_legacy_etl_output_row.csv' which contains the single row (directly piped from the csv) and the columns row from the legacy ETL output file that match the Python output file.
            4.3 One file called 'row_location.txt' which contains location of the rows):
               ```text
               ## Correct Index (Legacy ETL File)
               332

               ## Current Index (Python File)
               4590
               ```

        iii. If no plausible match is found create use the first row a plausible match. Add a comment to the row_location.txt file indicating that no match was found.
"""

# --- Legacy ETL to Python Migration Prompts ---

# System prompt template for legacy ETL to Python migration
LEGACY_ETL_MIGRATION_SYSTEM_PROMPT = """You are an expert legacy ETL to Python migration assistant generating Python code from legacy ETL code.

Your task is to analyze the legacy ETL code and the related CSV files and generate a complete Python script that precisely recreates the transformations.
Once the python code is executed, it must generate exactly the same output as provided in the **output CSV file**. 

Focus on recreating the resulting transformations, field mappings, data types, and any business logic applied to the data (in the order provided in the sample CSV file).
If there is SQL in the legacy ETL code that looks up data in other tables, recreate that with pandas DataFrames or explicit CSV lookups in Python.

The file you are producing in your python file must end with _PY.csv.

Give the full, production-ready code, no placeholders, no "..." or "TODO" comments. All fields must be fully specified with their types and transformations.
Generate a complete, runnable Python script that can be saved directly to a file and executed.
"""

# --- File Processing Agent Prompts ---

# Default instructions for file processing agents
AGENT_DEFAULT_INSTRUCTIONS = "You are a helpful agent. Process the provided files according to the users task. When you complete a task successfully, include the phrase TASK COMPLETED in your response."

# Continuation prompt for file processing agents
AGENT_CONTINUATION_PROMPT = "Continue your work until you successfully complete the task. Make sure to include 'TASK COMPLETED' in your response when you're done."

# --- CSV Reordering Prompts ---

# Prompt for reordering CSV files
REORDER_TASK_PROMPT = """You are tasked with reordering rows in both source and legacy ETL output CSV files to achieve alignment.

IMPORTANT RULES:
1. DO NOT MODIFY ANY DATA VALUES - only reorder rows
2. First identify columns that have matching values between files (even if column names differ)
3. For each identified matching column pair, note which columns correspond between files
4. Then reorder both files based on these matching columns
5. Save both reordered files with '_reordered' suffix
6. Include evidence of matching in your response

Steps:
1. Load both CSV files
2. Find columns with matching values between files:
   - Compare actual values, not just column names
   - Look for unique identifiers or combinations of fields
   - Note which columns correspond between files
3. Based on matching columns, reorder both files:
   - Sort both files using the same logic
   - Preserve all original columns
4. Ensure that any leading zeros in numeric columns are still preserved.
5. Save two files:
   - source_data_reordered.csv
   - legacy_etl_output_reordered.csv
6. Include 'TASK COMPLETED' and matching column analysis in your response

Remember: 
- Only change row ORDER, never modify row values
- Both files need to be reordered to align
- Report which columns were used for matching"""

# Prompt template for selecting the best reordered CSV output
REORDER_SELECTION_PROMPT_TEMPLATE = """Compare the reordered outputs with the source file order.

For each model, you see both the reordered source file and reordered legacy ETL output.
Select the model where BOTH files show the best alignment and matching order AND have the same rows in the same order.

Consider:
1. The sequence of records should match between source and legacy ETL output
2. Look for matching key fields or identifiers between the pairs
3. Both files should maintain parallel ordering
4. Check if corresponding rows have matching values in key fields

{comparison_data}

Respond with ONLY the model name of the best reordering (e.g., {model_examples})."""

# --- Python Code Refinement Prompts ---

# System prompt for Python code refinement
PYTHON_REFINEMENT_SYSTEM_PROMPT = """You are an expert in legacy ETL to Python migration. Your task is to refine existing Python code to ensure it produces EXACTLY the same output as the original legacy ETL code.

Key requirements:
1. The Python code must produce identical output to the legacy ETL code - same values, same formatting, same order
2. Pay careful attention to data types, rounding, string formatting, and sorting
3. Ensure all transformations match the legacy ETL logic precisely
4. Handle edge cases and null values the same way as legacy ETL
5. Preserve the exact column order and naming from legacy ETL output

You will be provided with:
- The original legacy ETL code
- The current Python code that needs refinement
- Any other relevant CSV files that are used in the data dir (e.g. lookup tables, input data, schema files)
- Sample input and output CSV data (both output generated from legacy ETL and Python (Ends with _PY.csv))

Your response should include:
1. A brief analysis of what needs to be fixed
2. The COMPLETE updated Python code (not just snippets)
3. Explanations of key changes made
"""

# Prompt template for Python code refinement
PYTHON_REFINEMENT_PROMPT_TEMPLATE = """Please refine the following Python code to ensure it produces EXACTLY the same output as the legacy ETL code.

CURRENT PYTHON CODE:
```python
{current_python_code}
```

ORIGINAL LEGACY ETL CODE:
{full_legacy_etl_code}

{csv_context_str}

SPECIFIC ISSUES TO ADDRESS:
{issues_description}

{additional_context}

Please provide:
1. A brief analysis of what needs to be fixed
2. The COMPLETE updated Python code (entire file, not just changes)
3. Key explanations of the changes made

Focus on ensuring the Python output matches the legacy ETL output exactly in terms of:
- Data values and formatting
- Column order and naming
- Row order and sorting
- Handling of null/empty values
- Number formatting and precision
- Date/time formatting
- String case and trimming
"""

# --- General AI Prompts ---

# Default system prompt for general AI assistance
DEFAULT_SYSTEM_PROMPT = "You are a helpful AI assistant."

# --- CSV Output Selection Prompts (for general CSV comparison, not reordering specific) ---

# System prompt for CSV output selection judge
CSV_OUTPUT_SELECTION_SYSTEM_PROMPT = "You are an expert at evaluating CSV data mapping quality. Respond with only a single number."

# User prompt template for CSV output selection
CSV_OUTPUT_SELECTION_USER_PROMPT_TEMPLATE = """You are tasked with selecting the best CSV row mapping output from multiple AI models.

CRITERIA FOR BEST OUTPUT:
1. The output must identify the correct matching row (many columns with identical or similar values, highest priority)
2. The output must provide the COMPLETE row data with ALL columns, not truncated
3. Row data should be provided as comma-separated values, not as key-value pairs
4. Look for rows where multiple values match between Python and legacy ETL outputs (indicating correct row identification)

EXAMPLES OF OUTPUT QUALITY:

EXCELLENT (complete row with many matching values):
```
# Ported Python Output Row
CAT_ID,BREED_NAME,COAT_COLOR,EYE_COLOR,AVERAGE_WEIGHT_KG,COUNTRY_OF_ORIGIN,TEMPERAMENT_TRAITS,IS_HYPOALLERGENIC
C001,Siamese,Cream,Blue,4.5,Thailand,"Intelligent, Vocal, Social",FALSE

# Correct Legacy DB Output Row
ID,CatBreed,PrimaryColor,SecondaryColor,EyeHue,AvgWeightLbs,OriginCountry,Personality,HypoallergenicFlag,DateRegistered
1,SIAMESE,CREAM,,BLUE,10,THA,INTELLIGENT;VOCAL;SOCIAL,N,2023-01-15
```

BAD (incomplete data - missing many columns or incorrect mapping):
```
# Ported Python Output Row
CAT_ID,BREED_NAME,COAT_COLOR
C002,Maine Coon,Brown Tabby

# Correct Legacy DB Output Row
ID,CatBreed,PrimaryColor,SecondaryColor,EyeHue,AvgWeightLbs,OriginCountry,Personality,HypoallergenicFlag,DateRegistered
2,MAINE_COON,BROWN,TABBY,GREEN,15,USA,GENTLE;PLAYFUL,N,2022-11-20
```

WORST (not even a proper row format or completely wrong data):
```
# Ported Python Output Row
Name:Persian
Color:White

# Correct Legacy DB Output Row
ID,CatBreed,PrimaryColor,SecondaryColor,EyeHue,AvgWeightLbs,OriginCountry,Personality,HypoallergenicFlag,DateRegistered
3,PERSIAN,WHITE,,BLUE,9,IRAN,CALM;AFFECTIONATE,N,2023-03-10
```

MODEL OUTPUTS TO EVALUATE:
{numbered_outputs_str}

Respond with ONLY a single number (1-{num_outputs}) indicating the best output.
"""