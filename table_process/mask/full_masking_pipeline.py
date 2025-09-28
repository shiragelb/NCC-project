# -*- coding: utf-8 -*-

!pip install anthropic

import pandas as pd
import re
import json
import anthropic
from typing import Iterable, Optional
from pathlib import Path
from google.colab import drive, files
import shutil
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import glob
from collections import Counter

# Mount Google Drive
drive.mount('/content/drive')

class TableLoader:
    """Handles table loading and cleaning operations."""

    def load_and_clean(self, csv_path: str) -> pd.DataFrame:
        """
        Load CSV file and clean the data.

        Args:
            csv_path: Path to the CSV file

        Returns:
            Cleaned DataFrame with asterisks removed
        """
        # Before 2017 there were indices and title in the first two rows, after it there's just the table
        identifier = Path(csv_path).stem
        _, _, year = identifier.split('_')
        if int(year) < 2017:
            header=0
        else:
            header=None
        # Read CSV treating all rows equally (no header assumption)
        df = pd.read_csv(csv_path, header=header, dtype=str, keep_default_na=False)

        # Remove asterisks from all cells
        df = df.map(lambda x: str(x).replace('*', '') if pd.notna(x) else '')

        return df

class HardRuleClassifier:
    """Applies hard-coded classification rules to table cells."""

    def __init__(self, threshold: float = 0.8, consistency_threshold: float = 0.3):
        """
        Initialize the classifier with configurable threshold.

        Args:
            threshold: Minimum ratio for row identity rule (default 0.9)
        """
        self.threshold = threshold
        self.consistency_threshold = consistency_threshold

    def _is_missing_value(self, cell_value) -> bool:
        """
        Check if a cell value represents a missing value.

        Args:
            cell_value: Cell value to check (any type)

        Returns:
            True if cell represents a missing value
        """
        # Handle None values directly
        if cell_value is None or pd.isna(cell_value):
            return True

        # Convert to string and strip whitespace for pattern matching
        cell_str = str(cell_value).strip()

        # Empty string or whitespace-only
        if not cell_str:
            return True

        # Convert to lowercase for case-insensitive matching
        cell_lower = cell_str.lower()

        # Common missing value indicators (case-insensitive)
        missing_indicators = {
            "n/a", "na", "null", "nan", "none", "missing", "unknown", ""
        }

        if cell_lower in missing_indicators:
            return True

        return False

    def classify(self, table_df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply all hard-coded rules to classify cells.

        Args:
            table_df: Input table DataFrame

        Returns:
            Mask DataFrame with "feature", "data-point", "None", or "undecided"
        """
        # Initialize mask with "undecided"
        mask = pd.DataFrame("undecided", index=table_df.index, columns=table_df.columns)

        # Rule 0: Missing value check (highest priority)
        for i in range(len(table_df)):
            for j in range(len(table_df.columns)):
                cell_value = table_df.iloc[i, j]
                if self._is_missing_value(cell_value):
                    mask.iloc[i, j] = "None"

        # Rule 1: Row identity check
        for idx, row in table_df.iterrows():
            if self._check_row_identity(row):
                # Only set cells to "feature" if they're not already classified as "None"
                for col_idx in range(len(table_df.columns)):
                    if mask.iloc[idx, col_idx] != "None":
                        mask.iloc[idx, col_idx] = "feature"

        # Rules 2-4: Numeric patterns and missing values
        for i in range(len(table_df)):
            for j in range(len(table_df.columns)):
                # Only check if cell is still undecided (protects "None" and "feature" classifications)
                if mask.iloc[i, j] == "undecided":
                    cell_value = str(table_df.iloc[i, j]).strip()
                    if self._is_numeric_pattern(cell_value):
                        mask.iloc[i, j] = "data-point"

        # Rule 5: Row consistency enforcement
        mask = self._enforce_row_consistency(mask)

        return mask

    def _enforce_row_consistency(self, mask: pd.DataFrame) -> pd.DataFrame:
        """
        If a row is more features or data points, change all cells to that classification,
        given that more than 30% of the row is that classification.
        "None" values are treated as neutral and never changed.

        Args:
            mask: Current mask DataFrame

        Returns:
            Updated mask with row consistency enforced
        """
        for idx, row in mask.iterrows():
            # Count occurrences of each classification (excluding None)
            feature_count = (row == "feature").sum()
            datapoint_count = (row == "data-point").sum()
            none_count = (row == "None").sum()
            undecided_count = (row == "undecided").sum()

            # Total cells excluding None values for percentage calculation
            total_non_none_cells = len(row) - none_count

            # Skip if no non-None cells to evaluate
            if total_non_none_cells == 0:
                continue

            # Determine which classification should be applied
            target_classification = None
            if feature_count > datapoint_count:
                if feature_count / total_non_none_cells >= self.consistency_threshold:
                    target_classification = "feature"
            elif datapoint_count > feature_count:
                if datapoint_count / total_non_none_cells >= self.consistency_threshold:
                    target_classification = "data-point"

            # Apply the target classification only to non-None cells
            if target_classification:
                for col_idx, cell_value in enumerate(row):
                    if cell_value != "None":  # Protect None values from being overwritten
                        mask.iloc[idx, col_idx] = target_classification

        return mask

    def _check_row_identity(self, row: pd.Series) -> bool:
        """
        Check if a row meets the identity threshold.
        Missing values are excluded from identity calculations.

        Args:
            row: A row from the DataFrame

        Returns:
            True if >= threshold of non-missing cells are identical
        """
        if len(row) == 0:
            return False

        # Filter out missing values before checking identity
        non_missing_values = []
        for cell_value in row:
            if not self._is_missing_value(cell_value):
                non_missing_values.append(str(cell_value).strip())

        # If no non-missing values or a singular value, cannot determine identity
        if len(non_missing_values) < 2:
            return False

        # Convert to Series for value_counts
        cleaned_row = pd.Series(non_missing_values)
        value_counts = cleaned_row.value_counts()

        if len(value_counts) == 0:
            return False

        most_common_count = value_counts.iloc[0]
        ratio = most_common_count / len(non_missing_values)

        return ratio >= self.threshold

    def _is_numeric_pattern(self, cell: str) -> bool:
        """
        Check if cell matches any numeric pattern.

        Args:
            cell: Cell value as string

        Returns:
            True if cell matches numeric patterns
        """
        # Pattern 1: Decimal numbers (e.g., 123.45)
        if re.match(r'^\d+\.\d+', cell):
            return True

        # Pattern 2: Numbers with thousand separators (e.g., 1,234)
        if re.match(r'^\d{1,3}(,\d{3})+', cell):
            return True

        # Pattern 3: Combined - thousand separators with decimal (e.g., 1,234.56)
        if re.match(r'^\d{1,3}(,\d{3})+\.\d+', cell):
            return True


        # Pattern 4: Whole numbers (excluding single digits and years 1900-2030)
        if re.match(r'^\d+$', cell):
            # Convert to int for range checking
            try:
                num = int(cell)
                # Exclude single digits (0-9) and years (1900-2030)
                if not (0 <= num <= 9 or 1900 <= num <= 2030):
                    return True
            except ValueError:
                # If conversion fails, it's not a valid number
                pass

        # Dash patterns
        if cell in ["-", "--", "---"]:
            return True

        return False

class LLMClassifier:
    """Handles LLM-based classification for undecided cells using Claude."""

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the LLM classifier with API credentials.

        Args:
            api_key: API key for Anthropic. Uses env var ANTHROPIC_API_KEY if None.
        """
        self.api_key = api_key
        self.client = None  # Will be initialized lazily

        # Model configurations for Claude
        self.MODELS = {
            'haiku_3': "claude-3-haiku-20240307",
            'haiku_3_5': "claude-3-5-haiku-latest",
            'sonnet_4': "claude-sonnet-4-20250514"
        }

        # Pricing configuration (per 1M tokens)
        self.PRICING = {
            'haiku_3': {'input': 0.25, 'output': 1.25},
            'haiku_3_5': {'input': 1.0, 'output': 5.0},
            'sonnet_4': {'input': 3.0, 'output': 15.0}
        }

        # Initialize tracking for all models
        self.usage_stats = {}
        for model_key in self.MODELS.keys():
            self.usage_stats[model_key] = {
                'calls': 0,
                'total_input_tokens': 0,
                'total_output_tokens': 0,
                'total_cost': 0.0,
                'tables_processed': 0
            }

        # Add tracking for model selection strategies
        self.strategy_stats = {
            'size_based_haiku': {'attempts': 0, 'successes': 0, 'dimensional_failures': 0},
            'size_based_sonnet': {'attempts': 0, 'successes': 0, 'dimensional_failures': 0},
            'dimensional_fallback': {'attempts': 0, 'successes': 0},
            'emergency_sonnet': {'attempts': 0, 'successes': 0}
        }

        # Track which model succeeded for each table
        self.successful_models = []

    def _estimate_table_size(self, table_df: pd.DataFrame) -> tuple[int, bool]:
        """
        Estimate table size and determine if Sonnet should be used.

        Args:
            table_df: Table data DataFrame

        Returns:
            Tuple of (number_of_rows, should_use_sonnet)
        """
        num_rows = len(table_df)
        should_use_sonnet = num_rows > 30

        return num_rows, should_use_sonnet

    def _initialize_client(self):
        """Lazy initialization of the Claude client."""
        if self.client is not None:
            return  # Already initialized

        import os

        # Initialize Claude client
        api_key_to_use = self.api_key or os.getenv("ANTHROPIC_API_KEY")

        if not api_key_to_use:
            raise ValueError("Claude API key must be provided or set in ANTHROPIC_API_KEY environment variable")

        self.client = anthropic.Anthropic(api_key=api_key_to_use)

    def _call_claude_model(self, prompt: str, model: str) -> tuple[str, int, int]:
        """
        Call Claude API with a specific model.

        Returns:
            Tuple of (response_text, input_tokens, output_tokens)
        """
        response = self.client.messages.create(
            model=model,
            max_tokens=4095,
            messages=[{"role": "user", "content": prompt}]
        )

        input_tokens = response.usage.input_tokens
        output_tokens = response.usage.output_tokens

        return response.content[0].text.strip(), input_tokens, output_tokens

    def _call_llm_with_model(self, prompt: str, model_key: str) -> str:
        """
        Call the Claude API with a specific model.

        Args:
            prompt: The prompt to send to the LLM
            model_key: The model key (e.g., 'haiku_3', 'haiku_3_5', 'sonnet_4')

        Returns:
            The response text from the LLM
        """
        # Ensure client is initialized
        self._initialize_client()

        # Get the actual model string
        model = self.MODELS[model_key]

        # Call Claude API
        response_text, input_tokens, output_tokens = self._call_claude_model(prompt, model)

        # Calculate cost based on model
        pricing = self.PRICING[model_key]
        input_cost = (input_tokens / 1_000_000) * pricing['input']
        output_cost = (output_tokens / 1_000_000) * pricing['output']
        total_cost = input_cost + output_cost

        # Update tracking statistics
        self.usage_stats[model_key]['calls'] += 1
        self.usage_stats[model_key]['total_input_tokens'] += input_tokens
        self.usage_stats[model_key]['total_output_tokens'] += output_tokens
        self.usage_stats[model_key]['total_cost'] += total_cost

        return response_text

    def _build_prompt(self, table_name: str, table_df: pd.DataFrame, partial_mask: pd.DataFrame) -> str:
        """
        Build the prompt for LLM classification with enhanced dimensional accuracy instructions.

        Args:
            table_name: Name of the table
            table_df: Table data
            partial_mask: Current partial mask

        Returns:
            Formatted prompt string with dimensional enforcement
        """
        # Convert DataFrames to lists for cleaner prompt
        table_list = table_df.values.tolist()
        mask_list = partial_mask.values.tolist()
        rows, cols = partial_mask.shape

        prompt = f"""
    DIMENSIONAL REQUIREMENTS - READ CAREFULLY:
    - Input table has EXACTLY {rows} rows and {cols} columns
    - You MUST return EXACTLY {rows} rows and {cols} columns
    - Count your output rows before responding: it must equal {rows}
    - Count your output columns in each row before responding: each must equal {cols}

    Table name: "{table_name}"

    Table content:
    {json.dumps(table_list, ensure_ascii=False, indent=2)}

    Current partial mask:
    {json.dumps(mask_list, ensure_ascii=False, indent=2)}

    CHECKPOINT 1: The input mask has {rows} rows and {cols} columns. Your output must match this EXACTLY.

    Note: The mask corresponds 1:1 with the table content - mask[i][j] classifies table_content[i][j].

    Task: Classify ONLY "undecided" cells as "feature" or "data-point" using row-based logic.

    CRITICAL DIMENSIONAL REQUIREMENTS:
    - Return EXACTLY {rows} rows and {cols} columns
    - DO NOT add extra rows
    - DO NOT remove any rows
    - DO NOT add extra columns
    - DO NOT remove any columns
    - Every row must have exactly {cols} elements

    CLASSIFICATION RULES:
    1. DO NOT change existing "feature" or "data-point" classifications
    2. DO NOT change or add "None" values under any circumstances - "None" represents missing/invalid data that must be preserved exactly as-is
    3. Classify entire rows consistently - all cells in the same row should have the same classification
    4. Use already-classified cells in the same row as guidance for "undecided" cells in that row
    5. Compare with similar rows that are already classified for consistency
    6. Only leave cells "undecided" if the entire row's purpose is truly ambiguous
    7. Be careful with identifying a row as "feature". Sometimes a row containing both numerical and textual values can be "data-point"

    ROW CLASSIFICATION LOGIC:
    - Headers/labels/descriptive text → "feature"
    - Actual values/measurements/specific instances → "data-point"

    ANALYSIS STEPS:
    1. Identify the row type (header, data, label)
    2. Use classified cells in the same row for guidance
    3. Compare with similar classified rows
    4. Consider table structure and name context
    5. Ensure row-level consistency

    IMPORTANT PATTERNS:
    1. If a row contains solo digits, it might be "data-point" or "feature". As a rule of thumb, if the digits appear as a pattern in a row (1, 2, 3,... or 1995, 2000, 2001, 2002,...) this row is a "feature", and if the cells in the row don't have a discernible pattern it is a "data-point" row.

    CHECKPOINT 2: Before generating your response, verify you understand you need {rows} rows and {cols} columns.

    IMPORTANT: "None" values represent missing or invalid data and must NEVER be changed. They are not a classification you should assign.

    FINAL INSTRUCTION: Return ONLY the complete mask as a JSON array with EXACTLY {rows} rows and {cols} columns.
    Every cell must be "feature", "data-point", "None", or "undecided".

    BEFORE YOU OUTPUT: Count your rows - you must have exactly {rows} rows. Count your columns in each row - each row must have exactly {cols} columns.
    """
        return prompt

    def _parse_response(self, response_text: str, partial_mask: pd.DataFrame) -> tuple[pd.DataFrame, bool, str]:
        """
        Parse LLM response and update the mask with enhanced dimensional failure detection.

        Args:
            response_text: Raw response from LLM
            partial_mask: Current mask to update

        Returns:
            Tuple of (Updated mask DataFrame, Success flag, Failure reason)
        """
        try:
            # Try to extract JSON from response
            # Handle case where LLM might include extra text
            json_start = response_text.find('[')
            json_end = response_text.rfind(']') + 1

            if json_start != -1 and json_end > json_start:
                json_str = response_text[json_start:json_end]
                mask_list = json.loads(json_str)

                # Create new mask DataFrame
                updated_mask = pd.DataFrame(mask_list)

                # Validate dimensions match
                if updated_mask.shape == partial_mask.shape:
                    # Validate None values are preserved
                    for i in range(len(partial_mask)):
                        for j in range(len(partial_mask.columns)):
                            original = partial_mask.iloc[i, j]
                            updated = updated_mask.iloc[i, j]

                            # Preserve already classified cells
                            if original in ["None", "feature", "data-point"]:
                                updated_mask.iloc[i, j] = original

                            # Check if a "None" value was added by the LLM and return to "undecided"
                            if original == "undecided" and updated == "None":
                                updated_mask.iloc[i, j] = original

                    # All checks passed
                    return updated_mask, True, "success"  # Success
                else:
                    print(f"Dimension mismatch in LLM response: Expected {partial_mask.shape}, got {updated_mask.shape}")
                    return partial_mask, False, "dimension_mismatch"  # Shape mismatch
            else:
                print("Could not find JSON array in response")
                return partial_mask, False, "json_parse_error"  # Parse failure

        except json.JSONDecodeError as e:
            print(f"JSON parsing error: {e}")
            return partial_mask, False, "json_decode_error"  # JSON error
        except Exception as e:
            print(f"Error parsing LLM response: {e}")
            return partial_mask, False, "general_error"  # General error

    def classify_undecided(self, table_name: str, table_df: pd.DataFrame, partial_mask: pd.DataFrame, model: str = None) -> pd.DataFrame:
        """
        Classify remaining undecided cells using Claude LLM with smart model selection and dimensional fallback.
        Preserves "None" values from HardRuleClassifier output - they will never be changed.

        Args:
            table_name: Name/title of the table
            table_df: Original table data
            partial_mask: Current mask with some classifications (may include "None" values from HardRuleClassifier)
            model: Optional specific model to use ('haiku_3', 'haiku_3_5', 'sonnet_4').
                   If None, uses smart selection based on table size.

        Returns:
            Updated mask DataFrame with "None" values preserved exactly as input
        """
        # Smart model selection based on table size (if model not specified)
        if model is not None and model in self.MODELS:
            # Use specified model
            model_order = [model]
            strategy_used = None
            print(f"Using specified model: {model}")
        else:
            # Smart selection based on table size
            num_rows, should_use_sonnet = self._estimate_table_size(table_df)

            if should_use_sonnet:
                print(f"Table has {num_rows} rows (> 30), using Sonnet for reliability...")
                model_order = ['sonnet_4', 'haiku_3_5', 'haiku_3']
                strategy_used = 'size_based_sonnet'
            else:
                print(f"Table has {num_rows} rows (≤ 30), using Haiku for cost efficiency...")
                model_order = ['haiku_3', 'haiku_3_5', 'sonnet_4']
                strategy_used = 'size_based_haiku'

            # Track strategy attempt
            if strategy_used:
                self.strategy_stats[strategy_used]['attempts'] += 1

        # Build prompt
        prompt = self._build_prompt(table_name, table_df, partial_mask)

        # Track if we need Sonnet fallback for dimensional failures
        haiku_dimensional_failure = False
        primary_strategy_succeeded = False

        # Try each model in sequence
        for i, model_key in enumerate(model_order):
            try:
                print(f"Trying Claude/{model_key}...")
                response_text = self._call_llm_with_model(prompt, model_key)

                # Add validation here
                if not response_text or response_text.strip() == "":
                    print(f"Claude/{model_key} returned empty response. Trying next model...")
                    continue

                final_mask, success, failure_reason = self._parse_response(response_text, partial_mask)

                # Validate the return values
                if final_mask is None:
                    print(f"Claude/{model_key} parsing returned None. Trying next model...")
                    continue

                if success:
                    # Track successful model for statistics
                    self.usage_stats[model_key]['tables_processed'] += 1
                    self.successful_models.append(model_key)

                    # Track strategy success
                    if strategy_used and i == 0:  # First model succeeded
                        primary_strategy_succeeded = True
                        self.strategy_stats[strategy_used]['successes'] += 1

                    print(f"Successfully classified with Claude/{model_key}")
                    return final_mask
                else:
                    # Check if this was a Haiku dimensional failure
                    if model_key in ['haiku_3', 'haiku_3_5'] and failure_reason == "dimension_mismatch":
                        haiku_dimensional_failure = True
                        if strategy_used and i == 0:  # Primary model failed
                            self.strategy_stats[strategy_used]['dimensional_failures'] += 1
                        print(f"Claude/{model_key} failed with dimensional mismatch. Will prioritize Sonnet fallback...")

                    print(f"Claude/{model_key} failed validation ({failure_reason}). Trying next model...")

            except Exception as e:
                print(f"Claude/{model_key} API call failed: {e}. Trying next model...")

        # Special handling for Haiku dimensional failures - try Sonnet if not already tried
        if haiku_dimensional_failure and 'sonnet_4' not in [m for m in model_order if m == 'sonnet_4'][:1]:
            print("Haiku had dimensional failures. Trying Sonnet as emergency fallback...")
            self.strategy_stats['emergency_sonnet']['attempts'] += 1

            try:
                response_text = self._call_llm_with_model(prompt, 'sonnet_4')

                if response_text and response_text.strip() != "":
                    final_mask, success, failure_reason = self._parse_response(response_text, partial_mask)

                    if success:
                        self.usage_stats['sonnet_4']['tables_processed'] += 1
                        self.successful_models.append('sonnet_4')
                        self.strategy_stats['emergency_sonnet']['successes'] += 1

                        print(f"Emergency Sonnet fallback successful!")
                        return final_mask
                    else:
                        print(f"Emergency Sonnet fallback also failed ({failure_reason})")
            except Exception as e:
                print(f"Emergency Sonnet fallback failed: {e}")

        # If all models fail, return partial mask
        print(f"All Claude models failed to generate correct shape. Using partial mask.")
        return partial_mask

    def get_usage_summary(self):
        """
        Print a comprehensive summary of API usage and costs.

        Returns:
            Dictionary containing the usage statistics
        """
        print("\n" + "="*60)
        print(" CLAUDE LLM CLASSIFIER USAGE SUMMARY")
        print("="*60)

        total_cost = 0
        total_calls = 0
        total_tables = len(self.successful_models)

        # Print stats for Claude models
        print("\nCLAUDE MODELS:")
        print("-" * 40)

        # Get model keys in the correct order
        model_keys = ['haiku_3', 'haiku_3_5', 'sonnet_4']

        for model_key in model_keys:
            stats = self.usage_stats[model_key]

            if stats['calls'] > 0:
                avg_input = stats['total_input_tokens'] / stats['calls']
                avg_output = stats['total_output_tokens'] / stats['calls']
                avg_cost = stats['total_cost'] / stats['calls']

                # Get model display name
                model_name = self.MODELS[model_key]

                print(f"\n  {model_name}:")
                print(f"    API Calls Made: {stats['calls']}")
                print(f"    Tables Successfully Processed: {stats['tables_processed']}")
                success_rate = (stats['tables_processed']/stats['calls']*100) if stats['calls'] > 0 else 0
                print(f"    Success Rate: {success_rate:.1f}%")
                print(f"    Token Usage:")
                print(f"      Total Input: {stats['total_input_tokens']:,} | Avg: {avg_input:,.0f}")
                print(f"      Total Output: {stats['total_output_tokens']:,} | Avg: {avg_output:,.0f}")
                print(f"    Cost:")
                print(f"      Total: ${stats['total_cost']:.6f} | Per Call: ${avg_cost:.6f}")
                if stats['tables_processed'] > 0:
                    print(f"      Per Successful Table: ${stats['total_cost']/stats['tables_processed']:.6f}")

                # Update overall totals
                total_cost += stats['total_cost']
                total_calls += stats['calls']

        # Add strategy analytics section
        print("\n" + "="*60)
        print("SMART MODEL SELECTION ANALYTICS:")
        print("-" * 40)

        # Size-based strategy performance
        haiku_stats = self.strategy_stats['size_based_haiku']
        sonnet_stats = self.strategy_stats['size_based_sonnet']

        if haiku_stats['attempts'] > 0:
            haiku_success_rate = (haiku_stats['successes'] / haiku_stats['attempts']) * 100
            haiku_dim_failure_rate = (haiku_stats['dimensional_failures'] / haiku_stats['attempts']) * 100

            print(f"\nSize-based Haiku Strategy (≤30 rows):")
            print(f"  Tables Attempted: {haiku_stats['attempts']}")
            print(f"  Primary Success Rate: {haiku_success_rate:.1f}%")
            print(f"  Dimensional Failure Rate: {haiku_dim_failure_rate:.1f}%")

        if sonnet_stats['attempts'] > 0:
            sonnet_success_rate = (sonnet_stats['successes'] / sonnet_stats['attempts']) * 100
            sonnet_dim_failure_rate = (sonnet_stats['dimensional_failures'] / sonnet_stats['attempts']) * 100

            print(f"\nSize-based Sonnet Strategy (>30 rows):")
            print(f"  Tables Attempted: {sonnet_stats['attempts']}")
            print(f"  Primary Success Rate: {sonnet_success_rate:.1f}%")
            print(f"  Dimensional Failure Rate: {sonnet_dim_failure_rate:.1f}%")

        # Emergency fallback performance
        emergency_stats = self.strategy_stats['emergency_sonnet']
        if emergency_stats['attempts'] > 0:
            emergency_success_rate = (emergency_stats['successes'] / emergency_stats['attempts']) * 100

            print(f"\nEmergency Sonnet Fallback:")
            print(f"  Fallback Attempts: {emergency_stats['attempts']}")
            print(f"  Fallback Success Rate: {emergency_success_rate:.1f}%")
            print(f"  Cost Impact: {emergency_stats['attempts']} additional Sonnet calls")

        # Overall strategy effectiveness
        total_attempts = haiku_stats['attempts'] + sonnet_stats['attempts']
        total_primary_successes = haiku_stats['successes'] + sonnet_stats['successes']

        if total_attempts > 0:
            overall_primary_success_rate = (total_primary_successes / total_attempts) * 100
            total_dimensional_failures = haiku_stats['dimensional_failures'] + sonnet_stats['dimensional_failures']
            overall_dim_failure_rate = (total_dimensional_failures / total_attempts) * 100

            print(f"\nOverall Strategy Performance:")
            print(f"  Primary Model Success Rate: {overall_primary_success_rate:.1f}%")
            print(f"  Dimensional Failure Rate: {overall_dim_failure_rate:.1f}%")
            print(f"  Tables Saved from Expensive Retries: {total_primary_successes}")

        # Overall statistics
        print("\n" + "="*60)
        print("OVERALL STATISTICS:")
        print("-" * 40)
        print(f"  Total Tables Processed: {total_tables}")
        print(f"  Total API Calls: {total_calls}")
        print(f"  Total Cost: ${total_cost:.6f}")
        if total_tables > 0:
            print(f"  Average Cost per Table: ${total_cost/total_tables:.6f}")
            print(f"  Average Calls per Table: {total_calls/total_tables:.2f}")

        # Model success distribution
        if self.successful_models:
            print("\n  Model Success Distribution:")
            model_counts = Counter(self.successful_models)
            for model, count in model_counts.most_common():
                percentage = (count / total_tables) * 100
                model_name = self.MODELS[model]
                print(f"    {model_name}: {count} tables ({percentage:.1f}%)")

        print("="*60 + "\n")

        return self.usage_stats

    def reset_usage_stats(self):
        """Reset all usage statistics to zero."""
        for model_key in self.MODELS.keys():
            self.usage_stats[model_key] = {
                'calls': 0,
                'total_input_tokens': 0,
                'total_output_tokens': 0,
                'total_cost': 0.0,
                'tables_processed': 0
            }
        self.successful_models = []

        # Reset strategy stats
        self.strategy_stats = {
            'size_based_haiku': {'attempts': 0, 'successes': 0, 'dimensional_failures': 0},
            'size_based_sonnet': {'attempts': 0, 'successes': 0, 'dimensional_failures': 0},
            'dimensional_fallback': {'attempts': 0, 'successes': 0},
            'emergency_sonnet': {'attempts': 0, 'successes': 0}
        }

        print("Usage statistics and strategy analytics have been reset.")

class TableClassifier:
    """Orchestrates the entire table classification process."""

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the complete classification system.

        Args:
            api_key: Anthropic API key (uses env var if None)
        """
        self.loader = TableLoader()
        self.hard_classifier = HardRuleClassifier()
        self.llm_classifier = LLMClassifier(api_key)

    def classify_table(self, csv_path: str, table_name: str) -> pd.DataFrame:
        """
        Complete classification pipeline for a table.

        Args:
            csv_path: Path to the CSV file
            table_name: Name/title of the table for context

        Returns:
            Final mask DataFrame with classifications
        """
        # Step 1: Load and clean the table
        table_df = self.loader.load_and_clean(csv_path)

        # Step 2: Apply hard-coded rules
        partial_mask = self.hard_classifier.classify(table_df)
        # print(f"Partial mask shape: {partial_mask.shape}")

        # Step 3: Apply LLM classification for undecided cells only if cells are left "undecided"
        if "undecided" in partial_mask.values:
            final_mask = self.llm_classifier.classify_undecided(table_name, table_df, partial_mask)
        else:
            final_mask = partial_mask

        return final_mask

    def load_table_names(self, summary_path: str = "/content/tables_summary.json") -> dict:
        """
        Load table name mappings from the summary JSON file.

        Args:
            summary_path: Path to the tables_summary.json file

        Returns:
            Dictionary mapping CSV identifiers to table names
        """
        try:
            with open(summary_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Warning: {summary_path} not found. Using filenames as table names.")
            return {}
        except json.JSONDecodeError as e:
            print(f"Error parsing {summary_path}: {e}. Using filenames as table names.")
            return {}

    def process_single_table(self, input_path: str, output_path: str, table_name: str) -> bool:
        """
        Process a single table and save its classification mask.

        Args:
            input_path: Path to the input CSV file
            output_path: Path where the mask CSV should be saved
            table_name: Human-readable name of the table for LLM context

        Returns:
            True if processing succeeded, False otherwise
        """
        try:
            # print(f"Processing {os.path.splitext(os.path.basename(input_path))[0]}")
            # Process the table using existing classification pipeline
            mask_df = self.classify_table(input_path, table_name)

            # Save the mask to CSV
            mask_df.to_csv(output_path, index=False)

            return True

        except Exception as e:
            print(f"Failed to process {os.path.basename(input_path)}: {e}")
            return False

    def process_all_tables(self,
                          input_dir: str = "/content/drive/MyDrive/DSSG/tables/",
                          output_dir: str = "/content/mask",
                          summary_path: str = "/content/drive/MyDrive/DSSG/tables/tables_summary.json",
                          years: Optional[Iterable[int]] = None,
                          chapters: Optional[Iterable[int]] = None,
                          parallel: bool = False,
                          skip_existing: bool = True,
                          max_workers: int = 4) -> None:
        """
        Process all CSV files in the hierarchical directory structure and save masks to output directory.

        Args:
            input_dir: Base directory containing input CSV files organized by year/chapter
            output_dir: Base directory where mask CSV files will be saved
            summary_path: Path to the tables_summary.json file
            years: Iterable of years to process (list, range, numpy array, etc.) (default: 2001-2024)
            chapters: Iterable of chapters to process (list, range, numpy array, etc.) (default: 1-15)
            parallel: Whether to use parallel processing (default: False)
            skip_existing: Whether to skip already processed files (default: True)
            max_workers: Maximum number of parallel workers if parallel=True (default: 4)
        """
        # Set defaults if not provided
        if years is None:
            years = range(2001, 2025)
        if chapters is None:
            chapters = range(1, 16)  # Chapters 1-15

        # Load table name mappings
        table_names = self.load_table_names(summary_path)

        # Collect all files to process (for parallel processing or progress tracking)
        files_to_process = []

        # Iterate through year/chapter combinations

        for chapter in chapters:
            for year in years:
                chapter_year = f"{year}/{chapter:02d}"
                input_path = Path(input_dir) / chapter_year

                # Check if folder exists
                if not input_path.exists():
                    logging.warning(f"Folder does not exist: {input_path}")
                    continue

                # Get all CSV files in this folder
                csv_files = list(input_path.glob("*.csv"))

                if not csv_files:
                    logging.warning(f"No CSV files found in {input_path}")
                    continue

                # Process each CSV file
                for csv_file in csv_files:
                    # Get filename without extension for identifier lookup
                    identifier = csv_file.stem

                    # Get table name from mapping or use filename
                    table_name = table_names.get(identifier, identifier)

                    # Construct output path with same filename
                    output_path = Path(output_dir) / chapter_year / csv_file.name

                    # Check if we should skip this file
                    output_drive_path = Path("/content/drive/MyDrive/DSSG/mask/mask") / chapter_year / csv_file.name
                    if skip_existing and (output_path.exists() or output_drive_path.exists()):
                        logging.info(f"Skipping existing file: {identifier}")
                        continue

                    if parallel:
                        # Collect for parallel processing
                        files_to_process.append((str(csv_file), str(output_path), table_name))
                    else:
                        # Process immediately (sequential)
                        print(f"Processing: {identifier}")

                        # Ensure the output directory exists
                        self.ensure_output_directory(str(output_path.parent))

                        try:
                            # Process the table
                            self.process_single_table(str(csv_file), str(output_path), table_name)
                        except Exception as e:
                            logging.error(f"Error processing {identifier}: {str(e)}")
                            continue

        # If parallel processing is enabled, process all collected files
        if parallel:
            self.process_tables_parallel(files_to_process, max_workers)

    def process_tables_parallel(self, files_to_process: list, max_workers: int = 4) -> None:
        """
        Process tables in parallel using concurrent.futures.

        Args:
            files_to_process: List of tuples (input_path, output_path, table_name)
            max_workers: Maximum number of parallel workers
        """

        if not files_to_process:
            print("No files to process")
            return

        print(f"Processing {len(files_to_process)} files in parallel with {max_workers} workers")

        def process_file(args):
            input_path, output_path, table_name = args
            identifier = Path(input_path).stem

            try:
                # Ensure the output directory exists
                self.ensure_output_directory(str(Path(output_path).parent))

                # Process the table
                self.process_single_table(input_path, output_path, table_name)
                return f"Success: {identifier}"
            except Exception as e:
                return f"Error processing {identifier}: {str(e)}"

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            futures = {executor.submit(process_file, args): args for args in files_to_process}

            # Process completed tasks
            for future in as_completed(futures):
                result = future.result()
                if result.startswith("Error"):
                    logging.error(result)
                else:
                    print(result)

    def ensure_output_directory(self, output_dir: str) -> None:
        """
        Ensure the output directory exists, create it if necessary.

        Args:
            output_dir: Path to the output directory
        """
        Path(output_dir).mkdir(parents=True, exist_ok=True)

    def usage_summary(self):
        return self.llm_classifier.get_usage_summary()
