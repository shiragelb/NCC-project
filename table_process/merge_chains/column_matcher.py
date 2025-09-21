"""
Column Matcher module
Matches columns across years using edit distance and semantic similarity
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Tuple, Optional
import Levenshtein
import logging

logger = logging.getLogger(__name__)

class ColumnMatcher:
    def __init__(self, config: Dict):
        self.config = config
        # Thresholds for column matching decisions
        self.auto_accept_threshold = config['matching']['auto_accept_threshold']  # >= this: auto-accept
        self.manual_threshold = config['matching']['manual_review_threshold']  # < this: ignore completely
        # Between manual_threshold and auto_accept_threshold: would call API for verification
        self.edit_distance_threshold = config['matching']['edit_distance_threshold']

        # Initialize embeddings handler ONCE if semantic matching is enabled
        self.embeddings_handler = None
        if config['matching'].get('use_semantic_matching', False):
            try:
                from embeddings_handler import EmbeddingsHandler
                self.embeddings_handler = EmbeddingsHandler()
                logger.info("Initialized embeddings handler for semantic matching")
            except Exception as e:
                logger.warning(f"Could not initialize embeddings: {e}")

    def match_columns_to_schema(self, schema: Dict, new_table: pd.DataFrame, year: int) -> List[Dict]:
        """
        Match columns from new table to existing schema.
        Decision logic:
        - Similarity >= auto_accept_threshold: Automatically accept the match
        - manual_threshold <= Similarity < auto_accept_threshold: API verification needed (medium confidence)
        - Similarity < manual_threshold: Ignore the match (too low confidence)
        """
        if not schema.get('columns'):
            return []

        existing_columns = schema['columns']
        new_columns = list(new_table.columns)

        # Build similarity matrix
        sim_matrix = self.build_similarity_matrix(existing_columns, new_columns)

        # Apply matching with thresholds
        matches = self.apply_matching_with_thresholds(sim_matrix, existing_columns, new_columns)
        return matches

    def build_similarity_matrix(self, existing_columns: List, new_columns: List) -> np.ndarray:
        """Build similarity matrix between existing and new columns"""
        n_existing = len(existing_columns)
        n_new = len(new_columns)
        matrix = np.zeros((n_existing, n_new))

        for i, existing_col in enumerate(existing_columns):
            for j, new_col in enumerate(new_columns):
                # Get the most recent name from existing column
                col_name = existing_col.get('current_name', '')
                matrix[i, j] = self.calculate_similarity(col_name, new_col)

        return matrix

    def calculate_similarity(self, col1: str, col2: str) -> float:
        """Calculate similarity between two column names using semantic embeddings"""
        # Exact match
        if col1 == col2:
            return 1.0

        # Use semantic similarity if handler is available
        if self.embeddings_handler:
            try:
                from sklearn.metrics.pairwise import cosine_similarity

                emb1 = self.embeddings_handler.get_embedding(str(col1))
                emb2 = self.embeddings_handler.get_embedding(str(col2))

                similarity = cosine_similarity(emb1.reshape(1, -1),
                                              emb2.reshape(1, -1))[0, 0]
                return float(similarity)
            except Exception as e:
                logger.warning(f"Semantic similarity failed: {e}")

        # Fallback to edit distance
        import Levenshtein
        distance = Levenshtein.distance(str(col1), str(col2))
        max_len = max(len(str(col1)), len(str(col2)))
        return 1 - (distance / max_len) if max_len > 0 else 0.0

    def normalize_hebrew_text(self, text: str) -> str:
        """Normalize Hebrew text for comparison"""
        # Handle integer column names from header=None
        if not isinstance(text, str):
            text = str(text)
        if not text:
            return ""

        # Remove common Hebrew punctuation and normalize spaces
        import re
        text = re.sub(r'[״"׳\'\-\*\(\)]', '', text)
        text = ' '.join(text.split())
        return text.strip()

    def apply_matching_with_thresholds(self, sim_matrix: np.ndarray, existing_columns: List,
                                       new_columns: List) -> List[Dict]:
        """
        Apply matching with three confidence levels:
        1. High confidence (>= auto_accept_threshold): Auto-accept, no API needed
        2. Medium confidence (between thresholds): Would need API verification
        3. Low confidence (< manual_threshold): Ignore completely, don't waste API calls
        """
        matches = []
        used_existing = set()
        used_new = set()

        # Collect all possible matches with their confidence levels
        all_matches = []
        for i in range(len(existing_columns)):
            for j in range(len(new_columns)):
                score = sim_matrix[i, j]
                # Only consider matches above the manual threshold
                if score >= self.manual_threshold:
                    confidence_level = 'high' if score >= self.auto_accept_threshold else 'medium'
                    all_matches.append({
                        'existing_idx': i,
                        'new_idx': j,
                        'score': score,
                        'confidence_level': confidence_level,
                        'needs_api': confidence_level == 'medium'
                    })

        # Sort by score (descending) to prioritize best matches
        all_matches.sort(key=lambda x: x['score'], reverse=True)

        # Greedily select best matches
        for match in all_matches:
            if match['existing_idx'] not in used_existing and match['new_idx'] not in used_new:
                matches.append(match)
                used_existing.add(match['existing_idx'])
                used_new.add(match['new_idx'])

                # Log the decision
                if match['confidence_level'] == 'high':
                    logger.info(f"Auto-accepted match with score {match['score']:.2f}")
                else:
                    logger.info(f"Medium confidence match (score {match['score']:.2f}) - API verification needed")

        # Log summary
        high_conf = len([m for m in matches if m['confidence_level'] == 'high'])
        med_conf = len([m for m in matches if m['confidence_level'] == 'medium'])
        logger.info(f"Matching results: {high_conf} auto-accepted, {med_conf} need API verification")

        return matches
