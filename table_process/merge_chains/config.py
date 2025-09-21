"""
Configuration module for Table Chain Merger
Handles all configuration settings and defaults
"""

import os
import json
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from datetime import datetime
import uuid

# Configuration
DEFAULT_CONFIG = {
    'api': {
        'provider': 'anthropic',
        'model': 'claude-3',
        'max_calls_per_chain': 100,
        'confidence_threshold': 0.7,
        'rate_limit': 10,
        'timeout': 30
    },
    'matching': {
        'auto_accept_threshold': 0.85,
        'manual_review_threshold': 0.5,
        'use_embeddings': False,  # Set to False for initial implementation
        'embedding_model': 'alephbert-base',
        'edit_distance_threshold': 3,
        'use_pattern_library': True,
        'semantic_similarity_alpha': 0.7,  # Threshold for replacing vs chaining
        'use_semantic_matching': True,     # Enable semantic similarity
    },
    'processing': {
        'auto_invoke_reasoning': False,
        'handle_unit_changes': True,
        'max_table_size_mb': 500
    },
    'output': {
        'format': 'csv',
        'encoding': 'utf-8-sig',
        'include_metadata': True,
        'include_validation_report': True,
        'create_backup': True
    },
    'caching': {
        'enable_persistent_cache': True,
        'cache_directory': './cache',
        'max_cache_size_mb': 1000,
        'cache_expiry_days': 30
    }
}

@dataclass
class ColumnSchema:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    current_name: str = ""
    name_history: List[Dict] = field(default_factory=list)
    years_present: List[int] = field(default_factory=list)
    data_type: str = "text"
    confidence_scores: Dict[int, float] = field(default_factory=dict)
    sample_values: List = field(default_factory=list)
    statistics: Dict = field(default_factory=dict)

@dataclass
class Issue:
    type: str  # 'distortion', 'unit_change', 'low_confidence'
    description: str
    severity: float  # 0-1
    affected_years: List[int] = field(default_factory=list)
    affected_columns: List[str] = field(default_factory=list)

def load_config(config_path: Optional[str] = None) -> Dict:
    """Load configuration from file or use defaults"""
    if config_path and os.path.exists(config_path):
        with open(config_path, 'r') as f:
            user_config = json.load(f)
        # Merge with defaults
        config = {**DEFAULT_CONFIG, **user_config}
    else:
        config = DEFAULT_CONFIG
    return config
