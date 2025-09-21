import json
from dataclasses import dataclass
from typing import Optional

@dataclass
class MatchingConfig:
    tables_dir: str = "/content/tables"
    reference_json: str = "tables_summary.json"
    mask_dir: str = "/content/mask"  # New field for mask directory
    output_dir: str = "output"

    similarity_threshold: float = 0.78 # changed here
    confident_threshold: float = 0.97
    split_threshold: float = 0.80
    merge_threshold: float = 0.80
    max_gap_years: int = 3

    use_api_validation: bool = False
    api_key: Optional[str] = None

    def save(self, path="config.json"):
        with open(path, 'w') as f:
            json.dump(self.__dict__, f, indent=2)