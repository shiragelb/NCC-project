import numpy as np
from enum import Enum
from datetime import datetime

class RelationshipType(Enum):
    ONE_TO_ONE = "1:1"
    ONE_TO_MANY = "1:N"
    MANY_TO_ONE = "N:1"
    MANY_TO_MANY = "N:N"

class ComplexRelationshipDetector:
    def __init__(self):
        self.complex_relationships = []

    def detect_complex(self, sim_matrix, splits, merges):
        """Detect N:N complex reorganizations"""
        split_tables = set()
        for split in splits:
            split_tables.update([t[0] for t in split['targets']])

        merge_chains = set()
        for merge in merges:
            merge_chains.update([c[0] for c in merge['sources']])

        # Find overlapping splits and merges (N:N)
        for split in splits:
            if split['chain'] in merge_chains:
                for merge in merges:
                    if split['chain'] in [c[0] for c in merge['sources']]:
                        self.complex_relationships.append({
                            'type': RelationshipType.MANY_TO_MANY,
                            'chains': list(set([split['chain']] + [c[0] for c in merge['sources']])),
                            'tables': list(set([merge['table']] + [t[0] for t in split['targets']])),
                            'confidence': 0.7
                        })

        return self.complex_relationships