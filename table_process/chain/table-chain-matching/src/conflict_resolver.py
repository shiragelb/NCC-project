class ConflictResolver:
    def __init__(self):
        self.conflicts = {}
        self.resolutions = {}

    def detect_conflicts(self, sim_matrix, threshold=0.85):
        """Detect all conflicts"""
        matrix = sim_matrix['matrix']
        chain_ids = sim_matrix['chain_ids']
        table_ids = sim_matrix['table_ids']

        for j, table_id in enumerate(table_ids):
            claimants = []
            for i, chain_id in enumerate(chain_ids):
                if matrix[i, j] >= threshold:
                    claimants.append((chain_id, matrix[i, j]))

            if len(claimants) > 1:
                self.conflicts[table_id] = {
                    'claimants': claimants,
                    'max_similarity': max(c[1] for c in claimants)
                }

        return self.conflicts

    def resolve_conflicts(self, conflicts, api_validator=None):
        """Resolve all conflicts"""
        for table_id, conflict in conflicts.items():
            if api_validator:
                resolution = api_validator.validate_conflict(
                    table_id, conflict['claimants']
                )
                self.resolutions[table_id] = resolution
            else:
                # Default: highest similarity wins
                winner = max(conflict['claimants'], key=lambda x: x[1])
                self.resolutions[table_id] = {
                    'winning_chain': winner[0],
                    'confidence': winner[1]
                }

        return self.resolutions
    