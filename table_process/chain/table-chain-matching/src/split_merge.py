class SplitMergeDetector:
    def __init__(self, split_threshold=0.80, merge_threshold=0.80):
        self.split_threshold = split_threshold
        self.merge_threshold = merge_threshold

    def detect_splits(self, sim_matrix):
        splits = []
        matrix = sim_matrix['matrix']
        chain_ids = sim_matrix['chain_ids']
        table_ids = sim_matrix['table_ids']

        for i, chain_id in enumerate(chain_ids):
            high_sim_tables = []
            for j, table_id in enumerate(table_ids):
                if matrix[i, j] >= self.split_threshold:
                    high_sim_tables.append((table_id, matrix[i, j]))

            if len(high_sim_tables) >= 2:
                splits.append({
                    'chain': chain_id,
                    'targets': high_sim_tables
                })

        return splits

    def detect_merges(self, sim_matrix):
        merges = []
        matrix = sim_matrix['matrix']
        chain_ids = sim_matrix['chain_ids']
        table_ids = sim_matrix['table_ids']

        for j, table_id in enumerate(table_ids):
            high_sim_chains = []
            for i, chain_id in enumerate(chain_ids):
                if matrix[i, j] >= self.merge_threshold:
                    high_sim_chains.append((chain_id, matrix[i, j]))

            if len(high_sim_chains) >= 2:
                merges.append({
                    'table': table_id,
                    'sources': high_sim_chains
                })

        return merges