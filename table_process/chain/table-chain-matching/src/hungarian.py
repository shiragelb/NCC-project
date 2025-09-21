from scipy.optimize import linear_sum_assignment

class HungarianMatcher:
    def __init__(self, threshold=0.85):
        self.threshold = threshold

    def find_optimal_matching(self, sim_matrix):
        matrix = sim_matrix['matrix']
        chain_ids = sim_matrix['chain_ids']
        table_ids = sim_matrix['table_ids']

        # Convert to cost matrix
        cost = 1 - matrix
        row_ind, col_ind = linear_sum_assignment(cost)

        matches = []
        for i, j in zip(row_ind, col_ind):
            if i < len(chain_ids) and j < len(table_ids):
                similarity = matrix[i, j]
                if similarity >= self.threshold:
                    matches.append((chain_ids[i], table_ids[j], similarity))

        unmatched_chains = [c for i, c in enumerate(chain_ids)
                           if i not in row_ind]
        unmatched_tables = [t for j, t in enumerate(table_ids)
                           if j not in col_ind]

        return {
            'matches': matches,
            'unmatched_chains': unmatched_chains,
            'unmatched_tables': unmatched_tables
        }