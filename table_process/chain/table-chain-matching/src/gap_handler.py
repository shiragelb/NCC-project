import numpy as np

class GapHandler:
    def __init__(self, max_gap_years=3, reactivation_threshold=0.90):
        self.max_gap_years = max_gap_years
        self.reactivation_threshold = reactivation_threshold
        self.dormant_chains = {}
        self.ended_chains = {}

    def check_gaps(self, chains, current_year, matched_chains):
        """Check for gaps and handle dormant chains"""
        gap_report = {
            'new_dormant': [],
            'reactivated': [],
            'ended': [],
            'continuing_gaps': []
        }

        for chain_id, chain in chains.items():
            if chain['status'] == 'active' and chain_id not in matched_chains:
                # Chain has no match this year
                last_year = chain['years'][-1] if chain['years'] else 0
                gap_length = current_year - last_year

                if gap_length > self.max_gap_years:
                    # End chain
                    chain['status'] = 'ended'
                    self.ended_chains[chain_id] = chain
                    gap_report['ended'].append(chain_id)
                else:
                    # Mark dormant
                    chain['status'] = 'dormant'
                    chain['dormant_since'] = current_year
                    self.dormant_chains[chain_id] = chain
                    gap_report['new_dormant'].append(chain_id)

        return gap_report

    def check_reactivation(self, dormant_chain, new_tables, embeddings):
        """Check if dormant chain can be reactivated"""
        if dormant_chain['tables']:
            last_table = dormant_chain['tables'][-1]
            if last_table in embeddings:
                chain_emb = embeddings[last_table]

                candidates = []
                for table_id in new_tables:
                    if table_id in embeddings:
                        table_emb = embeddings[table_id]
                        similarity = self._compute_similarity(chain_emb, table_emb)

                        if similarity >= self.reactivation_threshold:
                            candidates.append((table_id, similarity))

                if candidates:
                    return max(candidates, key=lambda x: x[1])
        return None

    def _compute_similarity(self, emb1, emb2):
        """Compute cosine similarity"""
        from scipy.spatial.distance import cosine
        return (1 - cosine(emb1, emb2) + 1) / 2