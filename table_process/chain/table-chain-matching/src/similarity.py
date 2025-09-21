import numpy as np
from scipy.spatial.distance import cosine

class SimilarityBuilder:
    def compute_similarity_matrix(self, chain_embeddings, table_embeddings):
        chain_ids = list(chain_embeddings.keys())
        table_ids = list(table_embeddings.keys())

        n_chains = len(chain_ids)
        n_tables = len(table_ids)

        matrix = np.zeros((n_chains, n_tables))

        for i, chain_id in enumerate(chain_ids):
            for j, table_id in enumerate(table_ids):
                # Cosine similarity
                sim = 1 - cosine(chain_embeddings[chain_id],
                                 table_embeddings[table_id])
                matrix[i, j] = (sim + 1) / 2  # Normalize to [0,1]

        return {
            'matrix': matrix,
            'chain_ids': chain_ids,
            'table_ids': table_ids
        }