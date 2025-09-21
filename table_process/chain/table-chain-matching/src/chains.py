from collections import defaultdict

class ChainManager:
    def __init__(self):
        self.chains = {}
        self.match_details = {}  # Store similarity scores and API usage

    def initialize_from_first_year(self, tables):
        for table_id, metadata in tables.items():
            chain_id = f"chain_{table_id}"
            self.chains[chain_id] = {
                'id': chain_id,
                'tables': [table_id],
                'years': [metadata['year']],
                'headers': [metadata['header']],
                'mask_references': [metadata.get('mask_reference', '')],  # Track mask references
                'status': 'active',
                'gaps': [],
                'similarities': [],  # Store similarity scores
                'api_validated': []  # Track API validation usage
            }
        return len(self.chains)

    def update_chains(self, matches, year, table_metadata, api_validations=None):
        matched_chains = set()
        for match_info in matches:
            # Handle both tuple and dict formats
            if isinstance(match_info, tuple):
                chain_id, table_id, similarity = match_info
                api_used = False
            else:
                chain_id = match_info['chain_id']
                table_id = match_info['table_id']
                similarity = match_info['similarity']
                api_used = match_info.get('api_validated', False)

            if chain_id in self.chains:
                self.chains[chain_id]['tables'].append(table_id)
                self.chains[chain_id]['years'].append(year)
                self.chains[chain_id]['similarities'].append(similarity)
                self.chains[chain_id]['api_validated'].append(api_used)

                if table_id in table_metadata:
                    self.chains[chain_id]['headers'].append(table_metadata[table_id]['header'])
                    # Add mask reference
                    self.chains[chain_id]['mask_references'].append(table_metadata[table_id].get('mask_reference', ''))

                # Store match details for visualization
                edge_key = f"{self.chains[chain_id]['tables'][-2]}_{table_id}"
                self.match_details[edge_key] = {
                    'similarity': similarity,
                    'api_validated': api_used
                }

                matched_chains.add(chain_id)

        # Mark unmatched as dormant
        for chain_id, chain in self.chains.items():
            if chain['status'] == 'active' and chain_id not in matched_chains:
                chain['status'] = 'dormant'
                chain['gaps'].append(year)

    def get_chain_embeddings(self, embeddings_dict):
        chain_embeddings = {}
        for chain_id, chain in self.chains.items():
            if chain['status'] == 'active' and chain['tables']:
                last_table = chain['tables'][-1]
                if last_table in embeddings_dict:
                    chain_embeddings[chain_id] = embeddings_dict[last_table]
        return chain_embeddings

    def get_mask_references_for_chain(self, chain_id):
        """Get all mask references in a chain"""
        if chain_id in self.chains:
            return self.chains[chain_id].get('mask_references', [])
        return []