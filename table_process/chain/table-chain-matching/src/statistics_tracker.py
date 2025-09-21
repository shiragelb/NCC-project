import numpy as np
from collections import defaultdict
import json
import datetime
from datetime import datetime

class StatisticsTracker:
    def __init__(self):
        self.match_history = []
        self.year_statistics = {}
        self.chain_statistics = defaultdict(lambda: {
            'length': 0,
            'gaps': [],
            'similarity_scores': [],
            'api_validations': 0
        })

        self.global_stats = {
            'total_years_processed': 0,
            'total_matches': 0,
            'total_chains': 0,
            'total_api_calls': 0,
            'total_splits': 0,
            'total_merges': 0
        }

        self.similarity_distributions = defaultdict(list)

    def record_match(self, chain_id, table_id, year, similarity, match_type='confident'):
        """Record a single match"""
        self.match_history.append({
            'chain': chain_id,
            'table': table_id,
            'year': year,
            'similarity': similarity,
            'type': match_type,
            'timestamp': str(datetime.now())
        })

        self.chain_statistics[chain_id]['length'] += 1
        self.chain_statistics[chain_id]['similarity_scores'].append(similarity)
        self.similarity_distributions[year].append(similarity)
        self.global_stats['total_matches'] += 1

    def record_year(self, year, tables_count, matches_count,
                    unmatched_tables, unmatched_chains, processing_time):
        """Record year statistics"""
        self.year_statistics[year] = {
            'tables': tables_count,
            'matches': matches_count,
            'unmatched_tables': len(unmatched_tables),
            'unmatched_chains': len(unmatched_chains),
            'match_rate': matches_count / tables_count if tables_count > 0 else 0,
            'processing_time': processing_time,
            'similarity_distribution': {}
        }

        if year in self.similarity_distributions:
            scores = self.similarity_distributions[year]
            self.year_statistics[year]['similarity_distribution'] = {
                'mean': float(np.mean(scores)),
                'median': float(np.median(scores)),
                'std': float(np.std(scores)),
                'min': float(np.min(scores)),
                'max': float(np.max(scores))
            }

        self.global_stats['total_years_processed'] += 1

    def get_summary(self):
        """Get comprehensive summary"""
        chain_lengths = [s['length'] for s in self.chain_statistics.values()]

        return {
            'overview': {
                'total_years': self.global_stats['total_years_processed'],
                'total_matches': self.global_stats['total_matches'],
                'total_chains': len(self.chain_statistics),
                'match_rate': f"{np.mean([y['match_rate'] for y in self.year_statistics.values()])*100:.1f}%" if self.year_statistics else "0%"
            },
            'chain_statistics': {
                'average_length': np.mean(chain_lengths) if chain_lengths else 0,
                'max_length': max(chain_lengths) if chain_lengths else 0,
                'min_length': min(chain_lengths) if chain_lengths else 0,
                'chains_with_gaps': sum(1 for c in self.chain_statistics.values() if c['gaps'])
            },
            'year_by_year': {
                year: {
                    'tables': stats['tables'],
                    'matches': stats['matches'],
                    'match_rate': f"{stats['match_rate']*100:.1f}%",
                    'processing_time': f"{stats['processing_time']:.2f}s"
                }
                for year, stats in self.year_statistics.items()
            }
        }