import json
import numpy as np

class ParameterTuner:
    def __init__(self):
        self.param_history = []
        self.optimal_params = None

    def grid_search(self, param_ranges, validation_data):
        """Grid search for optimal parameters"""
        best_score = 0
        best_params = {}

        # Example grid search
        for sim_thresh in param_ranges.get('similarity_threshold', [0.85]):
            for split_thresh in param_ranges.get('split_threshold', [0.80]):
                score = self._evaluate_params({
                    'similarity_threshold': sim_thresh,
                    'split_threshold': split_thresh
                }, validation_data)

                if score > best_score:
                    best_score = score
                    best_params = {
                        'similarity_threshold': sim_thresh,
                        'split_threshold': split_thresh
                    }

        self.optimal_params = best_params
        return best_params

    def _evaluate_params(self, params, validation_data):
        """Evaluate parameter set"""
        # Mock evaluation - in reality would run matching and compare
        return np.random.random()

    def suggest_adjustments(self, current_stats):
        """Suggest parameter adjustments based on statistics"""
        suggestions = []

        if current_stats.get('match_rate', 0) < 0.7:
            suggestions.append("Consider lowering similarity_threshold")

        if current_stats.get('false_positives', 0) > 0.1:
            suggestions.append("Consider raising similarity_threshold")

        return suggestions