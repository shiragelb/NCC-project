import unittest
import numpy as np

class TestCompleteSystem(unittest.TestCase):
    def test_similarity_computation(self):
        """Test similarity computation"""
        emb1 = np.array([1, 0, 0])
        emb2 = np.array([1, 0, 0])
        emb3 = np.array([0, 1, 0])

        from scipy.spatial.distance import cosine
        sim12 = 1 - cosine(emb1, emb2)
        sim13 = 1 - cosine(emb1, emb3)

        self.assertAlmostEqual(sim12, 1.0)
        self.assertAlmostEqual(sim13, 0.0)

    def test_hungarian_matching(self):
        """Test Hungarian algorithm"""
        from scipy.optimize import linear_sum_assignment

        cost = np.array([[1, 2], [3, 4]])
        row_ind, col_ind = linear_sum_assignment(cost)

        self.assertEqual(len(row_ind), 2)
        self.assertEqual(len(col_ind), 2)

    def test_conflict_detection(self):
        """Test conflict detection"""
        matrix = np.array([[0.9, 0.3], [0.88, 0.4]])

        conflicts = []
        for j in range(matrix.shape[1]):
            high_sim = []
            for i in range(matrix.shape[0]):
                if matrix[i, j] >= 0.85:
                    high_sim.append(i)
            if len(high_sim) > 1:
                conflicts.append(j)

        self.assertEqual(len(conflicts), 0)  # No conflicts in this example

def run_all_tests():
    """Run complete test suite"""
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestCompleteSystem)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result.wasSuccessful()