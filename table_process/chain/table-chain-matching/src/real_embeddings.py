import numpy as np
import pickle
import os
import hashlib

try:
    from sentence_transformers import SentenceTransformer
    TRANSFORMER_AVAILABLE = True
except:
    TRANSFORMER_AVAILABLE = False
    print("Install with: !pip install sentence-transformers")

class RealEmbeddingGenerator:
    def __init__(self, model_name="imvladikon/sentence-transformers-alephbert", cache_dir="cache"):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        self.embedding_cache = {}

        if TRANSFORMER_AVAILABLE:
            self.model = SentenceTransformer(model_name)
            self.dimension = self.model.get_sentence_embedding_dimension()
        else:
            self.model = None
            self.dimension = 768

    def get_text_hash(self, text):
        return hashlib.md5(text.encode('utf-8')).hexdigest()

    def generate_embedding(self, text, use_cache=True):
        text_hash = self.get_text_hash(text)

        if use_cache and text_hash in self.embedding_cache:
            return self.embedding_cache[text_hash]

        if self.model:
            embedding = self.model.encode(text, convert_to_numpy=True)
        else:
            # Fallback to deterministic random
            np.random.seed(int(text_hash[:8], 16) % 10000)
            embedding = np.random.randn(self.dimension)

        if use_cache:
            self.embedding_cache[text_hash] = embedding

        return embedding

    def generate_batch(self, texts, show_progress=True):
        if self.model:
            return self.model.encode(texts,
                                    batch_size=32,
                                    show_progress_bar=show_progress,
                                    convert_to_numpy=True)
        else:
            return np.array([self.generate_embedding(t) for t in texts])

    def save_cache(self):
        cache_file = os.path.join(self.cache_dir, "embedding_cache.pkl")
        with open(cache_file, 'wb') as f:
            pickle.dump(self.embedding_cache, f)