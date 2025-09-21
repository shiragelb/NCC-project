"""
Embeddings Handler for semantic similarity
Uses AlephBERT for Hebrew text embeddings
"""

# not relevant for column matching
import torch
from transformers import AutoTokenizer, AutoModel
import numpy as np
from typing import List, Union
from sklearn.metrics.pairwise import cosine_similarity
import logging

logger = logging.getLogger(__name__)

class EmbeddingsHandler:
    def __init__(self, model_name: str = "onlplab/alephbert-base"):
        """Initialize AlephBERT model and tokenizer"""
        try:
            self.tokenizer = AutoTokenizer.from_pretrained(model_name)
            self.model = AutoModel.from_pretrained(model_name)
            self.model.eval()
            logger.info(f"Loaded embedding model: {model_name}")
        except Exception as e:
            logger.error(f"Failed to load embedding model: {e}")
            raise

    def get_embedding(self, text: str) -> np.ndarray:
        """Get embedding for a single text"""
        with torch.no_grad():
            inputs = self.tokenizer(text, return_tensors="pt",
                                  padding=True, truncation=True, max_length=512)
            outputs = self.model(**inputs)
            embedding = outputs.last_hidden_state[:, 0, :].numpy()
        return embedding.squeeze()



    def get_batch_embedding(self, feature_batch: List) -> np.ndarray:
        """Get embedding for a feature batch (list of DataFrame rows)"""
        # Concatenate all text from the feature batch
        batch_text = []
        for row in feature_batch:
            row_text = " | ".join([str(val).strip() for val in row.values if pd.notna(val)])
            batch_text.append(row_text)

        combined_text = " ".join(batch_text)

        # Get embedding
        with torch.no_grad():
            inputs = self.tokenizer(combined_text,
                                   return_tensors="pt",
                                   padding=True,
                                   truncation=True,
                                   max_length=512)
            outputs = self.model(**inputs)
            # Use CLS token embedding
            embedding = outputs.last_hidden_state[:, 0, :].numpy()

        return embedding.squeeze()

    def calculate_similarity(self, batch1: List, batch2: List) -> float:
        """Calculate cosine similarity between two feature batches"""
        try:
            emb1 = self.get_batch_embedding(batch1)
            emb2 = self.get_batch_embedding(batch2)

            # Reshape for sklearn
            emb1 = emb1.reshape(1, -1)
            emb2 = emb2.reshape(1, -1)

            similarity = cosine_similarity(emb1, emb2)[0, 0]
            return float(similarity)
        except Exception as e:
            logger.error(f"Error calculating similarity: {e}")
            return 0.0