import json
import pickle
import gzip
from datetime import datetime
from pathlib import Path

class StorageManager:
    def __init__(self, storage_dir="chain_storage"):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)

        # Create subdirectories
        (self.storage_dir / "checkpoints").mkdir(exist_ok=True)
        (self.storage_dir / "backups").mkdir(exist_ok=True)
        (self.storage_dir / "embeddings").mkdir(exist_ok=True)

    def save_checkpoint(self, year, chains, statistics):
        """Save processing checkpoint"""
        checkpoint = {
            'year': year,
            'timestamp': datetime.now().isoformat(),
            'chains': chains,
            'statistics': statistics
        }

        filename = f"checkpoint_{year}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        filepath = self.storage_dir / "checkpoints" / filename

        with gzip.open(filepath.with_suffix('.json.gz'), 'wt', encoding='utf-8') as f:
            json.dump(checkpoint, f, indent=2)

        return str(filepath)

    def load_checkpoint(self, year):
        """Load latest checkpoint for a year"""
        checkpoint_dir = self.storage_dir / "checkpoints"
        pattern = f"checkpoint_{year}_*.json.gz"

        files = list(checkpoint_dir.glob(pattern))
        if files:
            latest = max(files, key=lambda f: f.stat().st_mtime)
            with gzip.open(latest, 'rt', encoding='utf-8') as f:
                return json.load(f)
        return None

    def save_embeddings(self, embeddings, year):
        """Save embeddings for a year"""
        filepath = self.storage_dir / "embeddings" / f"embeddings_{year}.pkl"
        with open(filepath, 'wb') as f:
            pickle.dump(embeddings, f)

    def load_embeddings(self, year):
        """Load embeddings for a year"""
        filepath = self.storage_dir / "embeddings" / f"embeddings_{year}.pkl"
        if filepath.exists():
            with open(filepath, 'rb') as f:
                return pickle.load(f)
        return None

    def backup_chains(self, chains):
        """Create backup of chains"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = self.storage_dir / "backups" / f"chains_backup_{timestamp}.json.gz"

        with gzip.open(backup_file, 'wt', encoding='utf-8') as f:
            json.dump(chains, f, indent=2)