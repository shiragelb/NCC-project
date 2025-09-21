"""
Continuation table merger for combining split tables.
"""

# ========================================================================
# INSTRUCTIONS: PASTE YOUR CODE FROM NOTEBOOK
# ========================================================================
# FROM LINES: ~1150-1250
# CONTENT: GlobalContinuationMerger class (and ContinuationMerger if exists)
# 
# MODIFICATIONS NEEDED: None, paste as-is
# ========================================================================

import os
import json
import pandas as pd
from collections import defaultdict
import logging


logger = logging.getLogger(__name__)

# GlobalContinuationMerger class 

class GlobalContinuationMerger:
    def __init__(self, base_dir):
        self.base_dir = base_dir          # e.g., "/content/Tables"
        self.summary_path = os.path.join(base_dir, "summaries.json")
        self.ENCODING = "utf-8"

    def _identify_continuation_groups(self, summaries):
        """Identify continuation chains and duplicate-name groups across all chapters/years."""
        groups = {}
        continuation_marker = "(המשך)"

        # Sort identifiers
        sorted_ids = sorted(summaries.keys(), key=lambda x: (
            int(x.split('_')[2]),   # year
            x.split('_')[1],        # chapter
            int(x.split('_')[0])    # serial
        ))

        # --- Case 1: (המשך) chains ---
        current_group_original = None
        for identifier in sorted_ids:
            header = summaries[identifier]
            if continuation_marker in header:
                if current_group_original:
                    groups[current_group_original].append(identifier)
                else:
                    print(f"⚠️ Continuation without an original: {identifier}")
            else:
                current_group_original = identifier
                groups[identifier] = [identifier]

        # --- Case 2: exact same names ---
        name_groups = defaultdict(list)
        for identifier, name in summaries.items():
            clean_name = name.replace(continuation_marker, "").strip()
            if clean_name and "unnamed" not in clean_name.lower():
                name_groups[clean_name].append(identifier)

        for name, ids in name_groups.items():
            if len(ids) > 1:
                ids_sorted = sorted(ids, key=lambda x: (
                    int(x.split('_')[2]),
                    x.split('_')[1],
                    int(x.split('_')[0])
                ))
                original = ids_sorted[0]
                if original not in groups:
                    groups[original] = []
                for ident in ids_sorted:
                    if ident not in groups[original]:
                        groups[original].append(ident)

        return {k: v for k, v in groups.items() if len(v) > 1}

    def _find_csv_path(self, identifier):
        """Search recursively for the CSV file belonging to a given identifier."""
        for root, _, files in os.walk(self.base_dir):
            if f"{identifier}.csv" in files:
                return os.path.join(root, f"{identifier}.csv")
        return None

    def _combine_csv_files(self, identifiers):
        """Load and combine multiple CSVs into one."""
        dfs = []
        for identifier in identifiers:
            csv_path = self._find_csv_path(identifier)
            if not csv_path:
                print(f"⚠️ CSV not found for {identifier}")
                continue
            df = pd.read_csv(csv_path, encoding=self.ENCODING)
            dfs.append((identifier, csv_path, df))
        if not dfs:
            return None, {}
        combined_df = pd.concat([x[2] for x in dfs], ignore_index=True)
        return combined_df, {ident: path for ident, path, _ in dfs}

    def combine_continuation_tables(self):
        """Merge continuation/duplicate tables across ALL chapters and update global summaries.json."""
        if not os.path.exists(self.summary_path):
            print("⚠️ summaries.json not found")
            return {}

        with open(self.summary_path, "r", encoding="utf-8") as f:
            summaries = json.load(f)

        groups = self._identify_continuation_groups(summaries)
        if not groups:
            print("No continuation or duplicate-name tables found.")
            return {}

        print(f"Found {len(groups)} table group(s) to combine...")

        combined_info = {}
        for original_id, identifiers in groups.items():
            combined_df, paths_map = self._combine_csv_files(identifiers)
            if combined_df is not None:
                # Save combined CSV over the original
                orig_path = paths_map[original_id]
                combined_df.to_csv(orig_path, index=False, encoding=self.ENCODING)

                # Remove continuation CSVs
                for cont_id in identifiers[1:]:
                    cont_path = paths_map.get(cont_id)
                    if cont_path and os.path.exists(cont_path):
                        os.remove(cont_path)
                        print(f"Removed: {cont_id}.csv")

                combined_info[original_id] = {
                    "parts_combined": len(identifiers),
                    "continuation_ids": identifiers[1:],
                    "rows_in_combined": len(combined_df)
                }

        # Clean up summaries.json (drop merged continuation entries)
        keep_ids = set(combined_info.keys())
        summaries_clean = {
            k: v for k, v in summaries.items()
            if k in keep_ids or all(k not in group for group in groups.values())
        }

        # ✅ Save updated global summaries.json
        with open(self.summary_path, "w", encoding="utf-8") as f:
            json.dump(summaries_clean, f, ensure_ascii=False, indent=2)

        print("✓ Global continuation/duplicate tables merged, summaries.json updated.")
        return combined_info

