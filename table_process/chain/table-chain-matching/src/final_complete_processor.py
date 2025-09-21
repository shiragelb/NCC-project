import time
import os
from datetime import datetime
from scipy.spatial.distance import cosine

# Import ALL components
from config import MatchingConfig
from hebrew_processor import HebrewProcessor
from table_loader import TableLoader
from real_embeddings import RealEmbeddingGenerator
from similarity import SimilarityBuilder
from hungarian import HungarianMatcher
from split_merge import SplitMergeDetector
from complex_relationships import ComplexRelationshipDetector
from chains import ChainManager
from api_validator import ClaudeAPIValidator
from gap_handler import GapHandler
from storage_manager import StorageManager
from statistics_tracker import StatisticsTracker
from visualization import VisualizationGenerator
from report_gen import ReportGenerator
from networkx_builder import NetworkXGraphBuilder
from conflict_resolver import ConflictResolver
from response_handler import APIResponseHandler
from parameter_tuner import ParameterTuner
from test_suite import run_all_tests

def process_table_chains_final_complete():
    """Complete processing with chapter-by-chapter matching and proper validation"""
    print("="*60)
    print("CHAPTER-BY-CHAPTER TABLE CHAIN MATCHING SYSTEM")
    print("="*60)

    # Run tests first
    print("\nRunning system tests...")
    tests_passed = run_all_tests()
    print(f"Tests: {'PASSED' if tests_passed else 'FAILED'}")

    start_time = time.time()

    # Initialize components
    config = MatchingConfig()
    config.use_api_validation = True  # Enable API validation
    hebrew_proc = HebrewProcessor()

    # Initialize loader
    loader = TableLoader(
        tables_dir=config.tables_dir,
        reference_json=config.reference_json,
        mask_dir=config.mask_dir
    )

    embedder = RealEmbeddingGenerator()
    sim_builder = SimilarityBuilder()
    matcher = HungarianMatcher(config.similarity_threshold)
    split_detector = SplitMergeDetector()
    complex_detector = ComplexRelationshipDetector()
    api_validator = ClaudeAPIValidator(config.api_key)
    gap_handler = GapHandler(config.max_gap_years)
    storage_mgr = StorageManager()
    stats_tracker = StatisticsTracker()
    visualizer = VisualizationGenerator()
    reporter = ReportGenerator()
    nx_builder = NetworkXGraphBuilder()
    conflict_resolver = ConflictResolver()
    response_handler = APIResponseHandler()
    param_tuner = ParameterTuner()

    # Load tables
    print("\n1. Loading tables...")
    n_tables = loader.load_metadata()
    print(f"   Loaded {n_tables} tables")

    # Check if reference files exist
    if not os.path.exists(config.reference_json):
        print(f"   Warning: {config.reference_json} not found!")
        return None, None



    # Reorganize tables by chapter and year
    tables_by_chapter_year = {}
    for tid, metadata in loader.tables_metadata.items():
        chapter = metadata['chapter']
        year = metadata['year']

        if chapter not in tables_by_chapter_year:
            tables_by_chapter_year[chapter] = {}
        if year not in tables_by_chapter_year[chapter]:
            tables_by_chapter_year[chapter][year] = []
        tables_by_chapter_year[chapter][year].append(tid)

    # Filter years to 2001-2018
    year_range = range(2001, 2025)
    for chapter in tables_by_chapter_year:
        tables_by_chapter_year[chapter] = {
            year: tables
            for year, tables in tables_by_chapter_year[chapter].items()
            if year in year_range
        }

    # TEST MODE: Only process Chapter 1
    # Change to list(sorted(tables_by_chapter_year.keys())) for all chapters
    # chapters_to_process = [1] # when wanting only chapter 1
    chapters_to_process = list(sorted(tables_by_chapter_year.keys())) # when wanting all chapters

    print(f"\n2. Chapter Organization:")
    print(f"   Found {len(tables_by_chapter_year)} chapters total")
    print(f"   Processing chapters: {chapters_to_process}")

    all_chapter_chains = {}
    all_chapter_stats = {}

    # Process each chapter independently
    for chapter in chapters_to_process:
        print(f"\n{'='*60}")
        print(f"PROCESSING CHAPTER {chapter}")
        print(f"{'='*60}")

        if chapter not in tables_by_chapter_year:
            print(f"   No tables found for chapter {chapter}")
            continue

        chapter_years = sorted(tables_by_chapter_year[chapter].keys())
        print(f"   Years available: {min(chapter_years)} to {max(chapter_years)}")

        # Initialize fresh components for this chapter
        chain_mgr = ChainManager()
        chapter_stats = StatisticsTracker()

        # Initialize chains with first year
        first_year = chapter_years[0]
        first_year_tables = {tid: loader.tables_metadata[tid]
                            for tid in tables_by_chapter_year[chapter][first_year]}

        chain_mgr.initialize_from_first_year(first_year_tables)
        print(f"   Initialized {len(chain_mgr.chains)} chains for Year {first_year}")

        # Generate embeddings for all tables in this chapter
        print(f"\n   Generating embeddings for chapter {chapter}...")
        chapter_embeddings = {}

        for year in chapter_years:
            year_count = 0
            for tid in tables_by_chapter_year[chapter][year]:
                text = hebrew_proc.process_header(loader.tables_metadata[tid]['header'])
                embedding = embedder.generate_embedding(text)
                chapter_embeddings[tid] = embedding
                year_count += 1
            print(f"      Year {year}: {year_count} embeddings")

        last_sim_matrix = None

        # Process each subsequent year for this chapter
        for year in chapter_years[1:]:
            print(f"\n   Processing Chapter {chapter}, Year {year}...")
            year_start = time.time()

            # Get embeddings for matching
            chain_embeddings = chain_mgr.get_chain_embeddings(chapter_embeddings)
            table_embeddings = {tid: chapter_embeddings[tid]
                              for tid in tables_by_chapter_year[chapter][year]
                              if tid in chapter_embeddings}

            print(f"      Active chains: {len(chain_embeddings)}, Tables: {len(table_embeddings)}")

            if not table_embeddings:
                print(f"      No tables to match for year {year}")
                continue

            # Build similarity matrix
            sim_matrix = sim_builder.compute_similarity_matrix(
                chain_embeddings, table_embeddings
            )
            last_sim_matrix = sim_matrix

            # Detect conflicts
            conflicts = conflict_resolver.detect_conflicts(sim_matrix)
            if conflicts:
                print(f"      Conflicts detected: {len(conflicts)}")
                resolutions = conflict_resolver.resolve_conflicts(conflicts, api_validator)

            # Hungarian matching
            matching_result = matcher.find_optimal_matching(sim_matrix)
            print(f"      Initial matches found: {len(matching_result['matches'])}")


            # Detect splits and merges
            splits = split_detector.detect_splits(sim_matrix)
            merges = split_detector.detect_merges(sim_matrix)
            complex_rels = complex_detector.detect_complex(sim_matrix, splits, merges)

            if splits:
                print(f"      Splits detected: {len(splits)}")
            if merges:
                print(f"      Merges detected: {len(merges)}")
            if complex_rels:
                print(f"      Complex N:N relationships: {len(complex_rels)}")

            # FIX 1: Proper API validation with rejection of low-confidence matches
            validated_matches = []
            for chain_id, table_id, similarity in matching_result['matches']:
                # Reject low confidence matches unless API confirms
                if similarity < 0.97:
                    if config.use_api_validation and 0.85 <= similarity:
                        validation = api_validator.validate_edge_case(
                            chain_mgr.chains[chain_id]['headers'],
                            loader.tables_metadata[table_id]['header'],
                            similarity
                        )
                        action = response_handler.process_response(validation, 'edge_case')
                        if action.value == 'confirm':
                            validated_matches.append({
                                'chain_id': chain_id,
                                'table_id': table_id,
                                'similarity': similarity,
                                'api_validated': True
                            })
                            print(f"      API confirmed: {chain_id} -> {table_id} (sim={similarity:.3f})")
                        else:
                            print(f"      API rejected: {chain_id} -> {table_id} (sim={similarity:.3f})")
                            # Mark table as unmatched since it was rejected
                            if table_id not in matching_result['unmatched_tables']:
                                matching_result['unmatched_tables'].append(table_id)
                    else:
                        print(f"      Rejected low confidence: {chain_id} -> {table_id} (sim={similarity:.3f})")
                        # Mark table as unmatched
                        if table_id not in matching_result['unmatched_tables']:
                            matching_result['unmatched_tables'].append(table_id)
                else:
                    # High confidence match, accept
                    validated_matches.append({
                        'chain_id': chain_id,
                        'table_id': table_id,
                        'similarity': similarity,
                        'api_validated': False
                    })

            print(f"      Validated matches: {len(validated_matches)}")

            # Update chains
            chain_mgr.update_chains(validated_matches, year, loader.tables_metadata)

            # Handle gaps
            matched_chains = {m['chain_id'] for m in validated_matches}
            gap_report = gap_handler.check_gaps(chain_mgr.chains, year, matched_chains)

            if gap_report['new_dormant']:
                print(f"      New dormant chains: {len(gap_report['new_dormant'])}")
            if gap_report['ended']:
                print(f"      Ended chains: {len(gap_report['ended'])}")

            # FIX 2: Try to reactivate dormant chains
            reactivated_count = 0
            for chain_id, chain in chain_mgr.chains.items():
                if chain['status'] == 'dormant' and chain_id not in matched_chains:
                    # Try matching this dormant chain to unmatched tables
                    if chain['tables']:
                        last_table = chain['tables'][-1]
                        if last_table in chapter_embeddings:
                            chain_emb = chapter_embeddings[last_table]
                            for table_id in list(matching_result['unmatched_tables']):  # Use list() to avoid modification during iteration
                                if table_id in chapter_embeddings:
                                    table_emb = chapter_embeddings[table_id]
                                    similarity = (1 - cosine(chain_emb, table_emb) + 1) / 2

                                    # Check if high confidence or API validates
                                    should_reactivate = False
                                    if similarity >= 0.97:
                                        should_reactivate = True
                                    elif config.use_api_validation and 0.85 <= similarity:
                                        validation = api_validator.validate_edge_case(
                                            chain['headers'],
                                            loader.tables_metadata[table_id]['header'],
                                            similarity
                                        )
                                        action = response_handler.process_response(validation, 'edge_case')
                                        if action.value == 'confirm':
                                            should_reactivate = True

                                    if should_reactivate:
                                        chain['status'] = 'active'
                                        chain['tables'].append(table_id)
                                        chain['years'].append(year)
                                        chain['headers'].append(loader.tables_metadata[table_id]['header'])
                                        chain['mask_references'].append(loader.tables_metadata[table_id].get('mask_reference', ''))
                                        chain['similarities'].append(similarity)
                                        chain['api_validated'].append(similarity < 0.97)
                                        matching_result['unmatched_tables'].remove(table_id)
                                        reactivated_count += 1
                                        print(f"      Reactivated chain {chain_id} with {table_id} (sim={similarity:.3f})")
                                        break

            if reactivated_count > 0:
                print(f"      Total chains reactivated: {reactivated_count}")

            # FIX 3: Create new chains for unmatched tables
            new_chains_count = 0
            for table_id in matching_result['unmatched_tables']:
                if table_id in loader.tables_metadata:
                    new_chain_id = f"chain_{table_id}"
                    chain_mgr.chains[new_chain_id] = {
                        'id': new_chain_id,
                        'tables': [table_id],
                        'years': [year],
                        'headers': [loader.tables_metadata[table_id]['header']],
                        'mask_references': [loader.tables_metadata[table_id].get('mask_reference', '')],
                        'status': 'active',
                        'gaps': [],
                        'similarities': [],
                        'api_validated': []
                    }
                    new_chains_count += 1

            if new_chains_count > 0:
                print(f"      Created {new_chains_count} new chains for unmatched tables")

            # Record statistics for this chapter
            for match in validated_matches:
                chapter_stats.record_match(
                    match['chain_id'],
                    match['table_id'],
                    year,
                    match['similarity'],
                    'confident' if match['similarity'] >= 0.97 else 'uncertain'
                )

            year_time = time.time() - year_start
            chapter_stats.record_year(
                year, len(tables_by_chapter_year[chapter][year]),
                len(validated_matches),
                matching_result['unmatched_tables'],
                matching_result['unmatched_chains'],
                year_time
            )

        # Store results for this chapter
        all_chapter_chains[chapter] = chain_mgr.chains
        all_chapter_stats[chapter] = chapter_stats.get_summary()

        # Generate outputs for this chapter
        print(f"\n   Generating outputs for Chapter {chapter}...")

        # Create chapter-specific output files
        chapter_dir = "../chain-api-expantion"
        os.makedirs(chapter_dir, exist_ok=True)

        # Visualizations
        sankey = visualizer.create_sankey(chain_mgr.chains, last_sim_matrix)
        if sankey:
            sankey_file = f"{chapter_dir}/sankey_chapter_{chapter}.html"
            sankey.write_html(sankey_file)
            print(f"      Created {sankey_file}")

        # Reports
        chains_file = f"{chapter_dir}/chains_chapter_{chapter}.json"
        reporter.save_chains_json(chain_mgr.chains, chains_file)
        print(f"      Created {chains_file}")

        html_file = f"{chapter_dir}/report_chapter_{chapter}.html"
        with open(html_file, 'w', encoding='utf-8') as f:
            html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Chapter {chapter} Chain Report</title>
    <meta charset="utf-8">
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .summary {{ background: #f0f0f0; padding: 15px; margin: 20px 0; }}
    </style>
</head>
<body>
    <h1>Chapter {chapter} Chain Matching Report</h1>
    <div class="summary">
        <h2>Summary</h2>
        <p>Total chains: {len(chain_mgr.chains)}</p>
        <p>Active chains: {sum(1 for c in chain_mgr.chains.values() if c['status'] == 'active')}</p>
        <p>Dormant chains: {sum(1 for c in chain_mgr.chains.values() if c['status'] == 'dormant')}</p>
        <p>Year range: {min(chapter_years)} - {max(chapter_years)}</p>
    </div>
</body>
</html>"""
            f.write(html)
        print(f"      Created {html_file}")

        # Chapter summary
        print(f"\n   Chapter {chapter} Summary:")
        print(f"      Total chains: {len(chain_mgr.chains)}")
        print(f"      Active chains: {sum(1 for c in chain_mgr.chains.values() if c['status'] == 'active')}")
        print(f"      Dormant chains: {sum(1 for c in chain_mgr.chains.values() if c['status'] == 'dormant')}")
        print(f"      Total matches: {chapter_stats.global_stats['total_matches']}")

    # Final summary
    total_time = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"✅ COMPLETE Processing finished in {total_time:.2f} seconds")
    print(f"   Chapters processed: {len(all_chapter_chains)}")

    for chapter in chapters_to_process:
        if chapter in all_chapter_chains:
            print(f"\n   Chapter {chapter}:")
            print(f"      Chains: {len(all_chapter_chains[chapter])}")
            print(f"      Active: {sum(1 for c in all_chapter_chains[chapter].values() if c['status'] == 'active')}")
            print(f"      Dormant: {sum(1 for c in all_chapter_chains[chapter].values() if c['status'] == 'dormant')}")

    if config.use_api_validation:
        print(f"\n   Total API validations: {api_validator.validation_count}")

    return all_chapter_chains, all_chapter_stats

if __name__ == "__main__":
    chains, statistics = process_table_chains_final_complete()