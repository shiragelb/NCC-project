import json
import os
from datetime import datetime
import numpy as np
try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    PLOTLY_AVAILABLE = True
except:
    PLOTLY_AVAILABLE = False

class VisualizationGenerator:
    def __init__(self):
        self.colors = ['#4CAF50', '#FF9800', '#9C27B0', '#F44336', '#2196F3']

    def create_sankey(self, chains, sim_matrix_data=None):
        """Create enhanced Sankey diagram with similarity scores"""
        if not PLOTLY_AVAILABLE:
            print("Install plotly: !pip install plotly")
            return None

        # Create subplots - Sankey on top, heatmap below
        fig = make_subplots(
            rows=2, cols=1,
            row_heights=[0.7, 0.3],
            specs=[[{"type": "sankey"}],
                   [{"type": "heatmap"}]],
            subplot_titles=("Table Chain Evolution", "Similarity Matrix")
        )

        # Build Sankey data
        nodes = []
        node_labels = []
        sources = []
        targets = []
        values = []
        link_labels = []
        link_colors = []

        node_map = {}
        header_map = {}
        node_idx = 0

        for chain_id, chain in chains.items():
            for i, table in enumerate(chain['tables']):
                if table not in node_map:
                    node_map[table] = node_idx
                    header = chain['headers'][i] if i < len(chain['headers']) else 'No header'
                    header_map[table] = header

                    clean_header = header.replace('\n', ' ')[:50] + '...' if len(header) > 50 else header.replace('\n', ' ')

                    node_labels.append(f"{table}<br>Year: {chain['years'][i]}<br>{clean_header}")
                    node_idx += 1

                if i > 0:
                    prev_table = chain['tables'][i-1]
                    sources.append(node_map[prev_table])
                    targets.append(node_map[table])
                    values.append(1)

                    # Get similarity and API info if available
                    similarity = chain.get('similarities', [])[i-1] if i-1 < len(chain.get('similarities', [])) else 0.95
                    api_used = chain.get('api_validated', [])[i-1] if i-1 < len(chain.get('api_validated', [])) else False

                    # Create detailed hover text
                    source_header = header_map.get(prev_table, 'No header')
                    target_header = header_map.get(table, 'No header')
                    api_text = "✓ API Validated" if api_used else "Auto-matched"

                    hover_text = (f"<b>Similarity: {similarity:.3f}</b><br>"
                                f"{api_text}<br><br>"
                                f"<b>Source:</b> {prev_table}<br>{source_header}<br><br>"
                                f"<b>Target:</b> {table}<br>{target_header}")
                    link_labels.append(hover_text)

                    # Color based on similarity
                    if similarity >= 0.97:
                        color = 'rgba(76, 175, 80, 0.5)'  # Green
                    elif similarity >= 0.90:
                        color = 'rgba(255, 193, 7, 0.5)'  # Amber
                    elif similarity >= 0.85:
                        color = 'rgba(255, 152, 0, 0.5)'  # Orange
                    else:
                        color = 'rgba(244, 67, 54, 0.5)'  # Red

                    if api_used:
                        color = color.replace('0.5', '0.8')  # Darker if API validated

                    link_colors.append(color)

        # Add Sankey to subplot
        sankey = go.Sankey(
            node=dict(
                pad=15,
                thickness=20,
                line=dict(color="black", width=0.5),
                label=node_labels,
                hovertemplate='%{label}<extra></extra>'
            ),
            link=dict(
                source=sources,
                target=targets,
                value=values,
                label=link_labels,
                color=link_colors,
                hovertemplate='%{label}<extra></extra>'
            )
        )

        fig.add_trace(sankey, row=1, col=1)

        # Add similarity matrix heatmap if available
        if sim_matrix_data and 'matrix' in sim_matrix_data:
            matrix = sim_matrix_data['matrix']
            chain_ids = [c.split('_')[-1] if c.startswith('chain_') else c
                        for c in sim_matrix_data.get('chain_ids', [])]
            table_ids = sim_matrix_data.get('table_ids', [])

            heatmap = go.Heatmap(
                z=matrix,
                x=table_ids,
                y=chain_ids,
                colorscale='RdYlGn',
                zmin=0,
                zmax=1,
                text=np.round(matrix, 3),
                texttemplate='%{text}',
                textfont={"size": 8},
                hovertemplate='Chain: %{y}<br>Table: %{x}<br>Similarity: %{z:.3f}<extra></extra>',
                colorbar=dict(title="Similarity", len=0.3, y=0.15)
            )

            fig.add_trace(heatmap, row=2, col=1)

        # Update layout
        fig.update_layout(
            title_text="Table Chain Analysis with Similarity Metrics",
            height=1000,
            width=1400,
            showlegend=False,
            font_size=10
        )

        return fig

    def save_graph_json(self, chains, filepath="graph.json"):
        """Save graph structure as JSON with mask references"""
        graph = {
            'nodes': [],
            'edges': [],
            'metadata': {
                'created': str(datetime.now()),
                'total_chains': len(chains),
                'active_chains': sum(1 for c in chains.values() if c['status'] == 'active')
            }
        }

        for chain_id, chain in chains.items():
            for i, table in enumerate(chain['tables']):
                # Include mask reference in node data
                mask_ref = chain.get('mask_references', [])[i] if i < len(chain.get('mask_references', [])) else ''

                graph['nodes'].append({
                    'id': table,
                    'chain': chain_id,
                    'year': chain['years'][i] if i < len(chain['years']) else 0,
                    'header': chain['headers'][i] if i < len(chain['headers']) else '',
                    'mask_reference': mask_ref  # Include mask reference
                })

                if i > 0:
                    similarity = chain.get('similarities', [])[i-1] if i-1 < len(chain.get('similarities', [])) else None
                    api_used = chain.get('api_validated', [])[i-1] if i-1 < len(chain.get('api_validated', [])) else False

                    graph['edges'].append({
                        'source': chain['tables'][i-1],
                        'target': table,
                        'type': 'continuation',
                        'similarity': similarity,
                        'api_validated': api_used
                    })

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(graph, f, indent=2, ensure_ascii=False)

        return filepath