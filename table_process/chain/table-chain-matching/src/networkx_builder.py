try:
    import networkx as nx
    NX_AVAILABLE = True
except:
    NX_AVAILABLE = False

class NetworkXGraphBuilder:
    def __init__(self):
        self.G = None if not NX_AVAILABLE else nx.DiGraph()

    def build_graph(self, chains):
        """Build complete NetworkX graph"""
        if not NX_AVAILABLE:
            return None

        self.G = nx.DiGraph()

        # Add all nodes
        for chain_id, chain in chains.items():
            for i, table in enumerate(chain['tables']):
                self.G.add_node(table,
                              chain=chain_id,
                              year=chain['years'][i] if i < len(chain['years']) else 0,
                              header=chain['headers'][i] if i < len(chain['headers']) else '')

        # Add edges
        for chain_id, chain in chains.items():
            for i in range(1, len(chain['tables'])):
                self.G.add_edge(chain['tables'][i-1],
                              chain['tables'][i],
                              weight=1.0,
                              type='continuation')

        return self.G

    def analyze_graph(self):
        """Analyze graph properties"""
        if not self.G:
            return {}

        return {
            'nodes': self.G.number_of_nodes(),
            'edges': self.G.number_of_edges(),
            'connected_components': nx.number_weakly_connected_components(self.G),
            'average_degree': sum(dict(self.G.degree()).values()) / self.G.number_of_nodes()
        }