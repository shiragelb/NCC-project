import json
import numpy as np
from datetime import datetime

class ReportGenerator:
    def __init__(self):
        self.timestamp = datetime.now()

    def generate_summary(self, chains, statistics):
        """Generate summary without column statistics"""
        summary = {
            'timestamp': self.timestamp.isoformat(),
            'total_chains': len(chains),
            'active_chains': sum(1 for c in chains.values()
                               if c['status'] == 'active'),
            'statistics': statistics
        }
        return summary

    def save_chains_json(self, chains, filepath="chains.json"):
        """Save chains to JSON with proper type conversion"""

        def convert_to_native(obj):
            """Convert numpy types to native Python types"""
            if isinstance(obj, np.bool_):
                return bool(obj)
            elif isinstance(obj, (np.integer, np.int32, np.int64)):
                return int(obj)
            elif isinstance(obj, (np.floating, np.float32, np.float64)):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            elif isinstance(obj, list):
                return [convert_to_native(item) for item in obj]
            elif isinstance(obj, dict):
                return {key: convert_to_native(value) for key, value in obj.items()}
            else:
                return obj

        # Convert all chains
        chains_export = convert_to_native(chains)

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(chains_export, f, indent=2, ensure_ascii=False)
        return filepath

    def generate_html_report(self, chains, statistics):
        """Generate HTML report without column information"""
        html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Chain Matching Report</title>
    <meta charset="utf-8">
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        h1, h2, h3 {{ color: #333; }}
        .summary {{ background: #f0f0f0; padding: 15px; border-radius: 5px; margin: 20px 0; }}
        .chain {{ margin: 15px 0; padding: 10px; border-left: 3px solid #4CAF50; }}
        .active {{ background: #e8f5e9; }}
        .dormant {{ background: #fff3e0; border-color: #FF9800; }}
        .ended {{ background: #ffebee; border-color: #f44336; }}
        .table-info {{ margin-left: 20px; font-size: 0.9em; color: #666; }}
        .statistics {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; }}
        .stat-card {{ background: white; padding: 10px; border: 1px solid #ddd; border-radius: 5px; }}
    </style>
</head>
<body>
    <h1>Table Chain Matching Report</h1>
    <p><strong>Generated:</strong> {self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}</p>

    <div class="summary">
        <h2>Summary</h2>
        <div class="statistics">
            <div class="stat-card">
                <strong>Total Chains:</strong> {len(chains)}
            </div>
            <div class="stat-card">
                <strong>Active Chains:</strong> {sum(1 for c in chains.values() if c['status'] == 'active')}
            </div>
            <div class="stat-card">
                <strong>Dormant Chains:</strong> {sum(1 for c in chains.values() if c['status'] == 'dormant')}
            </div>
            <div class="stat-card">
                <strong>Ended Chains:</strong> {sum(1 for c in chains.values() if c['status'] == 'ended')}
            </div>
        </div>
    </div>

    <h2>Chain Details</h2>"""

        # Group chains by status
        active_chains = {k: v for k, v in chains.items() if v['status'] == 'active'}
        dormant_chains = {k: v for k, v in chains.items() if v['status'] == 'dormant'}
        ended_chains = {k: v for k, v in chains.items() if v['status'] == 'ended'}

        # Active Chains
        if active_chains:
            html += "<h3>Active Chains</h3>"
            for chain_id, chain in sorted(active_chains.items()):
                html += self._format_chain_html(chain_id, chain)

        # Dormant Chains
        if dormant_chains:
            html += "<h3>Dormant Chains</h3>"
            for chain_id, chain in sorted(dormant_chains.items()):
                html += self._format_chain_html(chain_id, chain)

        # Ended Chains (show only summary)
        if ended_chains:
            html += f"<h3>Ended Chains ({len(ended_chains)})</h3>"
            html += "<p>Chains that have not matched for multiple years and are considered ended.</p>"

        # Add statistics if available
        if statistics and 'year_by_year' in statistics:
            html += "<h2>Year-by-Year Statistics</h2>"
            html += "<table border='1' style='border-collapse: collapse; width: 100%;'>"
            html += "<tr><th>Year</th><th>Tables</th><th>Matches</th><th>Match Rate</th><th>Processing Time</th></tr>"

            for year, stats in sorted(statistics['year_by_year'].items()):
                html += f"""<tr>
                    <td>{year}</td>
                    <td>{stats.get('tables', 'N/A')}</td>
                    <td>{stats.get('matches', 'N/A')}</td>
                    <td>{stats.get('match_rate', 'N/A')}</td>
                    <td>{stats.get('processing_time', 'N/A')}</td>
                </tr>"""
            html += "</table>"

        html += """
</body>
</html>"""

        with open("report.html", "w", encoding='utf-8') as f:
            f.write(html)

        return "report.html"

    def _format_chain_html(self, chain_id, chain):
        """Format a single chain for HTML display"""
        status_class = chain['status']
        years_range = f"{min(chain['years'])}-{max(chain['years'])}" if chain['years'] else "N/A"

        html = f"""<div class="chain {status_class}">
            <strong>{chain_id}</strong>
            <span style="color: #666;">({len(chain['tables'])} tables, Years: {years_range})</span>
        """

        # Show first few tables
        tables_to_show = min(3, len(chain['tables']))
        for i in range(tables_to_show):
            table = chain['tables'][i]
            year = chain['years'][i] if i < len(chain['years']) else 'N/A'
            header = chain['headers'][i] if i < len(chain['headers']) else 'No header'

            # Clean header for display
            clean_header = header.replace('\n', ' ')[:100]
            if len(header) > 100:
                clean_header += '...'

            html += f"""<div class="table-info">
                <strong>{table}</strong> ({year}): {clean_header}
            </div>"""

        if len(chain['tables']) > tables_to_show:
            html += f"<div class='table-info'>... and {len(chain['tables']) - tables_to_show} more tables</div>"

        # Show gaps if any
        if chain.get('gaps'):
            html += f"<div class='table-info' style='color: #FF9800;'>Gaps in years: {', '.join(map(str, chain['gaps']))}</div>"

        html += "</div>"
        return html