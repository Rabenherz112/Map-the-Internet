import networkx as nx
import plotly.graph_objects as go
import mariadb
import os
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# Database connection setup
def establish_db_connection():
    try:
        return mariadb.connect(
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT")),
            database=os.getenv("DB_NAME"),
        )
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB: {e}")
        raise

# Fetch data from MariaDB
def fetch_graph_data(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT parent_id, child_id FROM domain_relationships")
    relationships = cursor.fetchall()

    cursor.execute("SELECT id, domain FROM domains")
    domains = cursor.fetchall()

    return relationships, {id: domain for id, domain in domains}

# Generate graph visualization
def generate_graph(relationships, domain_map):
    G = nx.DiGraph()

    # Add nodes and edges
    for domain_id, domain in domain_map.items():
        G.add_node(domain, size=0)

    for parent_id, child_id in relationships:
        parent_domain = domain_map.get(parent_id)
        child_domain = domain_map.get(child_id)
        if parent_domain and child_domain:
            G.add_edge(parent_domain, child_domain)
            G.nodes[parent_domain]["size"] += 1

    # Remove nodes with fewer than 2 parent connections
    nodes_to_remove = [node for node, data in G.nodes(data=True) if data["size"] < 2]
    G.remove_nodes_from(nodes_to_remove)

    # Calculate rankings based on size
    ranked_nodes = sorted(G.nodes(data=True), key=lambda x: x[1]["size"], reverse=True)
    for rank, (node, data) in enumerate(ranked_nodes, start=1):
        G.nodes[node]["rank"] = rank

    return G

# Create Plotly visualization
def create_plotly_graph(G, output_html="graph.html", output_png="graph.png"):
    pos = nx.spring_layout(G, k=0.1, seed=42)  # Force-directed layout with consistent seed

    # Prepare node trace
    node_x = []
    node_y = []
    node_sizes = []
    node_text = []
    node_customdata = []
    for node, (x, y) in pos.items():
        node_x.append(x)
        node_y.append(y)
        node_size = G.nodes[node]["size"] * 10 + 5
        node_sizes.append(node_size)
        node_text.append(node)
        node_customdata.append(
            f"<b>Domain: {node}</b><br>Ranking: #{G.nodes[node]['rank']}<br>Parent Connections: {G.nodes[node]['size']}<br>Child Connections: {G.out_degree(node)}"
        )

    # Adjust sizes to avoid overlap
    sizeref = 2.0 * max(node_sizes) / (100.0 ** 2)

    node_trace = go.Scatter(
        x=node_x,
        y=node_y,
        mode="markers",
        marker=dict(
            size=node_sizes,
            color=node_sizes,
            colorscale="Viridis",
            showscale=True,
            sizemode="area",
            sizeref=sizeref
        ),
        text=node_customdata,
        hoverinfo="text"
    )

    # Add watermark text
    watermark_text = "Map the Internet Project - Rabenherz112 - Generated at " + datetime.now().strftime("%Y-%m-%d %H:%M")
    annotation_bottom_right = dict(
        xref="paper", yref="paper",
        x=0.99, y=0.01,
        text=watermark_text,
        showarrow=False,
        font=dict(size=16, color="grey"),
        xanchor="right", yanchor="bottom"
    )

    # Create figure with dark theme
    fig = go.Figure(data=[node_trace],
                    layout=go.Layout(
                        title="Map the Internet",
                        showlegend=False,
                        hovermode="closest",
                        #template="plotly_dark",
                        margin=dict(b=0, l=0, r=0, t=40),
                        xaxis=dict(showgrid=False, zeroline=False, visible=False),
                        yaxis=dict(showgrid=False, zeroline=False, visible=False),
                        annotations=[annotation_bottom_right]))

    # Save to HTML and PNG
    fig.write_html(output_html)
    fig.write_image(output_png, width=4096, height=2304)

# Main workflow
def main():
    conn = establish_db_connection()
    try:
        relationships, domain_map = fetch_graph_data(conn)
        G = generate_graph(relationships, domain_map)
        create_plotly_graph(G)
        print("Graph generated successfully.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
