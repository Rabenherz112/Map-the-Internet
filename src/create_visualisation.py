from dotenv import load_dotenv
import os
import mariadb
import networkx as nx
import matplotlib.pyplot as plt
from datetime import datetime
import logging

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection setup
def connect_to_db():
    try:
        conn = mariadb.connect(
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT")),
            database=os.getenv("DB_NAME")
        )
        return conn
    except mariadb.Error as e:
        logging.error(f"Error connecting to MariaDB: {e}")
        raise

# Fetch domain relationships from the database
def fetch_relationships(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT parent_id, child_id FROM domain_relationships")
        relationships = cursor.fetchall()

        cursor.execute("SELECT id, domain FROM domains")
        domains = {row[0]: row[1] for row in cursor.fetchall()}

        return relationships, domains
    except mariadb.Error as e:
        logging.error(f"Error fetching relationships: {e}")
        return [], {}

# Create and save the visualization
def create_visualisation(relationships, domains, output_file):
    G = nx.Graph()

    # Add nodes with domain names
    for domain_id, domain_name in domains.items():
        G.add_node(domain_id, label=domain_name)

    # Add edges based on relationships, excluding self-loops
    for parent_id, child_id in relationships:
        if parent_id in domains and child_id in domains and parent_id != child_id:
            G.add_edge(parent_id, child_id)

    # Node sizes based on the number of incoming edges
    node_sizes = [G.degree(node) * 100 for node in G.nodes]

    # Generate spring layout
    pos = nx.spring_layout(G, seed=42)

    # Create the plot
    plt.figure(figsize=(20, 20), dpi=1200)
    nx.draw(
        G,
        pos,
        labels={node: G.nodes[node]['label'] for node in G.nodes},
        with_labels=True,
        node_size=node_sizes,
        font_size=8,
        font_color="black",
        font_weight="bold",
        edge_color="gray"
    )

    # Add watermark
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    watermark = f"Map the Internet Project - Rabenherz112\nGenerated: {timestamp}"
    plt.text(0.99, 0.01, watermark, fontsize=10, color="gray", ha='right', va='bottom', transform=plt.gcf().transFigure)

    # Save the high-resolution image
    plt.savefig(output_file, bbox_inches="tight")
    plt.close()
    logging.info(f"Graph saved as {output_file}")

if __name__ == "__main__":
    conn = connect_to_db()
    relationships, domains = fetch_relationships(conn)

    if relationships and domains:
        create_visualisation(relationships, domains, "domain_relationship_graph.png")
    else:
        logging.error("No data available to create visualization.")

    conn.close()
