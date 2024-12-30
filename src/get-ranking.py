from collections import defaultdict
from fnmatch import fnmatch
from dotenv import load_dotenv
import os
import mariadb
import logging
from publicsuffix2 import get_sld

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Fetch domain mappings from the database
def fetch_static_mappings(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT old_domain, new_domain, wildcard FROM static_domain_mappings")
        mappings = []
        for old_domain, new_domain, wildcard in cursor.fetchall():
            mappings.append((old_domain, new_domain, bool(wildcard)))
        return mappings
    except mariadb.Error as e:
        logging.error(f"Error fetching static mappings: {e}")
        return []

# Normalize domain
def normalize_domain(domain, static_mappings):
    # Check for static mappings and wildcard matches
    for old_domain, new_domain, is_wildcard in static_mappings:
        if is_wildcard and fnmatch(domain, old_domain):
            return new_domain
        if not is_wildcard and domain == old_domain:
            return new_domain

    # Normalize subdomains to their "main domain" using public suffixes
    main_domain = get_sld(domain)
    return main_domain

# Database connection setup
def establish_db_connection():
    try:
        conn = mariadb.connect(
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT")),
            database=os.getenv("DB_NAME"),
            autocommit=False
        )
        return conn
    except mariadb.Error as e:
        logging.error(f"Error connecting to MariaDB: {e}")
        raise

# Fetch the top 50 domains with the most parent connections (normalized and aggregated)
def fetch_top_domains(conn, static_mappings):
    try:
        query = """
            SELECT d.domain, COUNT(dr.parent_id) AS parent_count
            FROM domains d
            INNER JOIN domain_relationships dr ON d.id = dr.child_id
            GROUP BY d.domain;
        """
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()

        # Aggregate results based on normalized domain
        aggregated_results = defaultdict(int)
        for domain, parent_count in results:
            normalized_domain = normalize_domain(domain, static_mappings)
            aggregated_results[normalized_domain] += parent_count

        # Sort aggregated results by parent count in descending order
        sorted_results = sorted(aggregated_results.items(), key=lambda x: x[1], reverse=True)
        return sorted_results[:50]  # Return top 50 domains
    except mariadb.Error as e:
        logging.error(f"Error fetching top domains: {e}")
        return []

# Search for a specific domain and its ranking (normalized and aggregated)
def search_domain_with_ranking(conn, domain, static_mappings):
    try:
        normalized_domain = normalize_domain(domain, static_mappings)
        query = """
            SELECT d.domain, COUNT(dr.parent_id) AS parent_count
            FROM domains d
            INNER JOIN domain_relationships dr ON d.id = dr.child_id
            GROUP BY d.domain;
        """
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()

        # Aggregate results based on normalized domain
        aggregated_results = defaultdict(int)
        for domain, parent_count in results:
            normalized_domain_key = normalize_domain(domain, static_mappings)
            aggregated_results[normalized_domain_key] += parent_count

        # Sort aggregated results by parent count in descending order
        sorted_results = sorted(aggregated_results.items(), key=lambda x: x[1], reverse=True)

        # Find the ranking and return it
        for rank, (agg_domain, parent_count) in enumerate(sorted_results, start=1):
            if agg_domain == normalized_domain:
                return rank, agg_domain, parent_count

        return None  # If domain is not found
    except mariadb.Error as e:
        logging.error(f"Error searching for domain: {e}")
        return None

# Main function
def main():
    conn = establish_db_connection()
    try:
        # Fetch static mappings from the database
        static_mappings = fetch_static_mappings(conn)

        choice = input("Enter '1' to display top 50 domains or '2' to search for a domain: ").strip()

        if choice == '1':
            logging.info("Fetching top 50 domains by parent connections...")
            top_domains = fetch_top_domains(conn, static_mappings)

            if not top_domains:
                logging.error("No domains found. Is your database empty?")
                return

            logging.info("Top 50 Domains:")
            print(f"{'Rank':<5}{'Domain':<50}{'Parent Connections':<20}")
            print("=" * 75)
            for rank, (domain, parent_count) in enumerate(top_domains, start=1):
                print(f"{rank:<5}{domain:<50}{parent_count:<20}")

        elif choice == '2':
            domain = input("Enter the domain to search for: ").strip()
            result = search_domain_with_ranking(conn, domain, static_mappings)

            if result:
                rank, domain, parent_count = result
                print(f"{'Rank':<5}{'Domain':<50}{'Parent Connections':<20}")
                print("=" * 75)
                print(f"{rank:<5}{domain:<50}{parent_count:<20}")
            else:
                logging.error(f"Domain '{domain}' not found.")

        else:
            print("Invalid choice. Please enter '1' or '2'.")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
