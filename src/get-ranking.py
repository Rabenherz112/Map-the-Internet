from dotenv import load_dotenv
import os
import mariadb
import logging

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

# Fetch the top 50 domains with the most parent connections
def fetch_top_domains(conn):
    try:
        query = """
            SELECT d.domain, COUNT(dr.parent_id) AS parent_count
            FROM domains d
            INNER JOIN domain_relationships dr ON d.id = dr.child_id
            GROUP BY d.domain
            ORDER BY parent_count DESC
            LIMIT 50;
        """
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        return results
    except mariadb.Error as e:
        logging.error(f"Error fetching top domains: {e}")
        return []

# Search for a specific domain and its ranking
def search_domain_with_ranking(conn, domain):
    try:
        query = (
            """
            SELECT rank, domain, link_count FROM (
                SELECT ROW_NUMBER() OVER (ORDER BY COUNT(dr.child_id) DESC) AS rank,
                    d.domain,
                    COUNT(dr.child_id) AS link_count
                FROM domain_relationships dr
                JOIN domains d ON dr.child_id = d.id
                GROUP BY d.domain
            ) ranked_domains
            WHERE domain = ?;
            """
        )
        cursor = conn.cursor()
        cursor.execute(query, (domain,))
        result = cursor.fetchone()
        return result
    except mariadb.Error as e:
        logging.error(f"Error searching for domain: {e}")
        return None

# Main function
def main():
    conn = establish_db_connection()
    try:
        choice = input("Enter '1' to display top 50 domains or '2' to search for a domain: ").strip()

        if choice == '1':
            logging.info("Fetching top 50 domains by parent connections...")
            top_domains = fetch_top_domains(conn)

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
            result = search_domain_with_ranking(conn, domain)

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
