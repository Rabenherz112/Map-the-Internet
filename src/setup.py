from dotenv import load_dotenv
import os
import mariadb
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

# Create necessary tables
def setup_tables(conn):
    try:
        cursor = conn.cursor()

        # Create link_queue table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS link_queue (
                id INT AUTO_INCREMENT PRIMARY KEY,
                url TEXT NOT NULL,
                status ENUM('pending', 'processing', 'done', 'unreachable') NOT NULL,
                UNIQUE(url)
            )
        """)

        # Create domains table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS domains (
                id INT AUTO_INCREMENT PRIMARY KEY,
                domain VARCHAR(255) NOT NULL,
                processed_links INT DEFAULT 0,
                UNIQUE(domain)
            )
        """)

        # Create domain_relationships table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS domain_relationships (
                id INT AUTO_INCREMENT PRIMARY KEY,
                parent_id INT NOT NULL,
                child_id INT NOT NULL,
                FOREIGN KEY (parent_id) REFERENCES domains(id) ON DELETE CASCADE,
                FOREIGN KEY (child_id) REFERENCES domains(id) ON DELETE CASCADE,
                UNIQUE(parent_id, child_id)
            )
        """)

        # Create settings table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                name VARCHAR(255) PRIMARY KEY,
                value VARCHAR(255) NOT NULL
            )
        """)

        conn.commit()
        logging.info("Tables created successfully.")
    except mariadb.Error as e:
        logging.error(f"Error creating tables: {e}")
        conn.rollback()

# Add the initial link to the queue
def add_initial_link(conn, initial_url):
    try:
        cursor = conn.cursor()
        cursor.execute("INSERT IGNORE INTO link_queue (url, status) VALUES (?, 'pending')", (initial_url,))
        conn.commit()
        logging.info(f"Initial link '{initial_url}' added to the queue.")
    except mariadb.Error as e:
        logging.error(f"Error adding initial link: {e}")
        conn.rollback()

# Set the domain link limit in the settings table
def set_domain_link_limit(conn, limit):
    try:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO settings (name, value) VALUES ('domain_link_limit', ?) ON DUPLICATE KEY UPDATE value = ?", (limit, limit))
        conn.commit()
        logging.info(f"Domain link limit set to {limit}.")
    except mariadb.Error as e:
        logging.error(f"Error setting domain link limit: {e}")
        conn.rollback()

if __name__ == "__main__":
    conn = connect_to_db()
    setup_tables(conn)

    # Ask for the initial link
    initial_url = input("Enter the initial URL to start the crawl: ").strip()
    if not initial_url.endswith("/"):
        initial_url += "/"
    if initial_url:
        add_initial_link(conn, initial_url)

    # Ask for the domain link limit
    while True:
        try:
            domain_link_limit = int(input("Enter the maximum number of links to scan per domain (0 for unlimited): ").strip())
            if domain_link_limit >= 0:
                set_domain_link_limit(conn, domain_link_limit)
                break
            else:
                print("Please enter a non-negative integer.")
        except ValueError:
            print("Invalid input. Please enter an integer.")

    conn.close()
