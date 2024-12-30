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

        # Create domain mappping table
        cursor.execute("""
            CREATE TABLE static_domain_mappings (
                id INT AUTO_INCREMENT PRIMARY KEY,
                old_domain VARCHAR(255) NOT NULL,
                new_domain VARCHAR(255) NOT NULL,
                wildcard BOOLEAN NOT NULL DEFAULT FALSE
            );
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

def set_domain_mapping(conn, old_domain, new_domain, wildcard=False):
    try:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO static_domain_mappings (old_domain, new_domain, wildcard) VALUES (?, ?, ?)", (old_domain, new_domain, wildcard))
        conn.commit()
        logging.info(f"Domain mapping added successfully.")
    except mariadb.Error as e:
        logging.error(f"Error adding domain mapping: {e}")
        conn.rollback()

# Ask for custom rules
def ask_for_rules(conn):
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
    
    while True:
        old_domain = input("Enter the old domain to map (or 'done' to finish): ").strip()
        if old_domain.lower() == 'done':
            break
        new_domain = input("Enter the new domain to map: ").strip()
        wildcard_input = input("Is this a wildcard mapping (using * )? (y/n): ").strip().lower()
        wildcard = wildcard_input == 'y'
        set_domain_mapping(conn, old_domain, new_domain, wildcard)

if __name__ == "__main__":
    conn = connect_to_db()
    setup_tables(conn)

    # Ask for the initial link
    initial_url = input("Enter the initial URL to start the crawl: ").strip()
    if not initial_url.endswith("/"):
        initial_url += "/"
    if initial_url:
        add_initial_link(conn, initial_url)

    # Ask if default rules should be used
    default_rules = input("Use default config rules? (y/n): ").strip().lower()
    if default_rules == "y":
        set_domain_link_limit(conn, 3500)
        set_domain_mapping(conn, "x.com", "twitter.com")
        set_domain_mapping(conn, "discord.gg", "discord.com")
        set_domain_mapping(conn, "youtu.be", "youtube.com")
    else:
        ask_for_rules(conn)

    conn.close()
