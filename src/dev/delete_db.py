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

# Drop all tables
def drop_tables(conn):
    try:
        cursor = conn.cursor()

        # Disable foreign key checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")

        # Drop tables
        cursor.execute("DROP TABLE IF EXISTS link_queue;")
        cursor.execute("DROP TABLE IF EXISTS domains;")
        cursor.execute("DROP TABLE IF EXISTS domain_relationships;")
        cursor.execute("DROP TABLE IF EXISTS settings;")

        # Re-enable foreign key checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")

        conn.commit()
        logging.info("All tables have been successfully dropped.")
    except mariadb.Error as e:
        logging.error(f"Error dropping tables: {e}")
        conn.rollback()

if __name__ == "__main__":
    conn = connect_to_db()
    drop_tables(conn)
    conn.close()
