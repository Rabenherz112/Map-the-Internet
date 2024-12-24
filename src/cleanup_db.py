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

# Cleanup link_queue
def cleanup_link_queue(conn):
    try:
        cursor = conn.cursor()

        # Reset links stuck in 'processing' status to 'pending'
        cursor.execute("UPDATE link_queue SET status = 'pending' WHERE status = 'processing'")
        rows_reset = cursor.rowcount

        # Delete links with status 'done'
        cursor.execute("DELETE FROM link_queue WHERE status = 'done'")
        rows_deleted = cursor.rowcount
        
        conn.commit()

        logging.info(f"Cleanup completed. {rows_reset} entries were reset to 'pending'. {rows_deleted} entries with status 'done' were removed.")
    except mariadb.Error as e:
        logging.error(f"Error during cleanup: {e}")
        conn.rollback()

if __name__ == "__main__":
    conn = connect_to_db()
    cleanup_link_queue(conn)
    conn.close()
