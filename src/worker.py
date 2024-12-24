from dotenv import load_dotenv
import os
import mariadb
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse
import logging

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configure custom User-Agent
USER_AGENT = "MapWWWBot/1.0 (+http://map-the-internet.theravenhub.com/botinfo)"
HEADERS = {"User-Agent": USER_AGENT}

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

# Executes a query with retry logic to handle deadlocks
def execute_with_retry(cursor, query, params=None, retries=3, delay=1):
    for attempt in range(retries):
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return True
        except mariadb.Error as e:
            if "Deadlock found" in str(e):
                logging.warning(f"Deadlock detected on attempt {attempt + 1}. Retrying...")
                time.sleep(delay)  # Wait before retrying
            else:
                logging.error(f"Database error: {e}")
                return False
    logging.error("Failed to execute query after retries.")
    return False

# Add a new link to the queue
def add_link_to_queue(conn, url):
    try:
        parsed_url = urlparse(url)
        cleaned_url = urlunparse((parsed_url.scheme, parsed_url.netloc, parsed_url.path, '', '', ''))
        cursor = conn.cursor()
        cursor.execute("INSERT IGNORE INTO link_queue (url, status) VALUES (?, 'pending')", (cleaned_url,))
        rows_affected = cursor.rowcount  # Check if the insert was successful
        conn.commit()
        return rows_affected > 0  # Return True if the link was added, False otherwise
    except mariadb.Error as e:
        logging.error(f"Error inserting link into queue: {e}")
        return False

# Fetch the next pending link
def get_next_link(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT url FROM link_queue WHERE status = 'pending' LIMIT 1 FOR UPDATE")
        result = cursor.fetchone()
        if result:
            url = result[0]
            cursor.execute("UPDATE link_queue SET status = 'processing' WHERE url = ?", (url,))
            conn.commit()
            return url
        return None
    except mariadb.Error as e:
        logging.error(f"Error fetching next link: {e}")
        return None

# Mark a link as done
def mark_link_done(conn, url):
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE link_queue SET status = 'done' WHERE url = ?", (url,))
        conn.commit()
    except mariadb.Error as e:
        logging.error(f"Error marking link as done: {e}")

# Mark a link as unreachable
def mark_link_unreachable(conn, url):
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE link_queue SET status = 'unreachable' WHERE url = ?", (url,))
        conn.commit()
    except mariadb.Error as e:
        logging.error(f"Error marking link as unreachable: {e}")

# Check robots.txt for crawl permissions
def check_robots_txt(base_url):
    try:
        parsed_url = urlparse(base_url)
        robots_url = urljoin(f"{parsed_url.scheme}://{parsed_url.netloc}", "/robots.txt")
        response = requests.get(robots_url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            user_agent_lines = []
            disallow_lines = []
            current_user_agent = None

            # Generate possible user agent variants
            user_agent_variants = [USER_AGENT]
            user_agent_parts = USER_AGENT.split('/')
            if user_agent_parts:
                user_agent_variants.append(user_agent_parts[0])
                if len(user_agent_parts) > 1:
                    user_agent_variants.append(f"{user_agent_parts[0]}/{user_agent_parts[1].split(' ')[0]}")

            for line in response.text.splitlines():
                line = line.strip()
                if line.startswith("User-agent:"):
                    current_user_agent = line.split(":")[1].strip()
                elif line.startswith("Disallow:") and current_user_agent:
                    disallow_path = line.split(":", 1)[1].strip() if ":" in line else ""
                    user_agent_lines.append((current_user_agent, disallow_path))

            for user_agent, disallow in user_agent_lines:
                if user_agent in ["*"] + user_agent_variants:
                    if disallow:
                        disallow_lines.append(disallow)

            return not any(base_url.startswith(urljoin(base_url, disallow)) for disallow in disallow_lines)
        return True  # If robots.txt doesn't exist or isn't restrictive
    except requests.RequestException as e:
        logging.warning(f"Error fetching robots.txt: {e}")
        return True

# Fetch links from a webpage
def discover_links(base_url):
    if not check_robots_txt(base_url):
        logging.info(f"Crawling disallowed by robots.txt for {base_url}")
        return None

    try:
        response = requests.get(base_url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        links = set()
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            absolute_url = urljoin(base_url, href)
            parsed_url = urlparse(absolute_url)
            if parsed_url.scheme in ["http", "https"]:
                if any(parsed_url.path.endswith(ext) for ext in ["", "/", ".html", ".htm", ".php", ".asp", ".aspx", ".jsp", ".cfm", ".shtml", ".xhtml", ".rhtml", ".cgi", ".pl"]):
                    links.add(absolute_url)

        return links
    except requests.RequestException as e:
        logging.error(f"Error fetching {base_url}: {e}")
        return None

# Add domains and relationships to the database
def add_domain_and_relationship(conn, parent_url, child_url, domain_link_limit):
    try:
        cursor = conn.cursor()

        # Add the parent domain
        parent_domain = urlparse(parent_url).netloc
        execute_with_retry(cursor, "INSERT IGNORE INTO domains (domain) VALUES (?)", (parent_domain,))
        execute_with_retry(cursor, "SELECT id FROM domains WHERE domain = ?", (parent_domain,))
        parent_id = cursor.fetchone()[0]

        # Add the child domain
        child_domain = urlparse(child_url).netloc
        cursor.execute("SELECT id, processed_links FROM domains WHERE domain = ?", (child_domain,))
        child_domain_data = cursor.fetchone()

        if not child_domain_data:
            execute_with_retry(cursor, "INSERT IGNORE INTO domains (domain) VALUES (?)", (child_domain,))
            execute_with_retry(cursor, "SELECT id, processed_links FROM domains WHERE domain = ?", (child_domain,))
            child_id, child_processed_links = cursor.fetchone()
        else:
            child_id, child_processed_links = child_domain_data

        # Check if the child domain has exceeded the link limit
        if domain_link_limit > 0 and child_processed_links >= domain_link_limit:
            logging.info(f"Skipping {child_url}: link limit reached for domain {child_domain}")
            return

        # Attempt to add the link to the queue
        link_added = add_link_to_queue(conn, child_url)

        if link_added:
            # Add the relationship
            execute_with_retry(cursor, "INSERT IGNORE INTO domain_relationships (parent_id, child_id) VALUES (?, ?)", (parent_id, child_id))

            # Increment processed_links for the child domain
            execute_with_retry(cursor, "UPDATE domains SET processed_links = processed_links + 1 WHERE id = ?", (child_id,))

            conn.commit()
    except mariadb.Error as e:
        logging.error(f"Error adding domain or relationship: {e}")

# Main function to process a single domain
def process_domain(conn, domain, domain_link_limit):
    logging.info(f"Processing domain: {domain}")
    links = discover_links(domain)
    if links is None:
        mark_link_unreachable(conn, domain)
    else:
        for link in links:
            add_domain_and_relationship(conn, domain, link, domain_link_limit)

# Main Flow
if __name__ == "__main__":
    conn = connect_to_db()

    # Fetch domain link limit from the settings table
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM settings WHERE name = 'domain_link_limit'")
    result = cursor.fetchone()
    domain_link_limit = int(result[0]) if result else 0

    while True:
        # Get the next link to process
        next_link = get_next_link(conn)
        if not next_link:
            logging.info("No pending links to process. Exiting.")
            break

        # Process the link
        process_domain(conn, next_link, domain_link_limit)

        # Mark the link as done if reachable
        if next_link:
            mark_link_done(conn, next_link)

    conn.close()
