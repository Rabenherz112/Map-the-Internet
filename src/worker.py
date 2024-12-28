from dotenv import load_dotenv
import os
import mariadb
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse
import logging
import time
import validators

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configure custom User-Agent
USER_AGENT = "Mozilla/5.0 (compatible; MapWWWBot/1.0; +http://map-the-internet.theravenhub.com/botinfo)"
HEADERS = {"User-Agent": USER_AGENT, "Accept": "*/*"}

# Database connection setup
def establish_db_connection():
    try:
        conn = mariadb.connect(
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT")),
            database=os.getenv("DB_NAME"),
            autocommit = False
        )
        return conn
    except mariadb.Error as e:
        logging.error(f"Error connecting to MariaDB: {e}")
        raise

def execute(conn, query, params=None):
    try:
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        conn.commit()
        rowcount = cursor.rowcount
        data = None
        try:
            data = cursor.fetchone()
        except mariadb.Error as e:
            pass
        return {
            'rowcount': rowcount,
            'data': data
        }
    except mariadb.Error as e:
        if "Deadlock found" in str(e):
            logging.warning(f"Deadlock detected in query '{query}'")
        else:
            logging.error(f"Database error: {e}")
            return False

# Add a new link to the queue
def queue_link(conn, url):
    try:
        parsed_url = urlparse(url)
        path = parsed_url.path
        # Add trailing slash if the path is empty or doesn't end with a slash and isn't a file
        if not path or not path.endswith('/'):
            path = path + '/' if '.' not in path.split('/')[-1] else path
        # Remove query parameters and fragments
        cleaned_url = urlunparse((parsed_url.scheme, parsed_url.netloc, path, '', '', ''))
        result = execute(conn, "INSERT IGNORE INTO link_queue (url, status) VALUES (?, 'pending')", (cleaned_url,))
        rows_affected = result["rowcount"]
        return rows_affected > 0
    except mariadb.Error as e:
        logging.error(f"Error inserting link into queue: {e}")
        return False


# Fetch the next pending link
def fetch_next_pending_link(conn):
    try:
        result = execute(conn, "SELECT url FROM link_queue WHERE status = 'pending' LIMIT 1")
        if result:
            url = result["data"][0]
            execute(conn, "UPDATE link_queue SET status = 'processing' WHERE url = ?", (url,))
            return url
        return None
    except mariadb.Error as e:
        logging.error(f"Error fetching next link: {e}")
        return None

# Update link status
def update_link_status(conn, url, status):
    try:
        execute(conn, "UPDATE link_queue SET status = ? WHERE url = ?", (status, url))
    except mariadb.Error as e:
        logging.error(f"Error updating link status to '{status}': {e}")

# Check robots.txt for crawl permissions
def is_crawling_allowed(base_url):
    try:
        parsed_url = urlparse(base_url)
        robots_url = urljoin(f"{parsed_url.scheme}://{parsed_url.netloc}", "/robots.txt")
        response = requests.get(robots_url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            user_agent_lines = []
            disallow_lines = []
            current_user_agent = None

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
        return True
    except requests.RequestException as e:
        logging.warning(f"Error fetching robots.txt: {e}")
        return True

# Fetch links from a webpage
def extract_links_from_page(base_url):
    if not is_crawling_allowed(base_url):
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
                # Check if the URL is valid
                if not validators.url(absolute_url):
                    logging.warning(f"Malformed URL detected: {absolute_url}")
                    continue
                # Check if the URL is a valid type
                if any(parsed_url.path.endswith(ext) for ext in ["", "/", ".html", ".htm", ".php", ".asp", ".aspx", ".jsp", ".cfm", ".shtml", ".xhtml", ".rhtml", ".phtml", ".cgi", ".pl"]):
                    links.add(absolute_url)
        return links
    except requests.RequestException as e:
        logging.error(f"Error fetching {base_url}: {e}")
        return None

# Add domains and relationships to the database
def store_domain_and_relationship(conn, parent_url, child_url, domain_link_limit):
    try:
        parent_domain = urlparse(parent_url).netloc
        execute(conn, "INSERT IGNORE INTO domains (domain) VALUES (?)", (parent_domain,))
        result = execute(conn, "SELECT id FROM domains WHERE domain = ?", (parent_domain,))
        parent_id = result["data"][0]

        child_domain = urlparse(child_url).netloc
        result = execute(conn, "SELECT id, processed_links FROM domains WHERE domain = ?", (child_domain,))
        child_domain_data = result["data"]

        if not child_domain_data:
            execute(conn, "INSERT IGNORE INTO domains (domain) VALUES (?)", (child_domain,))
            result = execute(conn, "SELECT id, processed_links FROM domains WHERE domain = ?", (child_domain,))
            child_id, child_processed_links = result["data"]
        else:
            child_id, child_processed_links = child_domain_data

        if domain_link_limit > 0 and child_processed_links >= domain_link_limit:
            logging.debug(f"Skipping {child_url}: link limit reached for domain {child_domain}")
            return

        link_added = queue_link(conn, child_url)

        if link_added:
            execute(conn, "INSERT IGNORE INTO domain_relationships (parent_id, child_id) VALUES (?, ?)", (parent_id, child_id))
            execute(conn, "UPDATE domains SET processed_links = processed_links + 1 WHERE id = ?", (child_id,))
    except mariadb.Error as e:
        logging.error(f"Error storing domain or relationship: {e}")

# Process a single link
def process_single_link(conn, link, domain_link_limit):
    logging.debug(f"Processing link: {link}")
    links = extract_links_from_page(link)
    if links is None:
        update_link_status(conn, link, 'unreachable')
    else:
        for discovered_link in links:
            store_domain_and_relationship(conn, link, discovered_link, domain_link_limit)

# Main workflow
if __name__ == "__main__":
    conn = establish_db_connection()

    # Get the domain link limit setting
    result = execute(conn, "SELECT value FROM settings WHERE name = 'domain_link_limit'")
    result = result["data"]
    domain_link_limit = int(result[0]) if result else 0

    counter = 0
    start_time = time.time()

    while True:
        next_link = fetch_next_pending_link(conn)
        if not next_link:
            logging.info("No pending links to process. Exiting.")
            break

        process_single_link(conn, next_link, domain_link_limit)
        update_link_status(conn, next_link, 'done')

        counter += 1

        # Log every 10 minutes
        current_time = time.time()
        if current_time - start_time >= 600:
            logging.info(f"Processed {counter} URLs in the last 10 minutes.")
            counter = 0
            start_time = current_time

    conn.close()
