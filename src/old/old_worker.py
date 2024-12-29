import mariadb
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configure custom User-Agent
USER_AGENT = "MapWWWBot/1.0 (+http://map-the-internet.theravenhub.com/botinfo)"
HEADERS = {"User-Agent": USER_AGENT}

# Database connection setup
def connect_to_db():
    try:
        conn = mariadb.connect(
            user="user",
            password="password",
            host="host",
            port=3306,
            database="database"
        )
        return conn
    except mariadb.Error as e:
        logging.error(f"Error connecting to MariaDB: {e}")
        raise


# Add a new link to the queue
def add_link_to_queue(conn, url):
    try:
        parsed_url = urlparse(url)
        cleaned_url = urlunparse((parsed_url.scheme, parsed_url.netloc, parsed_url.path, '', '', ''))
        cursor = conn.cursor()
        cursor.execute("INSERT IGNORE INTO link_queue (url, status) VALUES (?, 'pending')", (cleaned_url,))
        conn.commit()
    except mariadb.Error as e:
        logging.error(f"Error inserting link into queue: {e}")

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
                if any(parsed_url.path.endswith(ext) for ext in ["", "/", ".html", ".htm", ".php", ".asp", ".aspx"]):
                    links.add(absolute_url)

        return links
    except requests.RequestException as e:
        logging.error(f"Error fetching {base_url}: {e}")
        return None

# Add domains and relationships to the database
def add_domain_and_relationship(conn, parent_url, child_url):
    try:
        cursor = conn.cursor()
        # Add the parent domain
        parent_domain = urlparse(parent_url).netloc
        cursor.execute("INSERT IGNORE INTO domains (domain) VALUES (?)", (parent_domain,))
        cursor.execute("SELECT id FROM domains WHERE domain = ?", (parent_domain,))
        parent_id = cursor.fetchone()[0]

        # Add the child domain
        child_domain = urlparse(child_url).netloc
        cursor.execute("INSERT IGNORE INTO domains (domain) VALUES (?)", (child_domain,))
        cursor.execute("SELECT id FROM domains WHERE domain = ?", (child_domain,))
        child_id = cursor.fetchone()[0]

        # Add the relationship
        cursor.execute("INSERT IGNORE INTO domain_relationships (parent_id, child_id) VALUES (?, ?)", (parent_id, child_id))
        conn.commit()
    except mariadb.Error as e:
        logging.error(f"Error adding domain or relationship: {e}")

# Main function to process a single domain
def process_domain(conn, domain):
    logging.info(f"Processing domain: {domain}")
    links = discover_links(domain)
    if links is None:
        mark_link_unreachable(conn, domain)
    else:
        for link in links:
            add_link_to_queue(conn, link)
            add_domain_and_relationship(conn, domain, link)

if __name__ == "__main__":
    conn = connect_to_db()

    while True:
        # Get the next link to process
        next_link = get_next_link(conn)
        if not next_link:
            logging.info("No pending links to process. Exiting.")
            break

        # Process the link
        process_domain(conn, next_link)

        # Mark the link as done if reachable
        if next_link:
            mark_link_done(conn, next_link)

    conn.close()
