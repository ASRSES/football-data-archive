# fetch_football_data.py
import os
import re
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

BASE_URL = "https://www.football-data.co.uk/"
DATA_PAGES = ["data.php"]
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

HEADERS = {"User-Agent": "Mozilla/5.0"}
MAX_RETRIES = 3
RETRY_DELAY = 5

def get_all_csv_links():
    links = []
    for page in DATA_PAGES:
        url = urljoin(BASE_URL, page)
        for attempt in range(MAX_RETRIES):
            try:
                html = requests.get(url, timeout=30, headers=HEADERS).text
                soup = BeautifulSoup(html, "html.parser")
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if href.lower().endswith(".csv"):
                        links.append(urljoin(BASE_URL, href))
                break
            except Exception as e:
                print(f"[WARNING] Attempt {attempt+1} failed for {url}: {e}")
                if attempt == MAX_RETRIES - 1:
                    print(f"[ERROR] BeautifulSoup failed, fallback to Selenium: {url}")
                    links += get_links_selenium(url)
                else:
                    time.sleep(RETRY_DELAY)
    return sorted(set(links))

def get_links_selenium(url):
    links = []
    try:
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        driver = webdriver.Chrome(options=options)
        driver.get(url)
        time.sleep(3)
        anchors = driver.find_elements("tag name", "a")
        for a in anchors:
            href = a.get_attribute("href")
            if href and href.lower().endswith(".csv"):
                links.append(href)
        driver.quit()
    except Exception as e:
        print(f"[ERROR] Selenium failed for {url}: {e}")
    return links

def parse_league_and_season(csv_url):
    filename = os.path.basename(csv_url)
    match = re.match(r"([A-Z]+\d*)(?:_(\d{2,4}))?\.csv", filename)
    league, season = ("misc", "current")
    if match:
        league = match.group(1)
        season = match.group(2) or "current"
    return league, season, filename

def download_csv(csv_url):
    league, season, filename = parse_league_and_season(csv_url)
    league_dir = os.path.join(DATA_DIR, league)
    os.makedirs(league_dir, exist_ok=True)
    filepath = os.path.join(league_dir, filename)

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(csv_url, timeout=60, headers=HEADERS)
            response.raise_for_status()
            new_content = response.content

            if os.path.exists(filepath):
                with open(filepath, "rb") as f:
                    old_content = f.read()
                if old_content == new_content:
                    print(f"[UNCHANGED] {filepath}")
                    return

            with open(filepath, "wb") as f:
                f.write(new_content)
            print(f"[UPDATED] {filepath}")
            return
        except Exception as e:
            print(f"[WARNING] Attempt {attempt+1} failed for {csv_url}: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
            else:
                print(f"[ERROR] Could not download CSV: {csv_url}")

def main():
    print("Starting CSV fetch process...")
    csv_links = get_all_csv_links()
    print(f"Found {len(csv_links)} CSV files")
    for csv_url in csv_links:
        download_csv(csv_url)
    print("CSV fetch process completed.")

if __name__ == "__main__":
    main()
