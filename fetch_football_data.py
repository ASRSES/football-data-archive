import os
import re
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# Sabitler (Ayarlar)
BASE_URL = "https://www.football-data.co.uk/"

# LİNK KEŞFİ İÇİN BAŞLANGIÇ SAYFALARI (Tüm ana lig ve veri sayfaları)
DATA_PAGES = [
    "data.php", "matches.php", "matches_new_leagues.php",
    "englandm.php", "scotlandm.php", "germanym.php", "italym.php", "spainm.php", 
    "francem.php", "netherlandsm.php", "belgiumm.php", "portugalm.php", 
    "turkeym.php", "greecem.php", "argentina.php", "austria.php", 
    "brazil.php", "china.php", "denmark.php", "finland.php", "ireland.php", 
    "japan.php", "mexico.php", "norway.php", "poland.php", "romania.php", 
    "russia.php", "sweden.php", "switzerland.php", "usa.php", "others.php"      
]

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True) 

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36"}
MAX_RETRIES = 3
RETRY_DELAY = 5
UNWANTED_FILES = ['example.csv', 'Latest_Results.csv']

def parse_league_and_season(csv_url):
    """URL'den lig kodunu (E0) ve klasör yolundan sezonu (9394/1415/2526) çıkarır."""
    parsed_url = urlparse(csv_url)
    path_segments = [s for s in parsed_url.path.upper().split('/') if s] 
    
    filename = os.path.basename(csv_url).upper()
    league = "MISC"
    season_key = 2099 # Varsayılan en yeni (E0.csv gibi)

    # 1. Lig Adını Çıkarma (Örn: E0, I1, T1)
    league_name_match = re.match(r"([A-Z]+\d*)\.CSV", filename)
    if league_name_match:
        league = league_name_match.group(1)

    # 2. Sezon Anahtarını Çıkarma (9394, 1415, 2526 gibi klasörlerden)
    for segment in reversed(path_segments):
        if re.match(r"^\d{4}$", segment): # Dört haneli klasör (9394, 1415)
            year_prefix = segment[:2] 
            season_int = int(year_prefix)
            
            # Yıl öneki 90-99 ise 19xx, 00-89 ise 20xx
            if season_int >= 90:
                season_key = int('19' + year_prefix) 
            else:
                season_key = int('20' + year_prefix) 
            break
        elif re.match(r"^\d{2}$", segment): # İki haneli sezon formatı (E0_99.csv gibi)
            season_int = int(segment)
            if season_int >= 90:
                season_key = int('19' + segment)
            else:
                season_key = int('20' + segment)
            break
    
    # E0.CSV gibi dosyalarda (mevcut sezon), sezon klasörü bulunamadıysa en yeni olarak işaretle
    if filename == f"{league}.CSV" and season_key > 2090: # Sadece güncel sezon dosyası ise 2099 yap
        season_key = 2099 
        
    return league, season_key, os.path.basename(csv_url)

def get_all_csv_links():
    """Tüm CSV linklerini çeker ve kronolojik olarak sıralar."""
    raw_links = []
    pages_to_visit = set(DATA_PAGES) 
    visited_pages = set()
    
    while pages_to_visit:
        page_path = pages_to_visit.pop() 
        url = urljoin(BASE_URL, page_path)
        if page_path in visited_pages: continue
        visited_pages.add(page_path)
        
        # print(f"Checking page: {url}")
        
        for attempt in range(MAX_RETRIES):
            try:
                time.sleep(0.5) 
                response = requests.get(url, timeout=30, headers=HEADERS)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, "html.parser")
                
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    full_link = urljoin(url, href) 
                    
                    if full_link.lower().endswith(".csv"):
                        filename = os.path.basename(full_link)
                        if filename.upper() not in [u.upper() for u in UNWANTED_FILES]:
                            raw_links.append(full_link)
                        # else: # Gereksiz logu kaldırdık
                            # print(f"[INFO] Skipping unwanted file link: {filename}")
                    
                    elif full_link.lower().endswith(".php"):
                        parsed_url = urlparse(full_link)
                        page_file_name = os.path.basename(parsed_url.path)
                        if page_file_name and page_file_name not in visited_pages and page_file_name not in ['index.php', 'notes.php', 'disclaimer.php', 'help.php', 'contact.php', 'download.php']: 
                            pages_to_visit.add(page_file_name)
                            
                break
                
            except Exception as e:
                print(f"[ERROR] Failed to crawl page after {MAX_RETRIES} retries: {url}. Error: {e}")
                break

    unique_links = sorted(list(set(raw_links))) 
    sortable_links = []
    
    for url in unique_links:
        league, season_key, original_filename = parse_league_and_season(url)
        sortable_links.append((league, season_key, url))
        
    # KRİTİK SIRALAMA: Önce Lig Kodu (Alfabetik), sonra Sezon Yılı (Eskiden Yeniye)
    sortable_links.sort(key=lambda x: (x[0], x[1]))
    sorted_links = [url for league, season_key, url in sortable_links]
    
    return sorted_links

def download_csv(csv_url):
    """CSV dosyasını indirir ve orijinal içeriği değiştirilmeden kaydeder."""
    league, season_key, filename = parse_league_and_season(csv_url) 
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
                    return

            with open(filepath, "wb") as f:
                f.write(new_content)
            print(f"[UPDATED] {filepath}")
            return
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
            else:
                print(f"[ERROR] Could not download CSV: {csv_url}")

def main():
    print("Starting fast, RAW content CSV fetch process (FINAL VERSION)...")
    
    csv_links = get_all_csv_links() 
    print(f"Found {len(csv_links)} unique CSV files to download.")
    print("Starting download in chronological order (Raw Content)...")
    
    for i, csv_url in enumerate(csv_links): 
        print(f"Processing [{i+1}/{len(csv_links)}]: {os.path.basename(csv_url)}")
        download_csv(csv_url)
        
    print("\nCSV fetch process completed.")

if __name__ == "__main__":
    main()
