import os
import re
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# Sabitler (Ayarlar)
BASE_URL = "https://www.football-data.co.uk/"

# BAŞLANGIÇ SAYFALARI: data.php ve diğer ana veri sayfalarını dahil ederek tüm ligleri yakalamayı sağlar.
DATA_PAGES = ["data.php", "matches.php", "matches_new_leagues.php"]
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True) 

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36"}
MAX_RETRIES = 3
RETRY_DELAY = 5

# İstenmeyen dosyalar listesi
UNWANTED_FILES = ['example.csv', 'Latest_Results.csv']

def parse_league_and_season(csv_url):
    """
    Dosya adından ligi ve sezon yılını (sıralama anahtarı olarak) çıkarır.
    """
    filename = os.path.basename(csv_url)
    # Lig kodunu ve opsiyonel sezon yılını (2 veya 4 hane) eşleştir
    match = re.match(r"([A-Z]+\d*)(?:_(\d{2,4}))?\.csv", filename)
    league, season_str = ("misc", None)
    
    if match:
        league = match.group(1)
        season_str = match.group(2)
        
    season_key = 9999
    
    if season_str:
        if len(season_str) == 2:
            season_int = int(season_str)
            # 93 ve sonrası 1900'ler, 93'ten küçükler 2000'ler varsayılır
            if season_int >= 93:
                prefix = '19'
            else:
                prefix = '20'
            season_key = int(prefix + season_str)
            
        elif len(season_str) == 4:
            season_key = int(season_str)

    return league, season_key, filename

def get_all_csv_links():
    """Tüm CSV linklerini çeker, lig ve sezona göre kronolojik olarak sıralar."""
    raw_links = []
    # pages_to_visit'i DATA_PAGES'daki tüm sayfalarla başlat
    pages_to_visit = set(DATA_PAGES) 
    visited_pages = set()
    
    while pages_to_visit:
        page_path = pages_to_visit.pop() 
        url = urljoin(BASE_URL, page_path)
        
        if page_path in visited_pages:
            continue
        visited_pages.add(page_path)
        
        print(f"Checking page: {url}")
        
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(url, timeout=30, headers=HEADERS)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, "html.parser")
                
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    full_link = urljoin(url, href)
                    
                    # 1. CSV dosyalarını topla (Veri çekilecek dosyalar)
                    if full_link.lower().endswith(".csv"):
                        filename = os.path.basename(full_link)
                        
                        if filename not in UNWANTED_FILES:
                            raw_links.append(full_link)
                        else:
                            print(f"[INFO] Skipping unwanted file link: {filename}")
                    
                    # 2. PHP Link takibini yap (Eksik veriyi çözen kritik kısım)
                    elif full_link.lower().endswith(".php"):
                        parsed_url = urlparse(full_link)
                        page_file_name = os.path.basename(parsed_url.path)

                        # Sadece kök dizinde bulunan ve genel olarak veri içermeyen (navigasyonel) sayfalar olmayanları takip et.
                        # Bu filtre, sizin listelediğiniz TÜM LİG sayfalarını (england.php, germany.php vb.) yakalar.
                        if parsed_url.path == f'/{page_file_name}' and \
                           page_file_name not in visited_pages and \
                           page_file_name not in ['index.php', 'notes.php', 'disclaimer.php', 'help.php', 'contact.php']: 
                            
                            pages_to_visit.add(page_file_name)
                            
                break
                
            except Exception as e:
                print(f"[WARNING] Attempt {attempt+1} failed for {url}: {e}")
                if attempt == MAX_RETRIES - 1:
                    print(f"[ERROR] Failed to crawl page after {MAX_RETRIES} retries: {url}")
                else:
                    time.sleep(RETRY_DELAY)

    # Linkleri benzersiz yapın ve sıralama için hazır hale getirin
    unique_links = sorted(list(set(raw_links))) 
    sortable_links = []
    
    for url in unique_links:
        league, season_key, _ = parse_league_and_season(url)
        sortable_links.append((league, season_key, url))
        
    sortable_links.sort(key=lambda x: (x[0], x[1]))
    
    sorted_links = [url for league, season_key, url in sortable_links]
    
    return sorted_links

def download_csv(csv_url):
    """CSV dosyasını indirir, lig klasörüne kaydeder ve değişiklik olup olmadığını kontrol eder."""
    league, season_key, filename = parse_league_and_season(csv_url) 
    
    # LİG KLASÖRÜ OLUŞTURMA: data/E0, data/SP1, vb.
    league_dir = os.path.join(DATA_DIR, league)
    os.makedirs(league_dir, exist_ok=True) 
    
    filepath = os.path.join(league_dir, filename)

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(csv_url, timeout=60, headers=HEADERS)
            response.raise_for_status()
            new_content = response.content

            # Dosya var ve içerik değişmemişse atla
            if os.path.exists(filepath):
                with open(filepath, "rb") as f:
                    old_content = f.read()
                if old_content == new_content:
                    print(f"[UNCHANGED] {filepath}")
                    return

            # Dosyayı içeriği olduğu gibi yazar (hiçbir sütun atılmaz!)
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
    print("Starting comprehensive CSV fetch process...")
    
    csv_links = get_all_csv_links() 
    print(f"Found {len(csv_links)} unique CSV files to download.")
    print("Starting download in chronological order (Oldest Season -> Newest Season, by League)...")
    
    for csv_url in csv_links: 
        download_csv(csv_url)
        
    print("CSV fetch process completed.")

if __name__ == "__main__":
    main()
