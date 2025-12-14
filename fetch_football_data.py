import os
import re
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# Sabitler (Ayarlar)
BASE_URL = "https://www.football-data.co.uk/"

# LİNK KEŞFİ İÇİN GENİŞLETİLMİŞ BAŞLANGIÇ SAYFALARI (Tüm ligler ve veri noktaları)
DATA_PAGES = [
    "data.php", "matches.php", "matches_new_leagues.php", "download.php",
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


# --- KRONOLOJİK SIRALAMA MANTIĞI (Doğruluğu Kanıtlanmış) ---
def parse_league_and_season(csv_url):
    """URL'den lig kodunu (E0) ve klasör yolundan sezonu (9394/1415/2526) çıkarır."""
    parsed_url = urlparse(csv_url)
    path_segments = [s for s in parsed_url.path.upper().split('/') if s] 
    
    filename = os.path.basename(csv_url).upper()
    league = "MISC"
    season_key = 2099 

    league_name_match = re.match(r"([A-Z]+\d*)\.CSV", filename)
    if league_name_match:
        league = league_name_match.group(1)

    for segment in reversed(path_segments):
        # 4 haneli sezon klasörünü yakala (9394, 1415, 2526)
        if re.match(r"^\d{4}$", segment): 
            year_prefix = segment[:2] 
            season_int = int(year_prefix)
            
            if season_int >= 90:
                season_key = int('19' + year_prefix) 
            else:
                season_key = int('20' + year_prefix) 
            break
        # 2 haneli sezon dosyasını yakala (E0_99.csv gibi)
        elif re.match(r"^\d{2}$", segment) and segment.upper() in filename:
            season_int = int(segment)
            if season_int >= 90:
                season_key = int('19' + segment)
            else:
                season_key = int('20' + segment)
            break
    
    # E0.CSV gibi dosyalarda (sezon bilgisi içermeyen en yeni dosyalar)
    if filename == f"{league}.CSV" and season_key > 2090:
        season_key = 2099 
        
    return league, season_key, os.path.basename(csv_url)


# --- LİNK KEŞFİ VE SIRALAMA MANTIĞI (NİHAİ SÜRÜM) ---
def get_all_csv_links():
    """
    Tüm CSV linklerini, tüm başlangıç sayfalarını ziyaret ederek ve 
    HTML metni içinde REGEX kullanarak çeker (Agresif Link Keşfi).
    """
    raw_links = []
    csv_regex = re.compile(r'(mmz4281/[^/]+/[A-Z]\d*\.csv)', re.IGNORECASE)
    
    for page_path in DATA_PAGES:
        url = urljoin(BASE_URL, page_path)
        
        for attempt in range(MAX_RETRIES):
            try:
                time.sleep(0.5) 
                response = requests.get(url, timeout=30, headers=HEADERS)
                response.raise_for_status()
                html_content = response.text 

                # 1. YÖNTEM: BeautifulSoup ile tüm <a> etiketlerini kontrol et (standart yol)
                soup = BeautifulSoup(html_content, "html.parser")
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    full_link = urljoin(url, href)
                    if full_link.lower().endswith(".csv"):
                        # İstenmeyen dosyaları atla
                        filename = os.path.basename(full_link)
                        if filename.upper() not in [u.upper() for u in UNWANTED_FILES]:
                            raw_links.append(full_link)

                # 2. YÖNTEM: REGEX ile HTML metnindeki tüm mmz4281/.../*.csv kalıbını ara (AGRESİF YOL)
                for match in csv_regex.finditer(html_content):
                    relative_link = match.group(0)
                    full_link = urljoin(BASE_URL, relative_link)
                    raw_links.append(full_link)
                
                break 
                
            except Exception as e:
                if attempt == MAX_RETRIES - 1:
                    print(f"[ERROR] Failed to fetch links from {url} after {MAX_RETRIES} retries. Error: {e}")
                else:
                    time.sleep(RETRY_DELAY)

    # Linkleri benzersiz yap ve sıralama anahtarları ile eşleştir
    unique_links = sorted(list(set(raw_links))) 
    sortable_links = []
    
    for url in unique_links:
        league, season_key, original_filename = parse_league_and_season(url)
        sortable_links.append((league, season_key, url))
        
    # KRİTİK SIRALAMA: Önce Lig Kodu (Alfabetik), sonra Sezon Yılı (En eskiden en yeniye)
    sortable_links.sort(key=lambda x: (x[0], x[1]))
    sorted_links = [url for league, season_key, url in sortable_links]
    
    return sorted_links

# --- İNDİRME VE MAIN FONKSİYONLARI (Değişmedi) ---
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
    print("Starting fast, RAW content CSV fetch process (FINAL VERSION: AGGRESSIVE LINK DISCOVERY)...")
    
    csv_links = get_all_csv_links() 
    print(f"Found {len(csv_links)} unique CSV files to download.")
    
    if len(csv_links) > 0:
        print("\n--- Download Order Check (First 10) ---")
        for i, url in enumerate(csv_links[:10]):
             league, season_key, filename = parse_league_and_season(url)
             print(f"[{i+1}] {league} (Year: {season_key}): {filename}")
        print("--------------------------------------\n")
        
    print("Starting download in chronological order (Raw Content)...")
    
    for i, csv_url in enumerate(csv_links): 
        print(f"Processing [{i+1}/{len(csv_links)}]: {os.path.basename(csv_url)}")
        download_csv(csv_url)
        
    print("\nCSV fetch process completed.")

if __name__ == "__main__":
    main()
