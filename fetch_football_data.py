import os
import re
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import pandas as pd # KRİTİK: Veri standardizasyonu için Pandas eklendi
from io import StringIO

# Sabitler (Ayarlar)
BASE_URL = "https://www.football-data.co.uk/"

# NİHAİ VE EKSİKSİZ DATA_PAGES: Tüm lig sayfaları dahil edildi
DATA_PAGES = [
    "data.php", "matches.php", "matches_new_leagues.php",
    # Ana Ligler
    "englandm.php", "scotlandm.php", "germanym.php", "italym.php", "spainm.php", 
    "francem.php", "netherlandsm.php", "belgiumm.php", "portugalm.php", 
    "turkeym.php", "greecem.php",      
    # Ekstra Ligler
    "argentina.php", "austria.php", "brazil.php", "china.php", "denmark.php", 
    "finland.php", "ireland.php", "japan.php", "mexico.php", "norway.php", 
    "poland.php", "romania.php", "russia.php", "sweden.php", "switzerland.php", 
    "usa.php", "others.php"      
]

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True) 

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36"}
MAX_RETRIES = 3
RETRY_DELAY = 5
UNWANTED_FILES = ['example.csv', 'Latest_Results.csv']

# Evrensel (Universal) başlıkları tutacak global set.
ALL_COLUMNS = set()

def parse_league_and_season(csv_url):
    """Dosya adından ligi ve sezon yılını (sıralama anahtarı) çıkarır."""
    filename = os.path.basename(csv_url)
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
    pages_to_visit = set(DATA_PAGES) 
    visited_pages = set()
    
    while pages_to_visit:
        page_path = pages_to_visit.pop() 
        url = urljoin(BASE_URL, page_path)
        if page_path in visited_pages: continue
        visited_pages.add(page_path)
        
        print(f"Checking page: {url}")
        
        for attempt in range(MAX_RETRIES):
            try:
                time.sleep(0.5) 
                response = requests.get(url, timeout=30, headers=HEADERS)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, "html.parser")
                
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    full_link = urljoin(url, href)
                    
                    # SADECE CSV dosyalarını topla
                    if full_link.lower().endswith(".csv"):
                        filename = os.path.basename(full_link)
                        if filename not in UNWANTED_FILES:
                            raw_links.append(full_link)
                        else:
                            print(f"[INFO] Skipping unwanted file link: {filename}")
                    
                    # PHP Link takibini yap
                    elif full_link.lower().endswith(".php"):
                        parsed_url = urlparse(full_link)
                        page_file_name = os.path.basename(parsed_url.path)
                        if parsed_url.path == f'/{page_file_name}' and page_file_name not in visited_pages and page_file_name not in ['index.php', 'notes.php', 'disclaimer.php', 'help.php', 'contact.php']: 
                            pages_to_visit.add(page_file_name)
                            
                break
                
            except Exception as e:
                print(f"[WARNING] Attempt {attempt+1} failed for {url}: {e}")
                if attempt == MAX_RETRIES - 1:
                    print(f"[ERROR] Failed to crawl page after {MAX_RETRIES} retries: {url}")
                else:
                    time.sleep(RETRY_DELAY)

    unique_links = sorted(list(set(raw_links))) 
    sortable_links = []
    
    for url in unique_links:
        league, season_key, _ = parse_league_and_season(url)
        sortable_links.append((league, season_key, url))
        
    # KRİTİK SIRALAMA
    sortable_links.sort(key=lambda x: (x[0], x[1]))
    sorted_links = [url for league, season_key, url in sortable_links]
    
    return sorted_links

def standardize_and_save_csv(csv_url):
    """
    CSV dosyasını indirir, evrensel başlıkları kullanarak standardize eder 
    ve değişiklik yoksa yazma işlemini atlar.
    """
    league, season_key, filename = parse_league_and_season(csv_url) 
    league_dir = os.path.join(DATA_DIR, league)
    os.makedirs(league_dir, exist_ok=True) 
    filepath = os.path.join(league_dir, filename)

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(csv_url, timeout=60, headers=HEADERS)
            response.raise_for_status()
            
            # --- YENİ BÖLÜM 1: Veri Yükleme ve Başlık Toplama ---
            try:
                # İndirilen veriyi Pandas ile oku
                # ISO-8859-1: Web sitesindeki yaygın kodlama
                # on_bad_lines="skip": Kötü formatlı satırları atla
                df = pd.read_csv(StringIO(response.text), low_memory=False, on_bad_lines="skip", encoding='ISO-8859-1')
            except pd.errors.ParserError as e:
                 print(f"[WARNING] CSV parsing error for {filename}: {e}. Skipping file.")
                 return None 
            
            # Başlıkları küresel sete ekle
            global ALL_COLUMNS
            ALL_COLUMNS.update(df.columns.tolist())
            
            return response.content # Başarıyla indirilen ham içeriği döndür

        except Exception as e:
            # Hata yönetimi (loglama ve tekrar deneme)
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
            else:
                print(f"[ERROR] Could not download CSV after {MAX_RETRIES} attempts: {csv_url}")
                return None 
    return None 

def main():
    print("Starting comprehensive CSV fetch and standardization process...")
    
    csv_links = get_all_csv_links() 
    print(f"Found {len(csv_links)} unique CSV files to process.")
    
    # 1. Aşama: Tüm dosyaları indir ve tüm evrensel başlıkları (ALL_COLUMNS) topla
    print("\n--- PHASE 1: Collecting all unique column names (Headers) ---")
    file_contents = {}
    
    for i, csv_url in enumerate(csv_links):
        content = standardize_and_save_csv(csv_url) 
        if content is not None:
            file_contents[csv_url] = content
            print(f"[{i+1}/{len(csv_links)}] Header collected for {os.path.basename(csv_url)}")
        
    if not ALL_COLUMNS:
        print("[FATAL ERROR] No columns found. Exiting.")
        return

    universal_columns = sorted(list(ALL_COLUMNS))
    print(f"\n--- Universal Header Found ({len(universal_columns)} total columns) ---")
    # print(universal_columns[:5], "...") # Debug için gösterilebilir

    # 2. Aşama: Tüm dosyaları standardize et ve kaydet
    print("\n--- PHASE 2: Standardizing and Saving Files ---")
    
    for i, csv_url in enumerate(csv_links):
        league, season_key, filename = parse_league_and_season(csv_url)
        filepath = os.path.join(DATA_DIR, league, filename)
        
        if csv_url not in file_contents:
            print(f"[{i+1}/{len(csv_links)}] Skipping {filename} (Download failed in Phase 1).")
            continue

        try:
            # Ham içeriği tekrar oku ve DataFrame'e yükle
            raw_content = file_contents[csv_url]
            df = pd.read_csv(StringIO(raw_content.decode('ISO-8859-1')), low_memory=False, on_bad_lines="skip", encoding='ISO-8859-1')

            # Standardlaştırma: Eksik sütunları ekle (NaN ile doldurulur)
            for col in universal_columns:
                if col not in df.columns:
                    df[col] = pd.NA

            # Sütunları evrensel sıraya göre yeniden düzenle
            df = df[universal_columns]
            
            # Yeni içeriği CSV formatına dönüştür (UTF-8 kodlama ile)
            new_csv_content = df.to_csv(index=False, header=True, encoding='utf-8')
            new_content_bytes = new_csv_content.encode('utf-8')
            
            # Değişiklik Kontrolü
            if os.path.exists(filepath):
                with open(filepath, "r", encoding='utf-8') as f:
                    old_csv_content = f.read()
                old_content_bytes = old_csv_content.encode('utf-8')

                if old_content_bytes == new_content_bytes:
                    print(f"[{i+1}/{len(csv_links)}] [UNCHANGED] {filepath}")
                    continue

            # Dosyayı yazar
            with open(filepath, "wb") as f:
                f.write(new_content_bytes)
            print(f"[{i+1}/{len(csv_links)}] [STANDARDIZED & UPDATED] {filepath}")

        except Exception as e:
            print(f"[ERROR] Could not standardize/save {filepath}: {e}")

    print("\nCSV fetch and standardization process completed.")

if __name__ == "__main__":
    main()
