import os
import re
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# Sabitler (Ayarlar)
BASE_URL = "https://www.football-data.co.uk/"
# Sadece ana dizin sayfasından başlayacağız ve tüm alt sayfaları takip edeceğiz.
DATA_PAGES = ["data.php"]
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True) # Ana veri klasörünü oluştur

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36"}
MAX_RETRIES = 3
RETRY_DELAY = 5

def parse_league_and_season(csv_url):
    """
    Dosya adından ligi ve sezon yılını (sıralama anahtarı olarak) çıkarır.
    1993'ten itibaren kronolojik sıralamayı garanti eder.
    """
    filename = os.path.basename(csv_url)
    # Match kısaltmaları: E0, I1, SP1 gibi
    match = re.match(r"([A-Z]+\d*)(?:_(\d{2,4}))?\.csv", filename)
    league, season_str = ("misc", None)
    
    if match:
        league = match.group(1)
        season_str = match.group(2)
        
    season_key = 9999 # Varsayılan: En güncel sezon (E0.csv gibi) en sona gider.
    
    if season_str:
        if len(season_str) == 2:
            season_int = int(season_str)
            
            # 2 haneli yıl mantığı: 93'ten büyük veya eşit olanlar 19XX, küçük olanlar 20XX.
            if season_int >= 93:
                prefix = '19'
            else:
                prefix = '20'
            season_key = int(prefix + season_str)
            
        elif len(season_str) == 4:
            season_key = int(season_str)

    # Eğer lig kısaltması Regex desenine uymazsa (örn: example.csv) "misc" olarak kalır.
    return league, season_key, filename

def get_all_csv_links():
    """Tüm CSV linklerini çeker, lig ve sezona göre kronolojik olarak sıralar."""
    raw_links = []
    # pages_to_visit'e başlangıç sayfasını ekle
    pages_to_visit = set(DATA_PAGES) 
    visited_pages = set()
    
    # Tüm alt sayfalara (englandm.php, italym.php vb.) erişmek için döngü
    while pages_to_visit:
        page = pages_to_visit.pop()
        url = urljoin(BASE_URL, page)
        
        # URL'yi sadece yol olarak kaydet
        page_key = url.replace(BASE_URL, '').split('?')[0].split('#')[0]

        if page_key in visited_pages:
            continue
        visited_pages.add(page_key)
        
        print(f"Checking page: {url}")
        
        for attempt in range(MAX_RETRIES):
            try:
                # Sayfayı çek
                response = requests.get(url, timeout=30, headers=HEADERS)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, "html.parser")
                
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    full_link = urljoin(url, href)
                    
                    if full_link.lower().endswith(".csv"):
                        # CSV linklerini indirme listesine ekle
                        raw_links.append(full_link)
                    
                    elif full_link.lower().endswith(".php"):
                        # PHP ile biten alt sayfalara git (englandm.php, italym.php, vb.)
                        relative_link = full_link.replace(BASE_URL, '').split('?')[0].split('#')[0]
                        if relative_link not in visited_pages:
                            pages_to_visit.add(relative_link)
                
                break # Başarılı olduysa deneme döngüsünden çık
                
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
        # Linki analiz et (lig ve sezon anahtarını çıkar)
        league, season_key, _ = parse_league_and_season(url)
        sortable_links.append((league, season_key, url))
        
    # KRONOLOJİK SIRALAMA: 1. Lig kısaltmasına göre, 2. Sezon yılına göre (eskiden yeniye)
    sortable_links.sort(key=lambda x: (x[0], x[1]))
    
    sorted_links = [url for league, season_key, url in sortable_links]
    
    return sorted_links

def download_csv(csv_url):
    """CSV dosyasını indirir, lig klasörüne kaydeder ve değişiklik olup olmadığını kontrol eder."""
    # Orijinal adı korur (örn: E0_93.csv)
    league, season_key, filename = parse_league_and_season(csv_url) 
    
    # LİG KLASÖRÜ OLUŞTURMA: data/E0, data/SP1, vb.
    league_dir = os.path.join(DATA_DIR, league)
    os.makedirs(league_dir, exist_ok=True) 
    
    filepath = os.path.join(league_dir, filename)

    for attempt in range(MAX_RETRIES):
        try:
            # İndirme işlemi
            response = requests.get(csv_url, timeout=60, headers=HEADERS)
            response.raise_for_status() # HTTP hata kodu varsa istisna fırlatır
            new_content = response.content

            # Dosya kontrolü (zaten varsa ve içerik aynıysa indirme yapmaz)
            if os.path.exists(filepath):
                with open(filepath, "rb") as f:
                    old_content = f.read()
                if old_content == new_content:
                    print(f"[UNCHANGED] {filepath}")
                    return

            # Dosyayı kaydetme
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
    
    # Tüm linkleri reküresif olarak çeker ve kronolojik sıraya koyar.
    csv_links = get_all_csv_links() 
    print(f"Found {len(csv_links)} unique CSV files to download.")
    print("Starting download in chronological order (Oldest Season -> Newest Season, by League)...")
    
    # İndirme döngüsü
    for csv_url in csv_links: 
        download_csv(csv_url)
        
    print("CSV fetch process completed.")

if __name__ == "__main__":
    main()
