# scripts/scraper.py
"""
NYC TLC Taxi Data Automated Scraper
Downloads Yellow/Green taxi parquet files from TLC website for specified years.
Handles missing month detection and historical data for December 2025 imputation.

"""
import requests
from bs4 import BeautifulSoup
import os
import time
from pathlib import Path
from urllib.parse import urljoin
import sys

# Configuration

PROJECT_ROOT = Path(r"C:\Users\black\OneDrive - FAST National University\nyc_congestion")
DATA_FOLDER = PROJECT_ROOT / "data" / "raw"
TLC_URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

# Helper Functions
def print_header(text):
    """Print a formatted header"""
    print("\n" + "="*70)
    print(f" {text}")
    print("="*70)

def print_status(message, status="INFO"):
    """Print status messages with timestamp"""
    timestamp = time.strftime("%H:%M:%S", time.localtime())
    if status == "SUCCESS":
        print(f"[{timestamp}] ✓ {message}")
    elif status == "WARNING":
        print(f"[{timestamp}] ⚠ {message}")
    elif status == "ERROR":
        print(f"[{timestamp}] ✗ {message}")
    else:
        print(f"[{timestamp}] → {message}")

def print_progress(current, total, file_name):
    """Show download progress"""
    percent = (current / total) * 100
    bar_length = 30
    filled = int(bar_length * current // total)
    bar = '█' * filled + '░' * (bar_length - filled)
    print(f"[{bar}] {percent:.1f}% - {file_name}", end='\r')
    if current == total:
        print()

# Main Scraping Functions

def get_page_content():
    """Fetch and parse the TLC data webpage"""
    print_status(f"Connecting to TLC website...")
    
    try:
        response = requests.get(TLC_URL, timeout=15)
        response.raise_for_status()
        print_status("Website loaded successfully", "SUCCESS")
        return BeautifulSoup(response.content, 'html.parser')
    except requests.RequestException as e:
        print_status(f"Failed to connect: {e}", "ERROR")
        return None

def extract_parquet_links(soup):
    """Find all parquet file links on the page"""
    print_status("Searching for taxi data files...")
    
    files_found = []
    for link in soup.find_all('a', href=True):
        href = link['href']
        if '.parquet' in href.lower():
            # Convert to full URL if needed
            if not href.startswith('http'):
                href = urljoin('https://www.nyc.gov', href)
            
            files_found.append(href)
    
    print_status(f"Found {len(files_found)} total data files", "SUCCESS")
    return files_found

def filter_year_files(all_links, target_year):
    """Filter for specific year Yellow and Green taxi files only"""
    print_status(f"Filtering for {target_year} Yellow and Green taxi data...")
    
    target_files = []
    for link in all_links:
        filename = link.split('/')[-1].lower()
        
        # Check if it's target year data
        if target_year not in filename:
            continue
        
        # Check if it's Yellow or Green taxi
        if 'yellow' in filename:
            taxi_type = 'yellow'
        elif 'green' in filename:
            taxi_type = 'green'
        else:
            continue
        
        # Extract month from filename (format: yellow_tripdata_2025-01.parquet)
        try:
            month_part = filename.split(target_year + '-')[1]
            month = month_part.split('.')[0]
        except:
            month = 'unknown'
        
        target_files.append({
            'url': link,
            'filename': filename,
            'taxi_type': taxi_type,
            'month': month,
            'year': target_year
        })
    
    print_status(f"Found {len(target_files)} relevant files for {target_year}", "SUCCESS")
    return target_files

def setup_folders(target_year="2025"):
    """Create necessary folder structure for specific year"""
    print_status(f"Setting up project folders for {target_year}...")
    
    # Always create base folders
    base_folders = [
        DATA_FOLDER / "yellow",
        DATA_FOLDER / "green", 
        DATA_FOLDER / "historical"
    ]
    
    # Create year-specific folders for 2024
    if target_year == "2024":
        base_folders.extend([
            DATA_FOLDER / "yellow_2024",
            DATA_FOLDER / "green_2024"
        ])
    
    for folder in base_folders:
        folder.mkdir(parents=True, exist_ok=True)
        print_status(f"Ensured folder exists: {folder.relative_to(PROJECT_ROOT)}", "SUCCESS")
    
    return base_folders

def download_parquet_file(file_info, target_year="2025"):
    """Download a single parquet file with year-aware paths"""
    taxi_type = file_info['taxi_type']
    month = file_info['month']
    file_year = file_info['year']
    
    # Determine save path based on year
    if file_year == target_year:
        if target_year == "2025":
            # 2025 data goes to main folders
            save_path = DATA_FOLDER / taxi_type / f"{target_year}_{month}.parquet"
        else:
            # 2024 data goes to year-specific folders
            save_path = DATA_FOLDER / f"{taxi_type}_{target_year}" / f"{target_year}_{month}.parquet"
    elif file_year in ['2023', '2024'] and target_year == "2025":
        # Historical data for 2025 imputation only
        save_path = DATA_FOLDER / "historical" / file_info['filename']
    else:
        # Other cases - use appropriate folder
        folder_name = taxi_type if file_year == "2025" else f"{taxi_type}_{file_year}"
        save_path = DATA_FOLDER / folder_name / file_info['filename']
    
    # Create folder if it doesn't exist
    save_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Skip if already downloaded
    if save_path.exists() and save_path.stat().st_size > 1000:
        print_status(f"Skipping (already exists): {save_path.name}", "INFO")
        return True
    
    print_status(f"Downloading: {save_path.name}")
    
    try:
        response = requests.get(file_info['url'], stream=True, timeout=60)
        response.raise_for_status()
        
        file_size = int(response.headers.get('content-length', 0))
        
        # Download with progress
        downloaded = 0
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if file_size > 0:
                        print_progress(downloaded, file_size, save_path.name)
        
        # Verify download
        if save_path.exists():
            actual_size = save_path.stat().st_size
            size_mb = actual_size / (1024 * 1024)
            print_status(f"Downloaded: {save_path.name} ({size_mb:.1f} MB)", "SUCCESS")
            return True
        else:
            print_status(f"Failed: File not saved", "ERROR")
            return False
            
    except Exception as e:
        print_status(f"Download failed: {str(e)}", "ERROR")
        return False

def check_data_completeness(files, target_year):
    """Check if we have all months of data for target year"""
    print_status(f"Checking data completeness for {target_year}...")
    
    # Filter only files for the target year
    year_files = [f for f in files if f['year'] == target_year]
    
    if not year_files:
        print_status(f"No files found for {target_year}", "WARNING")
        return [f"{m:02d}" for m in range(1, 13)]  # All months missing
    
    months_found = sorted(set([f['month'] for f in year_files]))
    
    print_status(f"Months available: {', '.join(months_found)}")
    
    # Check for missing months
    all_months = [f"{m:02d}" for m in range(1, 13)]
    missing_months = [m for m in all_months if m not in months_found]
    
    if missing_months:
        print_status(f"Missing months: {', '.join(missing_months)}", "WARNING")
        
        if '12' in missing_months and target_year == "2025":
            print_status("December 2025 is not available yet", "WARNING")
            print_status("Will use historical data for imputation", "INFO")
        elif '12' in missing_months:
            print_status(f"December {target_year} is missing", "WARNING")
    
    return missing_months

def download_historical_for_imputation(missing_months, target_year="2025"):
    """Download historical data if December is missing (2025 only)"""
    if '12' not in missing_months or target_year != "2025":
        return
    
    print_header("HISTORICAL DATA FOR IMPUTATION")
    print_status("December 2025 data is missing")
    print_status("Downloading December 2023 and 2024 for imputation...")
    
    # Files needed for imputation (30% 2023, 70% 2024)
    needed_files = [
        {'year': '2023', 'month': '12', 'type': 'yellow'},
        {'year': '2023', 'month': '12', 'type': 'green'},
        {'year': '2024', 'month': '12', 'type': 'yellow'},
        {'year': '2024', 'month': '12', 'type': 'green'}
    ]
    
    print_status("Getting list of all available historical files...")
    
    # Get all historical file links
    soup = get_page_content()
    if not soup:
        return
    
    all_links = extract_parquet_links(soup)
    
    # Find and download needed files
    downloaded_count = 0
    for needed in needed_files:
        target_name = f"{needed['type']}_tripdata_{needed['year']}-{needed['month']}.parquet"
        
        # Find the file link
        found_link = None
        for link in all_links:
            if target_name in link.lower():
                found_link = link
                break
        
        if found_link:
            file_info = {
                'url': found_link,
                'filename': target_name,
                'taxi_type': needed['type'],
                'month': needed['month'],
                'year': needed['year']
            }
            
            if download_parquet_file(file_info, target_year):
                downloaded_count += 1
        else:
            print_status(f"Not found: {target_name}", "WARNING")
    
    print_status(f"Downloaded {downloaded_count} historical files", "SUCCESS")

def display_summary(files, missing_months, target_year):
    """Display a clean summary of the scraping operation"""
    print_header(f"DOWNLOAD SUMMARY - {target_year}")
    
    # Count by taxi type for target year
    year_files = [f for f in files if f['year'] == target_year]
    yellow_count = sum(1 for f in year_files if f['taxi_type'] == 'yellow')
    green_count = sum(1 for f in year_files if f['taxi_type'] == 'green')
    
    print(f"Project Location: {PROJECT_ROOT}")
    print(f"Target Year: {target_year}")
    print(f"Total Files Downloaded: {len(year_files)}")
    print(f"  • Yellow Taxi: {yellow_count} files")
    print(f"  • Green Taxi: {green_count} files")
    
    if missing_months:
        print(f"\nMissing Months: {', '.join(missing_months)}")
        
        if '12' in missing_months and target_year == "2025":
            print("\nImputation Plan:")
            print("  • Will use December 2023 data (30% weight)")
            print("  • Will use December 2024 data (70% weight)")
            print("  • Combined to estimate December 2025")
    else:
        print("\n✓ All months available!")
    
    # Calculate total size for target year
    total_size_mb = 0
    if target_year == "2025":
        # Look in main yellow/green folders
        for taxi_type in ['yellow', 'green']:
            folder = DATA_FOLDER / taxi_type
            if folder.exists():
                for file in folder.glob(f"*.parquet"):
                    total_size_mb += file.stat().st_size / (1024 * 1024)
    else:
        # Look in year-specific folders for other years
        for taxi_type in ['yellow', 'green']:
            folder = DATA_FOLDER / f"{taxi_type}_{target_year}"
            if folder.exists():
                for file in folder.glob(f"*.parquet"):
                    total_size_mb += file.stat().st_size / (1024 * 1024)
    
    print(f"\nTotal Data Size: {total_size_mb:.1f} MB")
    print("\n" + "="*70)

# ============================================================================
# Main Execution
# ============================================================================
def main(target_year="2025"):
    """Main function to run the scraper for specific year"""
    print_header(f"NYC TLC DATA SCRAPER - {target_year}")
    print(f"Starting automated data collection for {target_year}...")
    
    # Step 1: Setup
    setup_folders(target_year)
    
    # Step 2: Get webpage content
    soup = get_page_content()
    if not soup:
        print_status("Cannot continue without website access", "ERROR")
        return []
    
    # Step 3: Extract and filter files
    all_links = extract_parquet_links(soup)
    target_files = filter_year_files(all_links, target_year)
    
    if not target_files:
        print_status(f"No {target_year} files found online. You may have manual data.", "WARNING")
        return []
    
    # Step 4: Download files
    print_header(f"DOWNLOADING {target_year} DATA")
    downloaded_files = []
    
    for i, file_info in enumerate(target_files, 1):
        print_status(f"File {i}/{len(target_files)}: {file_info['filename']}")
        
        if download_parquet_file(file_info, target_year):
            downloaded_files.append(file_info)
        
        # Small delay to be polite to server
        time.sleep(0.5)
    
    # Step 5: Check completeness
    missing = check_data_completeness(downloaded_files, target_year)
    
    # Step 6: Download historical if needed (only for 2025)
    if '12' in missing and target_year == "2025":
        download_historical_for_imputation(missing, target_year)
    
    # Step 7: Show summary
    display_summary(downloaded_files, missing, target_year)
    
    print_status(f"Data collection for {target_year} complete!", "SUCCESS")
    print_status(f"Data saved to: {DATA_FOLDER}")
    
    return downloaded_files

if __name__ == "__main__":
    start_time = time.time()
    
    try:
        # Support command line argument for year
        if len(sys.argv) > 1 and sys.argv[1] in ["2024", "2025"]:
            target_year = sys.argv[1]
        else:
            target_year = "2025"  # Default for backward compatibility
        
        main(target_year)
    except KeyboardInterrupt:
        print("\n\nScraping interrupted by user")
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()
    
    elapsed = time.time() - start_time
    print(f"\nTotal time: {elapsed:.1f} seconds")