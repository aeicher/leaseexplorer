import requests
import json
from concurrent.futures import ThreadPoolExecutor
import undetected_chromedriver as uc
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
import time
import re
from selenium.webdriver.common.by import By
from datetime import datetime
import os
import argparse
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Try to import beepy, set availability flag
try:
    import beepy
    BEEPY_AVAILABLE = True
except ImportError:
    BEEPY_AVAILABLE = False

def normalize_unit(unit_str):
    """Normalize unit numbers for deduplication (e.g., '3A', '3a', '3-A' -> '3A')"""
    if not unit_str or unit_str is None:
        return ''
    
    # Convert to string and strip whitespace, handle None values explicitly
    try:
        unit_str = str(unit_str).strip().upper()
    except (AttributeError, TypeError):
        return ''
    
    # Remove common separators and normalize
    unit_str = re.sub(r'[-_\s]+', '', unit_str)
    
    return unit_str

def filter_delisted_listings(listings):
    """Filter out listings with DELISTED status to prevent saving them"""
    if not listings:
        return listings
    
    filtered_listings = []
    delisted_count = 0
    
    for listing in listings:
        status = listing.get('status', '').upper() if listing.get('status') else ''
        if status == 'DELISTED':
            delisted_count += 1
            continue
        filtered_listings.append(listing)
    
    if delisted_count > 0:
        print(f"🚫 Filtered out {delisted_count} delisted listings (not saving)")
    
    return filtered_listings

def write_status(status, progress=None, message=None):
    """Write scraper status to status file for server monitoring"""
    try:
        status_data = {
            'status': status,
            'timestamp': datetime.now().isoformat(),
        }
        if progress is not None:
            status_data['progress'] = progress
        if message is not None:
            status_data['message'] = message
        
        # Use atomic write to prevent JSON corruption during concurrent access
        temp_file = 'scraper_status.json.tmp'
        with open(temp_file, 'w') as f:
            json.dump(status_data, f)
        
        # Atomic move to replace the original file
        if os.name == 'nt':  # Windows
            if os.path.exists('scraper_status.json'):
                os.remove('scraper_status.json')
        os.rename(temp_file, 'scraper_status.json')
        
    except Exception as e:
        # Fallback: try direct write if atomic write fails
        try:
            with open('scraper_status.json', 'w') as f:
                json.dump(status_data, f)
        except:
            pass  # Silent fail to prevent scraper crashes

def check_stop_signal():
    """Check if a stop signal has been sent via the web interface"""
    return os.path.exists('scraper_stop_signal.txt')

class RentalCollector:
    def __init__(self):
        options = uc.ChromeOptions()
        options.add_argument('--no-sandbox')
        options.add_argument('--window-size=1920,1080')
        
        # Initialize listings attribute
        self.listings = []
        
        # Initialize undetected-chromedriver
        self.driver = uc.Chrome(
            options=options,
            driver_executable_path=ChromeDriverManager().install()
        )
        self.wait = WebDriverWait(self.driver, 10)
        
        # Initialize session
        try:
            print("Getting homepage...")
            self.driver.get('https://streeteasy.com')
            time.sleep(1)
            
            self.api_url = 'https://api-v6.streeteasy.com/'
            self.session = requests.Session()
            self.headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'Origin': 'https://streeteasy.com',
                'Referer': 'https://streeteasy.com/',
                'X-Requested-With': 'XMLHttpRequest'
            }
            self.session.headers.update(self.headers)
            
            # Get cookies from Selenium and add to requests session
            selenium_cookies = self.driver.get_cookies()
            for cookie in selenium_cookies:
                self.session.cookies.set(cookie['name'], cookie['value'])
            
        except Exception as e:
            print(f"Warning: Error during initialization: {e}")
        
        try:
            with open('building_info.json', 'r') as f:
                self.building_info = json.load(f)
        except FileNotFoundError:
            self.building_info = {}
    
    def save_listings_to_json(self, listings, filename=None):
        """Save listings to JSON file with timestamp and metadata"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"rentals_{timestamp}.json"
        
        # Filter out delisted listings before saving
        listings = filter_delisted_listings(listings)
        
        # Create data structure with metadata
        data_to_save = {
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "total_listings": len(listings),
                "collection_method": "api",
                "area": getattr(self, 'current_area', 'unknown')
            },
            "listings": listings
        }
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data_to_save, f, indent=2, ensure_ascii=False)
            
            print(f"✅ Successfully saved {len(listings)} listings to {filename}")
            
            # Cleanup: Keep only the 5 most recent timestamped files
            self._cleanup_old_files()
            
            return filename
            
        except Exception as e:
            print(f"❌ Error saving listings to JSON: {e}")
            return None

    def save_progress_backup(self, listings, area=None):
        """Save progress backup that can be recovered if scraper is interrupted"""
        try:
            # Filter out delisted listings before saving backup
            listings = filter_delisted_listings(listings)
            
            data_to_save = {
                "metadata": {
                    "timestamp": datetime.now().isoformat(),
                    "total_listings": len(listings),
                    "collection_method": "api",
                    "area": area,
                    "is_backup": True
                },
                "listings": listings
            }
            
            with open('rentals_backup.json', 'w', encoding='utf-8') as f:
                json.dump(data_to_save, f, indent=2, ensure_ascii=False)
            
            # Also immediately update the latest file
            with open('rentals_latest.json', 'w', encoding='utf-8') as f:
                json.dump(data_to_save, f, indent=2, ensure_ascii=False)
                
            print(f"💾 Progress backup saved: {len(listings)} listings")
            
        except Exception as e:
            print(f"⚠️  Could not save progress backup: {e}")
    
    def _cleanup_old_files(self):
        """Keep only the 5 most recent timestamped rental files"""
        try:
            # Find all timestamped rental files
            json_files = [f for f in os.listdir('.') 
                         if f.startswith('rentals_') and f.endswith('.json') 
                         and f != 'rentals_latest.json' and '_' in f]
            
            if len(json_files) <= 5:
                return  # Keep all if 5 or fewer
            
            # Sort by filename (which includes timestamp)
            json_files.sort(reverse=True)
            
            # Delete the oldest files beyond the 5 most recent
            files_to_delete = json_files[5:]
            for file_to_delete in files_to_delete:
                try:
                    os.remove(file_to_delete)
                    print(f"🗑️  Cleaned up old file: {file_to_delete}")
                except Exception as e:
                    print(f"⚠️  Could not delete {file_to_delete}: {e}")
                    
        except Exception as e:
            print(f"⚠️  Error during cleanup: {e}")
    
    def load_previous_listings(self, filename):
        """Load previously saved listings from JSON file"""
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, dict) and 'listings' in data:
                    return data['listings'], data.get('metadata', {})
                else:
                    # Handle old format (just array of listings)
                    return data, {}
        except FileNotFoundError:
            print(f"No previous file found: {filename}")
            return [], {}
        except Exception as e:
            print(f"Error loading previous listings: {e}")
            return [], {}

    def get_building_ids_from_area(self, area):
        """Get building IDs from StreetEasy area page with progress tracking"""
        building_ids = []
        self.building_info = {}
        
        try:
            # Convert area name to URL slug format
            area_slug = area.lower().replace(' ', '-').replace('&', 'and')
            base_url = f"https://streeteasy.com/buildings/{area_slug}"
            
            # First, discover total number of pages
            print("🔍 Discovering total number of pages...")
            self.driver.get(base_url)
            time.sleep(2)
            
            # Check if we're blocked or redirected
            current_url = self.driver.current_url
            page_title = self.driver.title.lower()
            page_source = self.driver.page_source.lower()
            
            # Check for various blocking indicators
            # if ("access to this page has been denied" in page_source or 
            #     "blocked" in page_title or 
            #     "captcha" in page_title or
            #     current_url != base_url or
            #     f"buildings/{area_slug}" not in current_url):
            #     print(f"🚫 Unable to access West Village buildings page. URL: {current_url}")
            #     print(f"   Page title: {self.driver.title}")
            #     if "access to this page has been denied" in page_source:
            #         print("   Reason: Access denied (bot detection)")
            #     elif current_url != base_url:
            #         print(f"   Reason: Redirected from {base_url} to {current_url}")
            #     else:
            #         print("   Reason: Page blocked or inaccessible")
            #     return building_ids  # Return empty list instead of scraping wrong area
            
            # Handle any captcha that appears during discovery
            if "Press & Hold to confirm" in self.driver.page_source:
                print("\n\n🚨 CAPTCHA DETECTED DURING PAGE DISCOVERY! 🚨\n\n")
                if BEEPY_AVAILABLE:
                    for _ in range(5):
                        beepy.beep(sound=1)
                        time.sleep(0.5)
                else:
                    for _ in range(5):
                        print('\a', end='', flush=True)
                        time.sleep(0.5)
                input("Please solve the captcha manually and press Enter to continue...")
                self.driver.refresh()
                time.sleep(1)
            
            # Find total pages by looking at pagination
            total_pages = 1
            try:
                # Look for pagination info - try multiple selectors
                pagination_selectors = [
                    ".pagination li:nth-last-child(2) a",  # Second to last pagination item
                    ".pagination .page-numbers:not(.next):not(.prev):last-of-type",
                    ".pagination a[href*='page=']:last-of-type"
                ]
                
                for selector in pagination_selectors:
                    try:
                        last_page_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        if last_page_elements:
                            last_page_text = last_page_elements[-1].text.strip()
                            if last_page_text.isdigit():
                                total_pages = int(last_page_text)
                                break
                            # Extract page number from href if text isn't a number
                            href = last_page_elements[-1].get_attribute('href')
                            if href and 'page=' in href:
                                page_match = re.search(r'page=(\d+)', href)
                                if page_match:
                                    total_pages = int(page_match.group(1))
                                    break
                    except Exception:
                        continue
                
            except Exception as e:
                # Default to single page if unable to determine
                total_pages = 1

            print(f"📄 Total pages to scrape: {total_pages}")
            
            # TESTING LIMIT - Only scrape first 5 pages for debugging
            # if total_pages > 5:
            #     print(f"🧪 TESTING MODE: Limiting to 5 pages instead of {total_pages} for faster testing")
            #     total_pages = 5
                    
            # Write initial status
            write_status('running', 
                        {'pages': {'current': 0, 'total': total_pages, 'phase': 'scraping_buildings'}}, 
                        f"Starting to scrape {total_pages} pages of buildings")
            
            # Create a stop handler to handle Ctrl+C
            import signal
            
            def signal_handler(sig, frame):
                print('\n🔄 Gracefully stopping scraper...')
                # Set stop flag
                with open('scraper_stop_signal.txt', 'w') as f:
                    f.write('stop')
                print('Stop signal sent. The scraper will finish the current page and then stop.')
                
            signal.signal(signal.SIGINT, signal_handler)
            
            # Go through each page
            page = 1
            # max_debug_pages = 25  # DEBUG: Limit to 25 pages max
            
            while page <= total_pages:
                # DEBUG: Stop after 25 pages for debugging
                # if page > max_debug_pages:
                #     print(f"🧪 DEBUG: Hit max debug page limit of {max_debug_pages}. Stopping early.")
                #     break
                
                # Check if user requested stop
                if getattr(self, 'stop_requested', False) or check_stop_signal():
                    print(f"🔄 Scraping stopped by user after page {page-1}. Collected {len(building_ids)} buildings so far.")
                    if check_stop_signal():
                        # Clean up the stop signal file
                        try:
                            os.remove('scraper_stop_signal.txt')
                        except:
                            pass
                    break
                    
                try:
                    url = f"{base_url}?page={page}" if page > 1 else base_url
                    self.driver.get(url)
                    time.sleep(1)
                    
                    # Check for captcha on each page
                    if "Press & Hold to confirm" in self.driver.page_source:
                        print(f"\n\n🚨 CAPTCHA DETECTED ON PAGE {page}! 🚨\n\n")
                        if BEEPY_AVAILABLE:
                            for _ in range(5):
                                beepy.beep(sound=1)
                                time.sleep(0.5)
                        else:
                            for _ in range(5):
                                print('\a', end='', flush=True)
                                time.sleep(0.5)
                        input("Please solve the captcha manually and press Enter to continue...")
                        self.driver.refresh()
                        time.sleep(1)
                    
                    # Find building links from actual building listings (avoid navigation/menu links)
                    # Use semantic structure instead of hardcoded hash-based class names
                    
                    # Method 1: Target building cards with semantic class patterns
                    building_cards = self.driver.find_elements(By.CSS_SELECTOR, 
                        ".item.building, [class*='building-card'], [class*='property-card'], [class*='BuildingCard']")
                    building_links = []
                    for card in building_cards:
                        links = card.find_elements(By.CSS_SELECTOR, "a[href*='/building/']")
                        building_links.extend(links)
                    
                    # Method 2: If no building cards found, filter links by context
                    if not building_links:
                        all_links = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='/building/']")
                        building_links = []
                        for link in all_links:
                            try:
                                # Check if link is in a navigation context (avoid menu/nav items)
                                parent = link.find_element(By.XPATH, "..")
                                grandparent = parent.find_element(By.XPATH, "..")
                                
                                parent_class = (parent.get_attribute('class') or '').lower()
                                grandparent_class = (grandparent.get_attribute('class') or '').lower()
                                
                                # Skip if it looks like navigation/menu structure
                                nav_indicators = ['nav', 'menu', 'header', 'listitem', 'list_list']
                                is_nav_link = any(indicator in parent_class or indicator in grandparent_class 
                                                for indicator in nav_indicators)
                                
                                # Include if it looks like building content
                                building_indicators = ['item', 'building', 'property', 'card', 'photo', 'details']
                                is_building_link = any(indicator in parent_class or indicator in grandparent_class 
                                                     for indicator in building_indicators)
                                
                                if not is_nav_link or is_building_link:
                                    building_links.append(link)
                                    
                            except:
                                building_links.append(link)  # Include if we can't determine context
                    
                    print(f"🏢 Found {len(building_links)} building links on page {page} (Total collected: {len(building_ids)})")
                    
                    # Update progress status
                    write_status('running', 
                                {'pages': {'current': page, 'total': total_pages, 'phase': 'scraping_buildings'}}, 
                                f"Scraping page {page}/{total_pages} - found {len(building_ids)} buildings so far")
                    
                    for link_element in building_links:
                        try:
                            href = link_element.get_attribute("href")
                            
                            # Extract the slug from the URL
                            match = re.search(r'/building/([^/]+)', href)
                            if match:
                                slug = match.group(1)
                                
                                # Skip duplicates (in case same building appears multiple times on page)
                                if slug in building_ids:
                                    continue
                                    
                                building_ids.append(slug)
                                
                                # Try to get the address text from the link or nearby elements
                                address = f"Building {slug}"  # default
                                try:
                                    # First try the link text itself
                                    link_text = link_element.text.strip()
                                    if link_text and len(link_text) > 3:  # Avoid empty or very short text
                                        address = link_text
                                    else:
                                        # Try to find address in parent or sibling elements
                                        parent = link_element.find_element(By.XPATH, "..")
                                        address_selectors = ["h3", ".address", ".title", "[class*='address']", "[class*='Address']"]
                                        for selector in address_selectors:
                                            try:
                                                address_element = parent.find_element(By.CSS_SELECTOR, selector)
                                                text = address_element.text.strip()
                                                if text and len(text) > 3:
                                                    address = text
                                                    break
                                            except:
                                                continue
                                except:
                                    pass
                                
                                # Store additional info
                                self.building_info[slug] = {
                                    'href': href,
                                    'address': address
                                }
                                # print(f"Added building: {slug} - {address}")
                        except Exception as e:
                            continue
                    
                    # Move to next page
                    page += 1
                        
                except Exception as e:
                    break
            
            if getattr(self, 'stop_requested', False) or check_stop_signal():
                print(f"🔄 Scraping stopped by user after page {page-1}. Collected {len(building_ids)} buildings so far.")
                if check_stop_signal():
                    # Clean up the stop signal file
                    try:
                        os.remove('scraper_stop_signal.txt')
                    except:
                        pass
            
            
        except Exception as e:
            print(f"Error discovering buildings for {area}: {e}")
            # Ensure variables are properly initialized in case of exception
            if 'total_pages' not in locals():
                total_pages = 1
            if building_ids is None:
                building_ids = []
        
        # Defensive check to ensure building_ids is never None
        if building_ids is None:
            building_ids = []
            
        print(f"✅ Building discovery complete! Found {len(building_ids)} total buildings to process")
        
        # Update status to show building discovery is complete
        write_status('running', 
                    {'pages': {'current': total_pages, 'total': total_pages, 'phase': 'completed_discovery'}}, 
                    f"Building discovery complete - found {len(building_ids)} buildings")
        
        return building_ids

    def get_listings_api(
        self,
        min_price: int,
        max_price: int,
        bedrooms_filter: str,
        laundry_filter: str,
        pets_filter: str,
        outdoor_filter: str,
        by_owner_filter: str,
        days_on_market_filter: str,
        offmarket_month_start: int,
        offmarket_month_end: int,
        area: str,
        workers: int = 8,
        cookies: dict = None,
        cookie_string: str = None,
        save_to_file: bool = True,
        output_filename: str = None
    ):
        # Store the area being scraped for use in data saving
        self.current_area = area
        """Get all rental listings using advanced GraphQL API queries with full building scraping"""
        building_ids = self.get_building_ids_from_area(area)
        if not building_ids:
            print("No buildings discovered – aborting API mode.")
            return None

        # Parse cookies if provided as string
        session_cookies = {}
        if cookie_string:
            for cookie in cookie_string.split(';'):
                if '=' in cookie:
                    key, value = cookie.strip().split('=', 1)
                    session_cookies[key] = value
        elif cookies:
            session_cookies = cookies
        else:
            # Use cookies from Selenium session
            selenium_cookies = self.driver.get_cookies()
            for cookie in selenium_cookies:
                session_cookies[cookie['name']] = cookie['value']
        
        # Cookie setup complete
        print(f"🍪 Using {len(session_cookies)} cookies for API requests")
        
        listings_out: list[dict] = []
        listings_out.clear()  # Ensure no accumulation from previous runs

        def _get_building_id_from_slug(slug: str) -> tuple:
            """Convert building slug to building ID and get building name and geo using GraphQL"""
            query = {
                "query": """
                query GetBuilding($slug: String!) {
                    buildingBySlug(slug: $slug) {
                        id
                        name
                        geoCenter { latitude longitude }
                        address { street city state zipCode }
                    }
                }
                """,
                "variables": {"slug": slug}
            }
            try:
                headers = {
                    'Content-Type': 'application/json',
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                    'Accept': 'application/json',
                    'Referer': 'https://streeteasy.com/',
                }
                
                response = requests.post(
                    'https://api-v6.streeteasy.com/',
                    json=query,
                    headers=headers,
                    cookies=session_cookies,
                    timeout=5  # Reduced timeout
                )
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if 'data' in data and data['data']['buildingBySlug']:
                        building = data['data']['buildingBySlug']
                        building_id = building['id']
                        geo_center = building.get('geoCenter')
                        # Construct display address from address fields
                        address = building.get('address', {})
                        if address:
                            address_parts = []
                            if address.get('street'):
                                address_parts.append(address['street'])
                            if address.get('city'):
                                address_parts.append(address['city'])
                            if address.get('state'):
                                address_parts.append(address['state'])
                            if address.get('zipCode'):
                                address_parts.append(address['zipCode'])
                            building_title = ', '.join(address_parts) if address_parts else building.get('name') or slug.replace('-', ' ').title()
                        else:
                            building_title = building.get('name') or slug.replace('-', ' ').title()
                        # Store geoCenter in building_info
                        if slug not in self.building_info:
                            self.building_info[slug] = {}
                        self.building_info[slug]['geoCenter'] = geo_center
                        
                        return building_id, building_title
                    else:
                        print(f"❌ No building found for slug {slug}")
                        return None, None
                else:
                    print(f"❌ HTTP {response.status_code} getting building ID for {slug}")
                    return None, None
            except Exception as e:
                print(f"❌ Error getting building ID for {slug}: {e}")
                return None, None

        def _fetch_history(slug: str):
            """Fetch building history via API - simplified and faster"""
            print(f"🔍 Processing {slug}...")
            building_id, building_title = _get_building_id_from_slug(slug)
            if not building_id:
                print(f"❌ No building ID for {slug}")
                return []
            
            # Simplified strategy: try only the most reliable queries
            approaches = [
                ("minimal_fields_query", _try_minimal_fields_query),
                ("full_query", _try_full_query),
            ]
            
            for approach_name, approach_func in approaches:
                try:
                    print(f"🔍 Trying {approach_name} for {slug}")
                    rentals = approach_func(slug, building_id, building_title)
                    if rentals is not None:
                        rentals_count = len(rentals) if rentals else 0
                        print(f"✅ {approach_name} worked for {slug}: {rentals_count} rentals")
                        return rentals
                except Exception as e:
                    print(f"❌ {approach_name} failed for {slug}: {e}")
                    continue
            
            print(f"❌ All approaches failed for {slug}")
            return []

        def _try_full_query(slug: str, building_id: str, building_title: str):
            """Try the full query for rentals (agent fields not available on RentalListingDigest)"""
            full_query = {
                "query": """
                query GetRentalsHistoryByBuildingId($buildingId: ID!) {
                    rentalsHistoryByBuildingId(id: $buildingId) {
                        id
                        legacy { id }
                        street
                        displayUnit
                        buildingId
                        availableAt
                        offMarketAt
                        bedroomCount
                        fullBathroomCount
                        halfBathroomCount
                        livingAreaSize
                        noFee
                        price
                        interestingPriceDelta
                        netEffectiveRent
                        leaseTermMonths
                        monthsFree
                        mediaAssetCount
                        status
                        furnished
                        slug
                        areaName
                        urlPath
                    }
                }
                """,
                "variables": {"buildingId": building_id},
            }
            
            return _execute_query_with_retry(full_query, slug, building_id, building_title, "full query")

        def _try_minimal_query(slug: str, building_id: str, building_title: str):
            """Try minimal query with retry logic"""
            minimal_query = {
                "query": """
                query GetRentalsHistoryByBuildingId($buildingId: ID!) {
                    rentalsHistoryByBuildingId(id: $buildingId) {
                        id
                        price
                        bedroomCount
                        fullBathroomCount
                        halfBathroomCount
                        livingAreaSize
                        offMarketAt
                        status
                        furnished
                        slug
                        areaName
                        urlPath
                        displayUnit
                    }
                }
                """,
                "variables": {"buildingId": building_id},
            }
            
            return _execute_query_with_retry(minimal_query, slug, building_id, building_title, "minimal query")

        def _try_long_timeout_query(slug: str, building_id: str, building_title: str):
            """Disabled - was too slow"""
            return None

        def _try_force_parse_query(slug: str, building_id: str, building_title: str):
            """Disabled - was too slow"""
            return None

        def _try_curl_style_query(slug: str, building_id: str, building_title: str):
            """Disabled - was too slow"""
            return None

        def _try_minimal_fields_query(slug: str, building_id: str, building_title: str):
            """Try query with minimal fields for rentals (agent fields not available on RentalListingDigest)"""
            minimal_fields_query = {
                "query": """
                query GetRentalsHistoryByBuildingId($buildingId: ID!) {
                    rentalsHistoryByBuildingId(id: $buildingId) {
                        id
                        legacy { id }
                        street
                        displayUnit
                        buildingId
                        availableAt
                        offMarketAt
                        bedroomCount
                        fullBathroomCount
                        halfBathroomCount
                        livingAreaSize
                        price
                        urlPath
                        status
                        slug
                        areaName
                    }
                }
                """,
                "variables": {"buildingId": building_id},
            }
            
            return _execute_query_with_retry(minimal_fields_query, slug, building_id, building_title, "minimal fields query")

        def _execute_query_with_retry(query, slug, building_id, building_title, query_name):
            """Execute GraphQL query with optimized retry logic"""
            max_retries = 2  # Reduced retries
            timeout = 15  # Fixed shorter timeout
            
            for attempt in range(max_retries):
                try:
                    headers = {
                        'Content-Type': 'application/json',
                        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                        'Accept': 'application/json',
                        'Referer': 'https://streeteasy.com/',
                    }
                    
                    response = requests.post(
                        'https://api-v6.streeteasy.com/',
                        json=query,
                        headers=headers,
                        cookies=session_cookies,
                        timeout=timeout
                    )
                    
                    if response.status_code == 429:  # Rate limited
                        wait_time = 1 + attempt  # Shorter backoff
                        time.sleep(wait_time)
                        continue
                    
                    if response.status_code != 200:
                        if attempt < max_retries - 1:
                            time.sleep(0.5)  # Brief pause before retry
                            continue
                        return None
                    
                    data = response.json()
                    
                    # Check for GraphQL errors
                    if 'errors' in data:
                        error_messages = [err.get('message', 'Unknown error') for err in data['errors']]
                        if attempt < max_retries - 1 and any('timeout' in msg.lower() for msg in error_messages):
                            time.sleep(1)  # Brief backoff for timeout errors
                            continue
                        return None
                    
                    if 'data' not in data or not data['data']:
                        return None
                    
                    rental_data = data['data'].get('rentalsHistoryByBuildingId', [])
                    if rental_data is None:
                        return []
                    
                    return _process_rentals(rental_data, slug, building_id, building_title, None)
                    
                except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
                    if attempt < max_retries - 1:
                        time.sleep(0.5)
                        continue
                except Exception:
                    if attempt < max_retries - 1:
                        time.sleep(0.5)
                        continue
                    break
            
            return None

        def _detect_owner_comprehensive(rental_id: str) -> tuple:
            """
            Comprehensive owner detection using multiple StreetEasy API fields
            Returns: (is_owner: bool, detection_method: str, confidence: int, agent_info: dict)
            """
            if not rental_id:
                return False, 'no_rental_id', 0, {}
                
            try:
                # Comprehensive query to get all owner-related fields
                owner_query = {
                    "query": """
                    query GetListingOwnerInfo($id: ID!) {
                        rentalByListingId(id: $id) {
                            id
                            legacy {
                                sourceGroupLabel
                            }
                            backOffice {
                                ownerPaysFee
                                collectYourOwnFee
                                exclusivity {
                                    type
                                }
                            }
                            description
                        }
                        getAgentsForRentalExpress(id: $id) {
                            id
                            name
                            email
                            phone
                            sourceGroupLabel
                        }
                    }
                    """,
                    "variables": {"id": str(rental_id)}
                }
                
                response = self.session.post(
                    'https://api-v6.streeteasy.com/',
                    json=owner_query,
                    timeout=10
                )
                
                if response.status_code != 200:
                    return False, 'api_error', 0, {}
                
                data = response.json()
                
                if 'errors' in data:
                    return False, 'api_error', 0, {}
                
                # Extract listing data
                listing_data = data.get('data', {}).get('rentalByListingId')
                agents_data = data.get('data', {}).get('getAgentsForRentalExpress', [])
                
                if not listing_data:
                    return False, 'no_listing_data', 0, {}
                
                # Prepare agent info for return
                agent_info = {}
                if agents_data:
                    # Take the first agent's info (usually the primary contact)
                    primary_agent = agents_data[0]
                    agent_info = {
                        'name': primary_agent.get('name', ''),
                        'email': primary_agent.get('email', ''),
                        'phone': primary_agent.get('phone', ''),
                        'source_group': primary_agent.get('sourceGroupLabel', ''),
                        'agent_count': len(agents_data)
                    }
                    
                    # If there are multiple agents, store them all
                    if len(agents_data) > 1:
                        agent_info['all_agents'] = [
                            {
                                'name': agent.get('name', ''),
                                'email': agent.get('email', ''),
                                'phone': agent.get('phone', ''),
                                'source_group': agent.get('sourceGroupLabel', '')
                            }
                            for agent in agents_data
                        ]
                
                # Extract key fields
                source_group_label = listing_data.get('legacy', {}).get('sourceGroupLabel', '')
                back_office = listing_data.get('backOffice', {})
                owner_pays_fee = back_office.get('ownerPaysFee')
                collect_your_own_fee = back_office.get('collectYourOwnFee')
                exclusivity_type = back_office.get('exclusivity', {}).get('type') if back_office.get('exclusivity') else None
                description = listing_data.get('description', '')
                
                # TIER 1: Corporate exclusions first (to override owner_pays_fee for known brokerages)
                
                # Check for corporate indicators in source group FIRST
                if source_group_label:
                    source_lower = source_group_label.lower()
                    corporate_indicators = [
                        'realty', 'real estate', 'broker', 'brokerage', 'group', 'inc', 'llc',
                        'associates', 'properties', 'team', 'agency', 'company', 'corp',
                        'management', 'residential', 'commercial', 'advisors', 'partners',
                        'consultants', 'solutions', 'corcoran', 'compass', 'elliman',
                        'sotheby', 'halstead', 'warburg', 'nest', 'bond', 'tabak',
                        'brown harris', 'miron', 'serhant', 'living new york'
                    ]
                    
                    if any(indicator in source_lower for indicator in corporate_indicators):
                        return False, 'corporate_source_group', 0, agent_info
                
                # Check agents for corporate indicators FIRST
                if agents_data:
                    for agent in agents_data:
                        agent_email = agent.get('email', '').lower()
                        agent_source_group = agent.get('sourceGroupLabel', '').lower()
                        
                        # Corporate email domains
                        corporate_domains = [
                            'corcoran.com', 'compass.com', 'elliman.com', 'sothebys.com',
                            'halstead.com', 'warburgrealty.com', 'nest.com', 'bondny.com',
                            'brownharris.com', 'miron-properties.com', 'serhant.com', 'livingny.com'
                        ]
                        
                        if any(domain in agent_email for domain in corporate_domains):
                            return False, 'corporate_agent_email', 0, agent_info
                        
                        # Corporate source group
                        if any(indicator in agent_source_group for indicator in corporate_indicators):
                            return False, 'corporate_agent_source', 0, agent_info

                # TIER 2: Explicit owner indicators (95% confidence) - AFTER corporate exclusions
                
                # Check if owner pays fee (strong indicator)
                if owner_pays_fee is True:
                    return True, 'owner_pays_fee', 95, agent_info
                
                # Check for explicit "owner" mentions in source group
                if source_group_label and 'owner' in source_group_label.lower():
                    return True, 'source_group_owner', 95, agent_info
                
                # Check for "For Rent By Owner" patterns
                if (source_group_label and 
                    ('for rent by owner' in source_group_label.lower() or 
                     'rent by owner' in source_group_label.lower() or
                     'by owner' in source_group_label.lower())):
                    return True, 'source_group_by_owner', 95, agent_info
                
                # Check description for owner indicators
                if description:
                    desc_lower = description.lower()
                    if any(phrase in desc_lower for phrase in [
                        'posted by owner', 'listed by owner', 'rent by owner', 
                        'for rent by owner', 'owner listing', 'direct from owner',
                        'no broker fee', 'no brokerage fee'
                    ]):
                        return True, 'description_owner_indicators', 90, agent_info
                
                # TIER 3: Agent-based detection (85% confidence)
                
                # Check agents for owner indicators
                if agents_data:
                    for agent in agents_data:
                        agent_name = agent.get('name', '').lower()
                        agent_email = agent.get('email', '').lower()
                        agent_source_group = agent.get('sourceGroupLabel', '').lower()
                        
                        # Explicit owner mentions in agent data
                        if ('owner' in agent_name or 'owner' in agent_email or 
                            'owner' in agent_source_group):
                            return True, 'agent_owner_explicit', 85, agent_info
                        
                        # Check for personal email domains with individual names
                        personal_domains = ['@gmail.', '@yahoo.', '@hotmail.', '@outlook.', '@aol.', '@me.', '@icloud.']
                        if any(domain in agent_email for domain in personal_domains):
                            # Check if this looks like an individual vs company
                            if agent_source_group:
                                # If source group is just the person's name or similar, likely owner
                                name_parts = agent_name.split()
                                if len(name_parts) >= 2 and agent_source_group in agent_name:
                                    return True, 'agent_personal_individual', 75, agent_info
                
                # TIER 4: Default - insufficient data
                return False, 'insufficient_data', 0, agent_info
                
            except Exception as e:
                return False, 'api_exception', 0, {}

        def _detect_owner_from_agent_api(rental_id: str) -> bool:
            """
            Detect owner listings using the getAgentsForRentalExpress API
            This is the working solution that avoids 403s and provides reliable owner detection
            """
            if not rental_id:
                return False
                
            try:
                agents_query = {
                    "query": """
                    query GetAgentsForRental($id: ID!) {
                        getAgentsForRentalExpress(id: $id) {
                            id
                            name
                            email
                            phone
                        }
                    }
                    """,
                    "variables": {"id": rental_id}
                }
                
                response = requests.post(
                    'https://api-v6.streeteasy.com/',
                    json=agents_query,
                    headers={
                        'Content-Type': 'application/json',
                        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                        'Accept': 'application/json',
                        'Referer': 'https://streeteasy.com/',
                    },
                    cookies=session_cookies,
                    timeout=15
                )
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if 'errors' in data:
                        # Don't log errors for every rental - too noisy
                        return False
                    
                    if 'data' in data and data['data']:
                        agents = data['data'].get('getAgentsForRentalExpress', [])
                        
                        for agent in agents:
                            agent_name = agent.get('name', '').lower()
                            agent_email = agent.get('email', '').lower()
                            
                            # Check for owner indicators
                            if 'owner' in agent_name:
                                print(f"🏠 Owner listing detected for {rental_id}: agent name '{agent.get('name')}'")
                                return True
                            elif 'owner' in agent_email:
                                print(f"🏠 Owner listing detected for {rental_id}: agent email contains 'owner'")
                                return True
                        
                        return False
                    else:
                        return False
                else:
                    # Don't log HTTP errors for every rental - too noisy
                    return False
                    
            except Exception as e:
                # Don't log errors for every rental - too noisy
                return False
        


        def _process_rentals(rentals, slug, building_id, building_title, building_year=None):
            """Process raw rental data and return formatted listings"""
            
            if not rentals:
                return []
            
            formatted_rentals = []
            
            for i, rental in enumerate(rentals):
                try:
                    # Format the rental data (apply filters later, after deduplication)
                    formatted_rental = {
                        'id': rental.get('id'),
                        'building_slug': slug,
                        'building_id': building_id,
                        'building_address': building_title,
                        'price': rental.get('price', 0),
                        'bedroomCount': rental.get('bedroomCount', 0),
                        'fullBathroomCount': rental.get('fullBathroomCount', 0),
                        'halfBathroomCount': rental.get('halfBathroomCount', 0),
                        'displayUnit': rental.get('displayUnit') or (rental.get('unit', {}).get('displayName') if rental.get('unit') else 'N/A'),
                        'sqft': rental.get('livingAreaSize'),
                        'offMarketAt': rental.get('offMarketAt'),
                        'onMarketAt': rental.get('onMarketAt'),
                        'availableAt': rental.get('availableAt'),
                        'status': rental.get('status'),
                        'isNoFee': rental.get('isNoFee', False),
                        'lastPrice': rental.get('lastPrice'),
                        'priceHistory': rental.get('priceHistory', []),
                        'monthlyMaintenanceFee': rental.get('monthlyMaintenanceFee'),
                        'petPolicy': rental.get('petPolicy'),
                        'isRentStabilized': rental.get('isRentStabilized', False),
                        'floorLevel': rental.get('floorLevel'),
                        'laundryInBuilding': rental.get('laundryInBuilding', False),
                        'privateOutdoorSpace': rental.get('privateOutdoorSpace', False),
                        'petFriendly': rental.get('petFriendly', False),
                        'furnished': rental.get('furnished', False),
                        'source_area': getattr(self, 'current_area', 'unknown'),  # Store which area this was scraped from
                        'building_year_built': building_year,  # Add building year for stabilization analysis
                        'urlPath': rental.get('urlPath'),  # Store URL path for owner detection
                    }
                    
                    # Add agent information (if available from full queries)
                    agent = rental.get('agent')
                    if agent:
                        formatted_rental['agentName'] = agent.get('name')
                        formatted_rental['agentEmail'] = agent.get('email')
                        formatted_rental['agentPhone'] = agent.get('phoneNumber')
                    
                    # Add owner contact info (if available from full queries)
                    owner_info = rental.get('ownerContactInfo')
                    if owner_info:
                        formatted_rental['ownerName'] = owner_info.get('name')
                        formatted_rental['ownerPhone'] = owner_info.get('phoneNumber')
                    
                    # Enhanced owner detection with multiple methods - IMPROVED APPROACH
                    is_owner, detection_method, confidence_score, agent_info = _detect_owner_comprehensive(rental.get('id'))
                    
                    formatted_rental['is_owner'] = is_owner
                    formatted_rental['owner_detection_method'] = detection_method
                    formatted_rental['owner_detection_confidence'] = confidence_score
                    
                    # Add agent/contact information
                    if agent_info:
                        formatted_rental['agent_name'] = agent_info.get('name', '')
                        formatted_rental['agent_email'] = agent_info.get('email', '')
                        formatted_rental['agent_phone'] = agent_info.get('phone', '')
                        formatted_rental['agent_source_group'] = agent_info.get('source_group', '')
                        formatted_rental['agent_count'] = agent_info.get('agent_count', 0)
                        
                        # If there are multiple agents, store them all
                        if agent_info.get('all_agents'):
                            formatted_rental['all_agents'] = agent_info['all_agents']

                    # Add a flag indicating if owner/agent info is present from API
                    formatted_rental['has_owner_agent_info'] = (agent is not None or owner_info is not None)
                    
                    # Add geo coordinates if available
                    geo_center = self.building_info.get(slug, {}).get('geoCenter')
                    if geo_center:
                        formatted_rental['latitude'] = geo_center.get('latitude')
                        formatted_rental['longitude'] = geo_center.get('longitude')
                    
                    formatted_rentals.append(formatted_rental)
                        
                except Exception as e:
                    continue
            
            return formatted_rentals



        # Process buildings in parallel using ThreadPoolExecutor
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import threading
        
        # Thread-safe results collection and statistics
        results_lock = threading.Lock()
        
        # Create a progress tracking system with detailed statistics
        processed_buildings = 0
        total_buildings = len(building_ids)
        success_count = 0
        empty_count = 0
        error_count = 0
        total_listings = 0
        
        print(f"🔄 Processing {total_buildings} buildings with {workers} parallel workers...")
        write_status('running', {'buildings': {'current': 0, 'total': total_buildings}}, 
                    f"Processing buildings: 0/{total_buildings}")
        
        with ThreadPoolExecutor(max_workers=workers) as executor:
            # Submit all tasks
            future_to_slug = {executor.submit(_fetch_history, slug): slug for slug in building_ids}
            
            # Process completed futures
            for future in as_completed(future_to_slug):
                slug = future_to_slug[future]
                try:
                    building_listings = future.result()
                    # Atomic update within lock to prevent race conditions
                    with results_lock:
                        processed_buildings += 1
                        if building_listings is not None and len(building_listings) > 0:
                            listings_out.extend(building_listings)
                            success_count += 1
                            total_listings += len(building_listings)
                            print(f"✅ {slug}: {len(building_listings)} listings ({processed_buildings}/{total_buildings} buildings complete)")
                        elif building_listings is not None:
                            # Valid response but no listings - building exists but no rental data
                            empty_count += 1
                            print(f"📊 {slug}: No listings found (valid building, no rental history) ({processed_buildings}/{total_buildings} buildings complete)")
                        else:
                            # Actual error - couldn't get building data
                            error_count += 1
                            print(f"⚠️ {slug}: Failed to retrieve building data ({processed_buildings}/{total_buildings} buildings complete)")
                        
                        # Update status with statistics
                        write_status('running', 
                                   {'buildings': {'current': processed_buildings, 'total': total_buildings, 'phase': 'processing_buildings'},
                                    'stats': {'success': success_count, 'empty': empty_count, 'errors': error_count, 'total_listings': total_listings}}, 
                                   f"Processing buildings: {processed_buildings}/{total_buildings} (✅{success_count} 📊{empty_count} ❌{error_count})")
                        
                        # Save backup every 100 buildings processed
                        if processed_buildings % 100 == 0:
                            print(f"💾 Progress backup at {processed_buildings} buildings: {total_listings} listings collected")
                            if listings_out:
                                self.save_progress_backup(listings_out, area)
                    
                except Exception as exc:
                    # Atomic error handling within lock
                    with results_lock:
                        processed_buildings += 1
                        error_count += 1
                        print(f"❌ {slug}: Exception - {type(exc).__name__}: {exc} ({processed_buildings}/{total_buildings} buildings complete)")
                
                # Check for stop signal
                if check_stop_signal():
                    print("🛑 Stop signal detected! Cancelling remaining tasks...")
                    # Save current progress before stopping
                    if listings_out:
                        print("💾 Saving progress before stopping...")
                        self.save_progress_backup(listings_out, area)
                    for remaining_future in future_to_slug:
                        remaining_future.cancel()
                    break

        # Final statistics
        print(f"\n📊 Final Statistics:")
        print(f"   ✅ Successful buildings: {success_count}")
        print(f"   📊 Empty buildings (no rental data): {empty_count}")
        print(f"   ❌ Failed buildings: {error_count}")
        print(f"   📝 Total listings collected: {total_listings}")
        print(f"✅ API scraping complete! Collected {len(listings_out)} total listings from {total_buildings} buildings")

        # Group by unit to ensure only one listing per unit (most recent and most relevant)
        grouped_listings = {}
        for listing in listings_out:
            key = (listing.get('building_slug', ''), normalize_unit(listing.get('displayUnit', '')))
            # Keep the most relevant listing for each unit
            if key not in grouped_listings:
                grouped_listings[key] = listing
            else:
                current_listing = grouped_listings[key]
                
                def get_listing_priority(lst):
                    """Get priority score for listing (higher = better)"""
                    status = lst.get('status', '') or ''
                    try:
                        status = status.upper()
                    except (AttributeError, TypeError):
                        status = ''
                    
                    # Active listings get highest priority
                    if status in ['AVAILABLE', 'ON_MARKET']:
                        return 1000
                    # Recently off market gets medium priority  
                    elif status in ['OFF_MARKET', 'RENTED', 'NO_LONGER_AVAILABLE']:
                        return 500
                    # Any other status gets low priority
                    else:
                        return 100
                
                def get_listing_date(lst):
                    """Get the most relevant date for comparison"""
                    # Priority: onMarketAt > availableAt > offMarketAt
                    date_str = (lst.get('onMarketAt') or 
                               lst.get('availableAt') or 
                               lst.get('offMarketAt') or '1900-01-01')
                    
                    try:
                        from datetime import datetime
                        return datetime.strptime(date_str, '%Y-%m-%d') if date_str != '1900-01-01' else datetime(1900, 1, 1)
                    except:
                        return datetime(1900, 1, 1)
                
                # Compare by priority first, then by date
                current_priority = get_listing_priority(current_listing)
                new_priority = get_listing_priority(listing)
                
                should_replace = False
                
                if new_priority > current_priority:
                    # New listing has higher priority (e.g., active vs rented)
                    should_replace = True
                elif new_priority == current_priority:
                    # Same priority, compare by date
                    current_date = get_listing_date(current_listing)
                    new_date = get_listing_date(listing)
                    
                    if new_date > current_date:
                        should_replace = True
                
                if should_replace:
                    grouped_listings[key] = listing
        
        # Convert back to list
        grouped_listings = list(grouped_listings.values())
        
        # Update price for current listings that need it
        def _fetch_listing_price_by_id(listing_id):
            """Fetch current price for a listing by its ID"""
            query = {
                "query": """
                query GetListing($id: ID!) {
                    listing(id: $id) {
                        id
                        price
                        lastPrice
                        priceHistory { price timestamp }
                    }
                }
                """,
                "variables": {"id": listing_id}
            }
            
            try:
                headers = {
                    'Content-Type': 'application/json',
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                    'Accept': 'application/json',
                    'Referer': 'https://streeteasy.com/',
                }
                
                response = requests.post(
                    'https://api-v6.streeteasy.com/',
                    json=query,
                    headers=headers,
                    cookies=session_cookies,
                    timeout=10
                )
                
                if response.status_code == 200:
                    data = response.json()
                    if 'data' in data and data['data'] and data['data']['listing']:
                        listing_data = data['data']['listing']
                        return listing_data.get('price'), listing_data.get('lastPrice'), listing_data.get('priceHistory', [])
                return None, None, []
                
            except Exception as e:
                return None, None, []

        def needs_price_update(listing):
            """Check if a listing needs a price update"""
            status = listing.get('status', '')
            return status in ['AVAILABLE', 'ON_MARKET']

        # Update prices for current listings
        listings_to_update = [l for l in grouped_listings if needs_price_update(l)]
        
        if listings_to_update:
            print(f"🔄 Updating current prices for {len(listings_to_update)} active listings...")
            
            for listing in listings_to_update:
                listing_id = listing.get('id')
                if listing_id:
                    current_price, last_price, price_history = _fetch_listing_price_by_id(listing_id)
                    if current_price is not None:
                        listing['price'] = current_price
                        listing['lastPrice'] = last_price
                        listing['priceHistory'] = price_history

        def apply_all_filters(listing):
            """Apply all user filters to a processed listing"""
            try:
                # Basic filters
                price = listing.get('price', 0)
                bedrooms = listing.get('bedroomCount', 0)
                
                # Price filters
                if min_price and price < min_price:
                    return False
                if max_price and price > max_price:
                    return False
                
                # Bedroom filters
                if bedrooms_filter != 'all':
                    if bedrooms_filter == 'Studio' and bedrooms != 0:
                        return False
                    elif bedrooms_filter == '3+' and bedrooms < 3:
                        return False
                    elif bedrooms_filter.isdigit() and int(bedrooms_filter) != bedrooms:
                        return False
                
                # Agent/Owner filter - Use the improved detection from _process_rentals
                if by_owner_filter != 'all':
                    # Use the is_owner field that was calculated with improved logic
                    is_owner = listing.get('is_owner', False)
                    if by_owner_filter == 'true' and not is_owner:
                        return False
                    elif by_owner_filter == 'false' and is_owner:
                        return False
                
                # Laundry filter
                if laundry_filter != 'all':
                    laundry_in_building = listing.get('laundryInBuilding', False)
                    if laundry_filter == 'In Building' and not laundry_in_building:
                        return False
                    elif laundry_filter == 'In Unit':
                        # This would need more specific data
                        pass
                
                # Pets filter
                if pets_filter != 'all':
                    pet_friendly = listing.get('petFriendly', False)
                    if pets_filter == 'true' and not pet_friendly:
                        return False
                    elif pets_filter == 'false' and pet_friendly:
                        return False
                
                # Outdoor space filter
                if outdoor_filter != 'all':
                    private_outdoor = listing.get('privateOutdoorSpace', False)
                    if outdoor_filter == 'true' and not private_outdoor:
                        return False
                    elif outdoor_filter == 'false' and private_outdoor:
                        return False
                
                # Days on market filter
                if days_on_market_filter != 'all':
                    on_market_date_str = listing.get('onMarketAt') or listing.get('availableAt')
                    off_market_date_str = listing.get('offMarketAt')
                    
                    if on_market_date_str:
                        try:
                            on_market_dt = datetime.strptime(on_market_date_str, '%Y-%m-%d')
                            
                            # Calculate days on market correctly
                            status = listing.get('status', '').upper() if listing.get('status') else ''
                            unavailable_statuses = ['NO_LONGER_AVAILABLE', 'RENTED', 'DELISTED', 'IN_CONTRACT', 'TEMPORARILY_OFF_MARKET', 'PAUSED']
                            
                            # Determine end date for calculation
                            if status in unavailable_statuses or (status == '' and off_market_date_str):
                                # Use off market date if available for completed listings
                                if off_market_date_str:
                                    try:
                                        off_market_dt = datetime.strptime(off_market_date_str, '%Y-%m-%d')
                                        if off_market_dt >= on_market_dt:
                                            days_on_market = (off_market_dt - on_market_dt).days
                                        else:
                                            days_on_market = 0  # Data inconsistency
                                    except:
                                        days_on_market = 0
                                else:
                                    days_on_market = 0
                            else:
                                # Still available - calculate to today
                                days_on_market = max(0, (datetime.now() - on_market_dt).days)
                            
                            # Apply filter based on calculated days (consistent with server.py logic)
                            if days_on_market_filter == '0-7' and not (days_on_market < 7):
                                return False
                            elif days_on_market_filter == '7-30' and not (7 <= days_on_market <= 30):
                                return False
                            elif days_on_market_filter == '30+' and not (days_on_market > 30):
                                return False
                        except:
                            pass
                
                # Month filter
                if offmarket_month_start or offmarket_month_end:
                    off_market_date = listing.get('offMarketAt')
                    if off_market_date:
                        try:
                            off_market_dt = datetime.strptime(off_market_date, '%Y-%m-%d')
                            month = off_market_dt.month
                            if offmarket_month_start and month < offmarket_month_start:
                                return False
                            if offmarket_month_end and month > offmarket_month_end:
                                return False
                        except:
                            pass
                
                return True
                
            except Exception as e:
                return False

        # Apply final filtering
        grouped_listings = [listing for listing in grouped_listings if apply_all_filters(listing)]

        def add_stabilization_analysis(listings):
            """Add rent stabilization analysis to listings"""
            
            for listing in listings:
                try:
                    price = listing.get('price', 0)
                    bedrooms = listing.get('bedroomCount', 0)
                    building_year = listing.get('building_year_built')
                    
                    # Initialize rent stabilization fields
                    listing['likely_stabilized'] = False
                    listing['stabilization_confidence'] = ''
                    listing['stabilization_evidence'] = ''
                    
                    # If already marked as rent stabilized by StreetEasy
                    if listing.get('isRentStabilized'):
                        listing['likely_stabilized'] = True
                        listing['stabilization_confidence'] = 'High'
                        listing['stabilization_evidence'] = 'Marked as rent stabilized by StreetEasy'
                        continue
                    
                    evidence = []
                    
                    # Price-based analysis
                    if bedrooms == 0 and price < 2500:  # Studio
                        evidence.append(f'Studio under $2,500 ({price})')
                    elif bedrooms == 1 and price < 3000:  # 1BR
                        evidence.append(f'1BR under $3,000 ({price})')
                    elif bedrooms == 2 and price < 4000:  # 2BR
                        evidence.append(f'2BR under $4,000 ({price})')
                    elif bedrooms >= 3 and price < 5000:  # 3BR+
                        evidence.append(f'{bedrooms}BR under $5,000 ({price})')
                    
                    # Building age analysis
                    if building_year and building_year < 1974:
                        evidence.append(f'Pre-1974 building ({building_year})')
                    
                    # Determine confidence and stabilization likelihood
                    if len(evidence) >= 2:
                        listing['likely_stabilized'] = True
                        listing['stabilization_confidence'] = 'High'
                        listing['stabilization_evidence'] = '; '.join(evidence)
                    elif len(evidence) == 1:
                        listing['likely_stabilized'] = True
                        listing['stabilization_confidence'] = 'Medium'
                        listing['stabilization_evidence'] = evidence[0]
                    
                except Exception:
                    continue
            
            # Count stabilized units
            stabilized_count = sum(1 for listing in listings if listing.get('likely_stabilized'))
            print(f"✅ Rent stabilization analysis complete. {stabilized_count} likely stabilized units found.")
            
            return listings

        # Add rent stabilization analysis
        grouped_listings = add_stabilization_analysis(grouped_listings)

        if save_to_file:
            if output_filename:
                filename = self.save_listings_to_json(grouped_listings, output_filename)
            else:
                filename = self.save_listings_to_json(grouped_listings)
            
            if filename:
                # Dual-file system: Also save to rentals_latest.json for fast server access
                # This avoids the server having to scan for the latest timestamped file
                try:
                    # Filter out delisted listings before saving latest file
                    filtered_listings = filter_delisted_listings(grouped_listings)
                    
                    data_to_save = {
                        "metadata": {
                            "timestamp": datetime.now().isoformat(),
                            "total_listings": len(filtered_listings),
                            "collection_method": "api", 
                            "area": area
                        },
                        "listings": filtered_listings
                    }
                    
                    with open('rentals_latest.json', 'w', encoding='utf-8') as f:
                        json.dump(data_to_save, f, indent=2, ensure_ascii=False)
                    print(f"✅ Also saved to rentals_latest.json for server access")
                    
                except Exception as e:
                    print(f"⚠️  Warning: Could not save rentals_latest.json: {e}")
                    
                print(f"✅ Final results: {len(grouped_listings)} unique listings saved")
                return grouped_listings
        
        return grouped_listings
    
    def close(self):
        """Clean up resources"""
        if hasattr(self, 'driver'):
            self.driver.quit()

    @staticmethod
    def introspect_type(type_name, enum=False):
        """
        Print available fields for a GraphQL type or enum from the StreetEasy API.
        Usage:
            RentalCollector.introspect_type('RentalListingDigest')
            RentalCollector.introspect_type('PropertyFeature', enum=True)
        """
        url = 'https://api-v6.streeteasy.com/'
        if enum:
            query = {
                "query": f'{{ __type(name: "{type_name}") {{ name enumValues {{ name description }} }} }}'
            }
        else:
            query = {
                "query": f'{{ __type(name: "{type_name}") {{ name fields {{ name type {{ name kind ofType {{ name kind }} }} }} }} }}'
            }
        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0',
            'Accept': 'application/json',
        }
        print(f"\nIntrospecting type: {type_name} (enum={enum})\nQuery: {query['query']}\n")
        try:
            resp = requests.post(url, json=query, headers=headers, timeout=10)
            print(f"Status: {resp.status_code}")
            data = resp.json()
            print(json.dumps(data, indent=2))
        except Exception as e:
            print(f"Error introspecting type: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Rental Scraper & GraphQL Introspection Tool")
    parser.add_argument('--introspect', type=str, help='GraphQL type or enum to introspect (e.g. RentalListingDigest)')
    parser.add_argument('--enum', action='store_true', help='Set if the type is an enum')
    
    # User parameters from web interface (all required - no defaults)
    parser.add_argument('--area', type=str, required=True, help='Area to scrape (e.g., west village, east village, soho)')
    parser.add_argument('--min-price', type=int, required=True, help='Minimum price filter')
    parser.add_argument('--max-price', type=int, required=True, help='Maximum price filter')
    parser.add_argument('--bedrooms', type=str, required=True, help='Bedroom filter (all, Studio, 1, 2, 3+)')
    parser.add_argument('--laundry', type=str, required=True, help='Laundry filter (all, In unit, In building, None)')
    parser.add_argument('--pets', type=str, required=True, help='Pets filter (all, true, false)')
    parser.add_argument('--outdoor', type=str, required=True, help='Outdoor space filter (all, true, false)')
    parser.add_argument('--by-owner', type=str, required=True, help='By owner filter (all, true, false)')
    parser.add_argument('--days-on-market', type=str, required=True, help='Days on market filter (all, 0-7, 7-30, 30+)')
    parser.add_argument('--offmarket-month-start', type=int, required=True, help='Off market month start (1-12)')
    parser.add_argument('--offmarket-month-end', type=int, required=True, help='Off market month end (1-12)')
    parser.add_argument('--workers', type=int, default=4, help='Number of parallel workers for processing buildings (default: 4)')
    
    args = parser.parse_args()

    if args.introspect:
        RentalCollector.introspect_type(args.introspect, enum=args.enum)
    else:
        # Initialize status
        write_status('starting', None, f"Starting scraper for {args.area}")
        
        scraper = RentalCollector()
        try:
            # Run the scraper with user parameters
            listings = scraper.get_listings_api(
                min_price=args.min_price,
                max_price=args.max_price,
                bedrooms_filter=args.bedrooms,
                laundry_filter=args.laundry,
                pets_filter=args.pets,
                outdoor_filter=args.outdoor,
                by_owner_filter=args.by_owner,
                days_on_market_filter=args.days_on_market,
                offmarket_month_start=args.offmarket_month_start,
                offmarket_month_end=args.offmarket_month_end,
                area=args.area,
                workers=args.workers,
                save_to_file=True
            )
            print(f"\n📊 Summary: Collected {len(listings)} listings")
            write_status('completed', None, f"Scraping completed! Collected {len(listings)} listings")
        except KeyboardInterrupt:
            print(f"\n🛑 Scraping interrupted by user")
            write_status('stopped', None, "Scraping stopped by user")
        except Exception as e:
            print(f"Error during execution: {e}")
            write_status('error', None, f"Scraping failed: {str(e)}")
        finally:
            scraper.close()

"""
USAGE:
  # Introspect a type (fields):
  python3 scraper.py --introspect RentalListingDigest

  # Introspect an enum (values):
  python3 scraper.py --introspect PropertyFeature --enum

  # Run the scraper as normal:
  python3 scraper.py
"""