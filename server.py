from flask import Flask, jsonify, render_template, send_from_directory, request
import json
import os
from datetime import datetime
import requests
import subprocess
import threading
import time

app = Flask(__name__)

# Load the rental data
def load_rental_data():
    """
    Load rental data using a dual-file system for reliability:
    1. rentals_latest.json - Fast access to current data (primary)
    2. rentals_YYYYMMDD_HHMMSS.json - Timestamped backups (fallback)
    
    This approach provides both performance and data recovery capabilities.
    """
    try:
        # Primary: Load rentals_latest.json for fast access
        if os.path.exists('rentals_latest.json'):
            with open('rentals_latest.json', 'r') as f:
                data = json.load(f)
                if isinstance(data, dict) and 'listings' in data:
                    return data['listings']
                return data
        
        # Fallback: Find the most recent timestamped backup file
        json_files = [f for f in os.listdir('.') if f.startswith('rentals_') and f.endswith('.json') and f != 'rentals_latest.json']
        if not json_files:
            return []
        latest_file = max(json_files)
        print(f"Using fallback file: {latest_file}")
        with open(latest_file, 'r') as f:
            data = json.load(f)
            if isinstance(data, dict) and 'listings' in data:
                return data['listings']
            return data
    except Exception as e:
        print(f"Error loading rental data: {e}")
        return []

# Load data at startup
rental_data = load_rental_data()

LOCATIONIQ_API_KEY = os.environ.get('LOCATIONIQ_API_KEY', 'your_locationiq_api_key_here')
SCRAPER_STATUS_FILE = 'scraper_status.json'

def set_scraper_status(status):
    try:
        with open(SCRAPER_STATUS_FILE, 'w') as f:
            json.dump({'status': status, 'timestamp': datetime.now().isoformat()}, f)
    except Exception as e:
        print(f"Error writing scraper status: {e}")

def get_scraper_status():
    if not os.path.exists(SCRAPER_STATUS_FILE):
        return {'status': 'idle', 'timestamp': datetime.now().isoformat()}
    
    # Try multiple times with small delays to handle concurrent writes
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            with open(SCRAPER_STATUS_FILE, 'r') as f:
                content = f.read().strip()
                if not content:  # Empty file
                    if attempt < max_attempts - 1:
                        time.sleep(0.1)  # Wait briefly and retry
                        continue
                    else:
                        return {'status': 'unknown', 'timestamp': datetime.now().isoformat()}
                
                return json.loads(content)
        except json.JSONDecodeError as e:
            if attempt < max_attempts - 1:
                time.sleep(0.1)  # Wait briefly and retry
                continue
            else:
                print(f"Error reading scraper status after {max_attempts} attempts: {e}")
                return {'status': 'error', 'timestamp': datetime.now().isoformat(), 'error': 'JSON decode error'}
        except Exception as e:
            print(f"Error reading scraper status: {e}")
            return {'status': 'error', 'timestamp': datetime.now().isoformat(), 'error': str(e)}

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/listings')
def get_listings():
    rental_data = load_rental_data()  # Always reload latest data
    if not rental_data:
        return jsonify([])
    
    # Get all filter parameters from request
    filters = {
        'area': request.args.get('area', 'all'),
        'by_owner': request.args.get('by_owner', 'all'),
        'bedrooms': request.args.get('bedrooms', 'all'),
        'min_price': request.args.get('min_price', type=int),
        'max_price': request.args.get('max_price', type=int), 
        'laundry': request.args.get('laundry', 'all'),
        'pets': request.args.get('pets', 'all'),
        'outdoor': request.args.get('outdoor', 'all'),
        'days_filter': request.args.get('days_filter', 'all'),
        'offmarket_month_start': request.args.get('offmarket_month_start', type=int),
        'offmarket_month_end': request.args.get('offmarket_month_end', type=int),
        'rent_stabilized': request.args.get('rent_stabilized', 'all')
    }

    # Only keep the most recent listing per unit (building_slug + unit)
    unavailable_statuses = [
        'NO_LONGER_AVAILABLE', 'RENTED', 'DELISTED', 'IN_CONTRACT', 'TEMPORARILY_OFF_MARKET', 'PAUSED'
    ]
    most_recent_per_unit = {}
    for listing in rental_data:
        key = (listing.get('building_slug', ''), listing.get('displayUnit', ''))
        status = listing.get('status', '')
        off_market = listing.get('offMarketAt')
        if not off_market:
            off_market_dt = datetime.min
        else:
            try:
                off_market_dt = datetime.strptime(off_market, '%Y-%m-%d')
            except Exception:
                off_market_dt = datetime.min
        # Prefer available listings
        if key not in most_recent_per_unit:
            most_recent_per_unit[key] = (off_market_dt, listing)
        else:
            current_status = most_recent_per_unit[key][1].get('status', '')
            # If current is unavailable and this one is available, prefer available
            if current_status in unavailable_statuses and status not in unavailable_statuses:
                most_recent_per_unit[key] = (off_market_dt, listing)
            # If both are available, prefer the one with the latest onMarketAt or availableAt if present
            elif status not in unavailable_statuses and current_status not in unavailable_statuses:
                # Use onMarketAt or availableAt if present, else fallback to offMarketAt
                this_on = listing.get('onMarketAt') or listing.get('availableAt') or off_market
                curr_on = most_recent_per_unit[key][1].get('onMarketAt') or most_recent_per_unit[key][1].get('availableAt') or most_recent_per_unit[key][1].get('offMarketAt')
                try:
                    this_on_dt = datetime.strptime(this_on, '%Y-%m-%d')
                except Exception:
                    this_on_dt = datetime.min
                try:
                    curr_on_dt = datetime.strptime(curr_on, '%Y-%m-%d')
                except Exception:
                    curr_on_dt = datetime.min
                if this_on_dt > curr_on_dt:
                    most_recent_per_unit[key] = (off_market_dt, listing)
            # If both are unavailable, prefer the one with the latest offMarketAt
            elif status in unavailable_statuses and current_status in unavailable_statuses:
                if off_market_dt > most_recent_per_unit[key][0]:
                    most_recent_per_unit[key] = (off_market_dt, listing)
    filtered_listings = [v[1] for v in most_recent_per_unit.values()]

    # Transform the data to match the frontend's expected format
    transformed_listings = []
    for listing in filtered_listings:
        # Calculate days on market
        off_market_date = datetime.strptime(listing.get('offMarketAt', datetime.now().isoformat()), '%Y-%m-%d')
        days_on_market = (datetime.now() - off_market_date).days
        transformed_listing = {
            'id': listing.get('id', ''),
            'price': listing.get('price', 0),
            'beds': str(listing.get('bedroomCount', 0)),
            'baths': str(listing.get('fullBathroomCount', 0) + (0.5 * listing.get('halfBathroomCount', 0))),
            'sqft': listing.get('sqft', listing.get('livingAreaSize', 0)),
            'unit': listing.get('displayUnit', 'N/A'),
            'address': listing.get('building_address', ''),
            'building_slug': listing.get('building_slug', ''),
            'building_id': listing.get('building_id', ''),
            'building_year_built': listing.get('building_year_built', 'N/A'),
            'building_total_units': listing.get('building_total_units', 'N/A'),
            'laundry_type': 'In building' if listing.get('laundryInBuilding', False) else 'None',
            'pets_allowed': listing.get('petFriendly', False),
            'private_outdoor_space': listing.get('privateOutdoorSpace', False),
            'offMarketAt': listing.get('offMarketAt', datetime.now().isoformat()),
            'days_on_market': days_on_market,
            'url': f"https://streeteasy.com/rental/{listing.get('id', '')}",
            'agent_name': listing.get('agentName', 'Owner'),
            'agent_phone': listing.get('agentPhone', 'N/A'),
            'agent_email': listing.get('agentEmail', 'N/A'),
            'likely_stabilized': listing.get('likely_stabilized', False),
            'stabilization_confidence': listing.get('stabilization_confidence', ''),
            'stabilization_evidence': listing.get('stabilization_evidence', ''),
            'is_owner': listing.get('is_owner', False),
            'latitude': listing.get('latitude', None),
            'longitude': listing.get('longitude', None),
            'source_area': listing.get('source_area', '')
        }
        transformed_listings.append(transformed_listing)

    # Apply server-side filters
    filtered_listings = apply_filters(transformed_listings, filters)
    
    return jsonify(filtered_listings)

def apply_filters(listings, filters):
    """Apply all filters server-side for better performance"""
    filtered = listings
    
    # Apply area filter using the source_area field from scraper
    if filters['area'] != 'all' and filters['area']:
        area_filter = filters['area'].lower().strip()
        
        area_filtered = []
        for listing in filtered:
            # Check if listing has source_area field (from scraper)
            source_area = listing.get('source_area', '').lower().strip()
            
            # Match against the area the building was scraped from
            if source_area and area_filter == source_area:
                area_filtered.append(listing)
            # Handle variations like "west village" vs "west-village" 
            elif source_area and area_filter.replace(' ', '-') == source_area.replace(' ', '-'):
                area_filtered.append(listing)
            elif source_area and area_filter.replace('-', ' ') == source_area.replace('-', ' '):
                area_filtered.append(listing)

        
        # If no source_area field exists in data, show message that scraper needs to be updated
        if len(area_filtered) == 0 and any('source_area' not in listing for listing in filtered[:5]):
            print(f"WARNING: No source_area field found in listings. Data needs to be re-scraped with updated scraper.")
        
        filtered = area_filtered
    
    # Apply by_owner filter
    if filters['by_owner'] != 'all':
        if filters['by_owner'] == 'true':
            filtered = [l for l in filtered if l.get('is_owner', False)]
        elif filters['by_owner'] == 'false':
            filtered = [l for l in filtered if not l.get('is_owner', False)]
    
    # Apply bedroom filter
    if filters['bedrooms'] != 'all':
        if filters['bedrooms'] == '3+':
            filtered = [l for l in filtered if l.get('beds') and l['beds'].isdigit() and int(l['beds']) >= 3]
        elif filters['bedrooms'] == 'Studio':
            filtered = [l for l in filtered if l.get('beds') in ['0', 'Studio']]
        else:
            filtered = [l for l in filtered if l.get('beds') == filters['bedrooms']]
    
    # Apply price filters
    if filters['min_price'] is not None:
        filtered = [l for l in filtered if l.get('price', 0) >= filters['min_price']]
    if filters['max_price'] is not None:
        filtered = [l for l in filtered if l.get('price', 0) <= filters['max_price']]
    
    # Apply laundry filter
    if filters['laundry'] != 'all':
        filtered = [l for l in filtered if l.get('laundry_type') == filters['laundry']]
    
    # Apply pets filter
    if filters['pets'] != 'all':
        pets_bool = filters['pets'].lower() == 'true'
        filtered = [l for l in filtered if l.get('pets_allowed', False) == pets_bool]
    
    # Apply outdoor filter
    if filters['outdoor'] != 'all':
        outdoor_bool = filters['outdoor'].lower() == 'true'
        filtered = [l for l in filtered if l.get('private_outdoor_space', False) == outdoor_bool]
    
    # Apply days on market filter
    if filters['days_filter'] != 'all':
        if filters['days_filter'] == '0-7':
            filtered = [l for l in filtered if l.get('days_on_market', 0) < 7]
        elif filters['days_filter'] == '7-30':
            filtered = [l for l in filtered if 7 <= l.get('days_on_market', 0) <= 30]
        elif filters['days_filter'] == '30+':
            filtered = [l for l in filtered if l.get('days_on_market', 0) > 30]
    
    # Apply off-market month filter
    if filters['offmarket_month_start'] and filters['offmarket_month_end']:
        month_start = filters['offmarket_month_start']
        month_end = filters['offmarket_month_end']
        filtered = [l for l in filtered if filter_by_month(l, month_start, month_end)]
    
    # Apply rent stabilized filter
    if filters['rent_stabilized'] != 'all':
        if filters['rent_stabilized'] == 'likely':
            filtered = [l for l in filtered if l.get('likely_stabilized', False)]
        elif filters['rent_stabilized'] == 'unlikely':
            filtered = [l for l in filtered if not l.get('likely_stabilized', False)]
        elif filters['rent_stabilized'] in ['high', 'medium', 'low']:
            filtered = [l for l in filtered if (l.get('likely_stabilized', False) and 
                       l.get('stabilization_confidence', '').lower() == filters['rent_stabilized'])]
    
    return filtered

def filter_by_month(listing, month_start, month_end):
    """Helper function to filter by off-market month"""
    off_market_date = listing.get('offMarketAt')
    if not off_market_date:
        return False
    
    try:
        month = datetime.strptime(off_market_date, '%Y-%m-%d').month
        if month_start <= month_end:
            return month_start <= month <= month_end
        else:
            # Wrap around year (e.g., Nov to Feb)
            return month >= month_start or month <= month_end
    except:
        return False

@app.route('/api/statistics')
def get_statistics():
    if not rental_data:
        return jsonify({
            'total_listings': 0,
            'average_price': 0,
            'price_range': {'min': 0, 'max': 0},
            'last_updated': datetime.now().isoformat()
        })
    
    prices = [listing.get('price', 0) for listing in rental_data if 'price' in listing]
    
    return jsonify({
        'total_listings': len(rental_data),
        'average_price': sum(prices) / len(prices) if prices else 0,
        'price_range': {
            'min': min(prices) if prices else 0,
            'max': max(prices) if prices else 0
        },
        'last_updated': datetime.now().isoformat()
    })

@app.route('/api/geocode')
def geocode():
    address = request.args.get('address')
    if not address:
        return jsonify({'error': 'Missing address parameter'}), 400
    url = f'https://us1.locationiq.com/v1/search.php?key={LOCATIONIQ_API_KEY}&q={address}, New York, NY&format=json&limit=1'
    resp = requests.get(url, headers={'User-Agent': 'LeaseExplorer/1.0'})
    return jsonify(resp.json())

@app.route('/api/scraper-status')
def scraper_status():
    status = get_scraper_status()
    
    # Enhance status with more details for frontend display
    if status.get('status') == 'running' and 'progress' in status:
        progress = status['progress']
        
        # Format progress message based on phase
        if 'pages' in progress:
            pages = progress['pages']
            if pages.get('phase') == 'discovery':
                status['display_message'] = f"Discovering buildings ({pages['total']} pages found)"
            elif pages.get('phase') == 'scraping_buildings':
                percent = (pages['current'] / pages['total']) * 100 if pages['total'] > 0 else 0
                status['display_message'] = f"Scraping buildings: page {pages['current']}/{pages['total']} ({percent:.1f}%)"
                status['progress_percent'] = percent
        
        elif 'buildings' in progress:
            buildings = progress['buildings'] 
            if buildings.get('phase') == 'processing_buildings':
                percent = (buildings['current'] / buildings['total']) * 100 if buildings['total'] > 0 else 0
                status['display_message'] = f"Processing buildings: {buildings['current']}/{buildings['total']} ({percent:.1f}%)"
                status['progress_percent'] = percent
    
    return jsonify(status)

@app.route('/api/stop-scraper', methods=['POST'])
def stop_scraper():
    """Request the scraper to stop gracefully"""
    try:
        # Create a stop signal file that the scraper can check
        with open('scraper_stop_signal.txt', 'w') as f:
            f.write(datetime.now().isoformat())
        
        # Update status to indicate stop was requested
        set_scraper_status('stopping')
        
        return jsonify({
            'success': True,
            'message': 'Stop signal sent to scraper'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/run-scraper', methods=['POST'])
def run_scraper():
    """Run the scraper with user-specified parameters"""
    try:
        # Get parameters from request
        params = request.get_json()
        
        # Get parameters from the client (no server defaults)
        area = params.get('area')
        min_price = params.get('min_price')
        max_price = params.get('max_price')
        bedrooms = params.get('bedrooms')
        laundry = params.get('laundry')
        pets = params.get('pets')
        outdoor = params.get('outdoor')
        by_owner = params.get('by_owner')
        days_on_market = params.get('days')
        offmarket_month_start = params.get('offmarket_month_start')
        offmarket_month_end = params.get('offmarket_month_end')
        
        
        
        # Build command with parameters
        cmd = [
            'python3', '-u', 'scraper.py',
            '--area', str(area),
            '--min-price', str(min_price),
            '--max-price', str(max_price),
            '--bedrooms', str(bedrooms),
            '--laundry', str(laundry),
            '--pets', str(pets),
            '--outdoor', str(outdoor),
            '--by-owner', str(by_owner),
            '--days-on-market', str(days_on_market),
            '--offmarket-month-start', str(offmarket_month_start),
            '--offmarket-month-end', str(offmarket_month_end),
            '--workers', '4'  # Use 4 workers for faster processing
        ]
        
        # Run scraper in a separate thread to avoid blocking
        def run_scraper_thread():
            import threading
            def stream_output(stream, prefix):
                for line in iter(stream.readline, ''):
                    if line:
                        print(f"{prefix} {line}", end='')
                stream.close()
            try:
                set_scraper_status('running')
                process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
                t_out = threading.Thread(target=stream_output, args=(process.stdout, '[SCRAPER STDOUT]'))
                t_err = threading.Thread(target=stream_output, args=(process.stderr, '[SCRAPER STDERR]'))
                t_out.start()
                t_err.start()
                process.wait()
                t_out.join()
                t_err.join()
                if process.returncode == 0:
                    print("Scraper completed successfully")
                    # Force reload the rental data
                    global rental_data
                    rental_data = load_rental_data()
                    print(f"Reloaded {len(rental_data)} listings after scraper completion")
                    set_scraper_status('idle')
                else:
                    print(f"Scraper failed with exit code {process.returncode}")
                    set_scraper_status('error')
            except subprocess.TimeoutExpired:
                print("Scraper timed out")
                set_scraper_status('error')
            except Exception as e:
                print(f"Scraper error: {e}")
                set_scraper_status('error')
        
        # Start scraper in background
        thread = threading.Thread(target=run_scraper_thread)
        thread.daemon = True
        thread.start()
        
        # For now, return success immediately (in a real implementation, you'd want to track progress)
        return jsonify({
            'success': True,
            'message': 'Scraper started successfully',
            'listings_count': 0  # Will be updated when scraper completes
        })
        
    except Exception as e:
        set_scraper_status('error')
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

if __name__ == '__main__':
    app.run(port=5001)