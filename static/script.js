document.addEventListener('DOMContentLoaded', function() {
    // Restore filters from localStorage if present
    const filterIds = [
        'area-filter', 'custom-area-input', 'min-price', 'max-price', 'bedrooms-filter', 'laundry-filter', 'pets-filter',
        'outdoor-filter', 'days-filter', 'offmarket-month-start', 'offmarket-month-end', 'by-owner-filter', 'rent-stabilized-filter', 'last-off-market-filter'
    ];
    filterIds.forEach(id => {
        const saved = localStorage.getItem('filter_' + id);
        if (saved !== null) {
            const el = document.getElementById(id);
            if (el) {
                el.value = saved;

            }
        }
    });

    // Set off-market month dropdowns to all months by default if not set (permissive filtering)
    if (!localStorage.getItem('filter_offmarket-month-start'))
        document.getElementById('offmarket-month-start').value = 1;  // January (all months)
    if (!localStorage.getItem('filter_offmarket-month-end'))
        document.getElementById('offmarket-month-end').value = 12; // December (all months)


    // Load initial data
    fetchListings();
    
    // Add event listeners for all filter dropdowns and save to localStorage
    const saveFilter = id => {
        const el = document.getElementById(id);
        if (!el) return;
        const eventType = el.tagName === 'INPUT' ? 'input' : 'change';
        el.addEventListener(eventType, e => {
            localStorage.setItem('filter_' + id, el.value);
            debouncedApplyFilters();
        });
    };
    filterIds.forEach(saveFilter);

    // Add scraper button event listeners
    document.getElementById('run-scraper-btn').addEventListener('click', runScraper);
    document.getElementById('stop-scraper-btn').addEventListener('click', stopScraper);
    
    // Add refresh button event listener (only if element exists)
    const refreshBtn = document.getElementById('refresh-btn');
    if (refreshBtn) {
        refreshBtn.addEventListener('click', () => {
        showToast('Refreshing listings...', 2000);
        fetchListings();
    });
    }
    
    // Handle area filter custom option
    document.getElementById('area-filter').addEventListener('change', function() {
        const customGroup = document.getElementById('custom-area-group');
        if (this.value === 'custom') {
            customGroup.style.display = 'block';
        } else {
            customGroup.style.display = 'none';
        }
    });
    
    // Trigger area filter change on page load to show/hide custom input
    document.getElementById('area-filter').dispatchEvent(new Event('change'));
    
    // Initialize map immediately since it's always visible
    setTimeout(() => {
        initializeMap();
    }, 100);
});

// Global variables for map
let map;
let markers = [];
window.mapMarkersById = {};
window.mapInitialized = false;
window.currentFilteredListings = [];
let mapLoadingTimeout;
let mapMarkersLoaded = 0;
let mapMarkersExpected = 0;
let mapGeocodeErrors = 0;

// Throttle geocode requests to avoid rate limiting
let lastGeocodeTime = 0;

function initializeMap() {
    if (window.mapInitialized) return;
    
    // Check if Leaflet is loaded
    if (typeof L === 'undefined') {
        console.error('Leaflet library not loaded');
        document.getElementById('loading').textContent = 'Map library failed to load. Please refresh the page.';
        document.getElementById('loading').style.display = '';
        return;
    }
    
    // Initialize the map centered on West Village
    map = L.map('map').setView([40.7359, -74.0036], 15);
    
    // Add OpenStreetMap tiles
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '© OpenStreetMap contributors'
    }).addTo(map);
    
    window.mapInitialized = true;
    
    // Show loading overlay and set timeout to hide it after 3 seconds
    document.getElementById('loading').style.display = '';
    mapLoadingTimeout = setTimeout(() => {
        document.getElementById('loading').style.display = 'none';
    }, 3000);
    
    // Update map with current filtered data
    updateMap();
}

function updateMap() {
    if (!window.mapInitialized) return;
    
    // Clear existing markers
    markers.forEach(marker => map.removeLayer(marker));
    markers = [];
    mapMarkersLoaded = 0;
    mapGeocodeErrors = 0;
    window.mapMarkersById = {};
    
    // Use client-side filtered listings if available, otherwise server listings
    const mapListings = window.clientFilteredListings || window.allListings || [];
    window.currentFilteredListings = mapListings;
    mapMarkersExpected = mapListings.length;
    
    if (mapListings.length === 0) {
        document.getElementById('loading').style.display = 'none';
        return;
    }
    
    // Show loading overlay if there are markers to load
    if (mapMarkersExpected > 0) {
        document.getElementById('loading').style.display = '';
        if (mapLoadingTimeout) clearTimeout(mapLoadingTimeout);
        mapLoadingTimeout = setTimeout(() => {
            document.getElementById('loading').style.display = 'none';
        }, 3000);
    } else {
        document.getElementById('loading').style.display = 'none';
    }
    
    // Add markers using lat/lon if present, otherwise geocode
    if (mapMarkersExpected === 0) return;
    mapListings.forEach(listing => {
        if (listing.latitude && listing.longitude) {
            addMarkerToMap(parseFloat(listing.latitude), parseFloat(listing.longitude), listing);
        } else {
            geocodeAddress(listing.address, listing);
        }
    });
}

function geocodeAddress(address, listing) {
    const now = Date.now();
    const delay = Math.max(0, 500 - (now - lastGeocodeTime)); // 500ms between requests
    setTimeout(() => {
        lastGeocodeTime = Date.now();
        // Use backend proxy for geocoding
        const url = `/api/geocode?address=${encodeURIComponent(address)}`;
        fetch(url)
            .then(response => response.json())
            .then(data => {
                if (data && data.length > 0) {
                    const location = data[0];
                    addMarkerToMap(
                        parseFloat(location.lat), 
                        parseFloat(location.lon), 
                        listing
                    );
                    mapMarkersLoaded++;
                    // Hide loading overlay after first marker loads
                    if (mapMarkersLoaded === 1) {
                        document.getElementById('loading').style.display = 'none';
                        if (mapLoadingTimeout) clearTimeout(mapLoadingTimeout);
                    }
                } else {
                    mapGeocodeErrors++;
                    checkMapGeocodeErrors();
                }
            })
            .catch(error => {
                mapGeocodeErrors++;
                checkMapGeocodeErrors();
            });
    }, delay);
}

function checkMapGeocodeErrors() {
    // If all geocoding failed, show error
    if (mapGeocodeErrors >= mapMarkersExpected && mapMarkersExpected > 0) {
        document.getElementById('loading').textContent = 'Could not geocode any addresses. Please try again later.';
        document.getElementById('loading').style.display = '';
        setTimeout(() => {
            document.getElementById('loading').style.display = 'none';
        }, 4000);
    }
}

function addMarkerToMap(lat, lon, listing) {
    // Create custom marker
    const marker = L.divIcon({
        className: 'price-marker',
        html: `$${(listing.price / 1000).toFixed(0)}k`,
        iconSize: [30, 30],
        iconAnchor: [15, 15]
    });
    
    // Add marker to map
    const mapMarker = L.marker([lat, lon], { icon: marker }).addTo(map);
    
    // Store marker by listing ID
    if (listing.id) {
        window.mapMarkersById[listing.id] = mapMarker;
    }
    
    markers.push(mapMarker);
    
    // Create popup content
    const popupContent = `
        <div class="popup-content">
            <div class="popup-header">
                <div class="popup-price">$${listing.price.toLocaleString()}/month</div>
                <div class="popup-details">
                    <div><strong>Beds:</strong> ${listing.beds === '0' ? 'Studio' : listing.beds}</div>
                    <div><strong>Baths:</strong> ${listing.baths}</div>
                    <div><strong>Unit:</strong> ${listing.unit}</div>
                    <div><strong>SqFt:</strong> ${listing.sqft || 'N/A'}</div>
                    <div><strong>Days on Market:</strong> ${listing.days_on_market}</div>
                    <div><strong>Agent:</strong> ${listing.agent_name}</div>
                    <div><strong>Email:</strong> ${listing.agent_email || 'N/A'}</div>
                </div>
            </div>
            <div class="popup-address">${listing.address}</div>
            <div class="popup-actions">
                <a href="${listing.url}" target="_blank" class="popup-link">View</a>
            </div>
        </div>
    `;
    
    mapMarker.bindPopup(popupContent);
    
    // Add click handler to highlight corresponding listing
    mapMarker.on('click', function() {
        const listingRow = document.querySelector(`[data-listing-id="${listing.id}"]`);
        if (listingRow) {
            // Remove previous highlights
            document.querySelectorAll('.listing-row.highlighted').forEach(row => {
                row.classList.remove('highlighted');
            });
            
            // Add highlight to clicked listing
            listingRow.classList.add('highlighted');
            
            // Scroll to the listing
            listingRow.scrollIntoView({ behavior: 'smooth', block: 'center' });
        }
    });
}

function getCurrentFilteredListings() {
    // Guard: if listings not loaded, return empty array
    if (!Array.isArray(window.allListings)) {
        return [];
    }
    // Get selected values for each filter dropdown
    const selectedBeds = document.getElementById('bedrooms-filter').value;
    const selectedLaundry = document.getElementById('laundry-filter').value;
    const selectedPets = document.getElementById('pets-filter').value;
    const selectedOutdoor = document.getElementById('outdoor-filter').value;
    const selectedDays = document.getElementById('days-filter').value;
    const monthStart = parseInt(document.getElementById('offmarket-month-start').value);
    const monthEnd = parseInt(document.getElementById('offmarket-month-end').value);
    let filteredListings = window.allListings;
    
    // Apply beds filter
    if (selectedBeds !== 'all') {
        filteredListings = filteredListings.filter(listing => {
            // Handle "3+" selection
            if (selectedBeds === '3+') {
                const beds = parseInt(listing.beds);
                if (!isNaN(beds) && beds >= 3) {
                    return true;
                }
            }
            
            // Handle other bed selections (including Studio)
            return selectedBeds === listing.beds || 
                  (selectedBeds === 'Studio' && (listing.beds === '0' || listing.beds === 'Studio'));
        });
    }
    
    // Apply laundry filter
    if (selectedLaundry !== 'all') {
        filteredListings = filteredListings.filter(listing => 
            selectedLaundry === listing.laundry_type
        );
    }
    
    // Apply pets filter
    if (selectedPets !== 'all') {
        filteredListings = filteredListings.filter(listing => {
            // Accept true/false, string, or null
            const petsAllowed = (typeof listing.pets_allowed === 'boolean') ? listing.pets_allowed : String(listing.pets_allowed).toLowerCase() === 'true';
            return String(petsAllowed) === selectedPets;
        });
    }
    
    // Apply outdoor space filter
    if (selectedOutdoor !== 'all') {
        filteredListings = filteredListings.filter(listing => 
            selectedOutdoor === String(listing.private_outdoor_space)
        );
    }
    
    // Apply days on market filter
    if (selectedDays !== 'all') {
        filteredListings = filteredListings.filter(listing => {
            const days = listing.days_on_market;
            if (!days) return false;
            
            return (selectedDays === '0-7' && days < 7) || 
                (selectedDays === '7-30' && days >= 7 && days <= 30) ||
                (selectedDays === '30+' && days > 30);
        });
    }
    
    // Apply offMarketAt month filter
    if (!isNaN(monthStart) && !isNaN(monthEnd)) {
        filteredListings = filteredListings.filter(listing => {
            if (!listing.offMarketAt) return false;
            const month = new Date(listing.offMarketAt).getMonth() + 1; // JS months are 0-based
            if (monthStart <= monthEnd) {
                return month >= monthStart && month <= monthEnd;
            } else {
                // Wrap around year (e.g., Nov to Feb)
                return month >= monthStart || month <= monthEnd;
            }
        });
    }
    
    return filteredListings;
}

// Debounced version of updateMap
const debouncedUpdateMap = debounce(updateMap, 300);

function updateStats(listings) {
    // Update total count
    document.getElementById('total-count').textContent = listings.length;
    
    // Calculate average price
    const totalPrice = listings.reduce((sum, listing) => sum + listing.price, 0);
    const avgPrice = listings.length > 0 ? Math.round(totalPrice / listings.length) : 0;
    document.getElementById('avg-price').textContent = `$${avgPrice.toLocaleString()}`;
    
    // Count stabilized units
    const stabilizedCount = listings.filter(listing => listing.likely_stabilized).length;
    document.getElementById('stabilized-count').textContent = stabilizedCount;
}

function displayListings(listings) {
    listings.sort((a, b) => a.price - b.price);
    const container = document.getElementById('listings-container');
    if (listings.length === 0) {
        container.innerHTML = `
            <div class="listing-card empty-state-card">
                <div class="empty-state-icon">🔍</div>
                <div class="empty-state-title">No listings found</div>
                <div class="empty-state-subtitle">Try running the scraper or adjusting your filters to see available rentals.</div>
            </div>
        `;
        return;
    }
    
    // Clear the container
    container.innerHTML = '';
    
    // Create and append listing cards
    listings.forEach(listing => {
        const card = document.createElement('div');
        card.className = `listing-card ${listing.likely_stabilized ? 'stabilized' : ''}`;
        
        // Scroll to marker on map when card is clicked
        card.addEventListener('click', () => {
            if (listing.id && window.mapMarkersById[listing.id]) {
                const marker = window.mapMarkersById[listing.id];
                map.setView(marker.getLatLng(), map.getZoom(), { animate: true });
                marker.openPopup();
            }
        });
        
        // Create header with address and price
        const header = document.createElement('div');
        header.className = 'listing-header';
        
        const addressTitle = document.createElement('h2');
        addressTitle.textContent = listing.address;
        header.appendChild(addressTitle);
        
        const priceTag = document.createElement('span');
        priceTag.className = 'price';
        priceTag.textContent = `$${listing.price.toLocaleString()}`;
        header.appendChild(priceTag);
        
        // Create details section
        const details = document.createElement('div');
        details.className = 'listing-details';
        
        const specs = document.createElement('p');
        specs.innerHTML = `${listing.beds} bed${listing.beds !== '1' ? 's' : ''} • ${listing.baths} bath${listing.baths !== '1' ? 's' : ''} • ${listing.sqft} sq ft`;
        
        const unitInfo = document.createElement('p');
        unitInfo.innerHTML = `Unit: ${listing.unit || 'N/A'}`;
        
        const offMarket = document.createElement('p');
        offMarket.textContent = `Off market: ${new Date(listing.offMarketAt).toLocaleDateString()}`;
        
        const building = document.createElement('p');
        building.textContent = `Building: Built ${listing.building_year_built} • ${listing.building_total_units} units`;
        
        details.appendChild(specs);
        details.appendChild(unitInfo);
        details.appendChild(offMarket);
        details.appendChild(building);
        
        // Create amenities section
        const amenities = document.createElement('div');
        amenities.className = 'amenities';

        // Laundry amenity
        if (listing.laundry_type === 'In unit') {
            const inUnit = document.createElement('span');
            inUnit.className = 'amenity active';
            inUnit.textContent = 'Laundry in-unit';
            amenities.appendChild(inUnit);
        } else if (listing.laundry_type === 'In building') {
            const inBuilding = document.createElement('span');
            inBuilding.className = 'amenity active';
            inBuilding.textContent = 'Laundry in building';
            amenities.appendChild(inBuilding);
        }

        // Pets amenity
        const petsAllowed = document.createElement('span');
        petsAllowed.className = `amenity ${listing.pets_allowed ? 'active' : ''}`;
        petsAllowed.textContent = 'Pets allowed';
        amenities.appendChild(petsAllowed);

        // Outdoor space amenity
        const outdoorSpace = document.createElement('span');
        outdoorSpace.className = `amenity ${listing.private_outdoor_space ? 'active' : ''}`;
        outdoorSpace.textContent = 'Outdoor space';
        amenities.appendChild(outdoorSpace);
        
        // Add stabilized info if applicable
        if (listing.likely_stabilized) {
            const stabilizedDiv = document.createElement('div');
            stabilizedDiv.className = 'stabilized-info';
            
            const stabilizedTitle = document.createElement('p');
            stabilizedTitle.innerHTML = `<strong>Likely Rent Stabilized</strong> (${listing.stabilization_confidence} confidence)`;
            
            const stabilizedEvidence = document.createElement('p');
            stabilizedEvidence.textContent = listing.stabilization_evidence;
            
            stabilizedDiv.appendChild(stabilizedTitle);
            stabilizedDiv.appendChild(stabilizedEvidence);
            
            card.appendChild(stabilizedDiv);
        }
        
        // Create footer with agent info and link
        const footer = document.createElement('div');
        footer.className = 'listing-footer';
        
        const agent = document.createElement('p');
        agent.innerHTML = `Listed by: ${listing.agent_name || 'Owner'}<br>Email: ${listing.agent_email || 'N/A'}<br>Phone: ${listing.agent_phone || 'N/A'}`;
        
        const viewButton = document.createElement('a');
        viewButton.href = listing.url;
        viewButton.className = 'view-button';
        viewButton.textContent = 'View';
        viewButton.target = '_blank';
        
        footer.appendChild(agent);
        footer.appendChild(viewButton);
        
        // Assemble the card
        card.appendChild(header);
        card.appendChild(details);
        card.appendChild(amenities);
        card.appendChild(footer);
        
        // Add the card to the container
        container.appendChild(card);
    });
}

function applyFilters() {
    // Server-side filtering: just fetch new data with current filter parameters
    fetchListings();
}

// Client-side filtering function for last off market date
function applyClientSideFilters(listings) {
    if (!listings || listings.length === 0) return listings;
    
    const lastOffMarketFilter = document.getElementById('last-off-market-filter')?.value;
    
    if (!lastOffMarketFilter || lastOffMarketFilter === 'all') {
        return listings;
    }
    
    const currentDate = new Date();
    const filterValue = parseInt(lastOffMarketFilter);
    
    return listings.filter(listing => {
        if (!listing.offMarketAt) return false;
        
        try {
            const offMarketDate = new Date(listing.offMarketAt);
            const yearsDiff = (currentDate - offMarketDate) / (1000 * 60 * 60 * 24 * 365.25);
            
            if (filterValue === 6) {
                // 6+ years ago
                return yearsDiff >= 6;
            } else {
                // Within X years
                return yearsDiff <= filterValue;
            }
        } catch (error) {
            console.error('Error parsing off market date:', listing.offMarketAt, error);
            return false;
        }
    });
}

// Use debounce to prevent too many rapid updates
function debounce(func, wait) {
    let timeout;
    return function() {
        const context = this;
        const args = arguments;
        clearTimeout(timeout);
        timeout = setTimeout(() => func.apply(context, args), wait);
    };
}

// Debounced version of applyFilters
const debouncedApplyFilters = debounce(applyFilters, 300);

function fetchListings() {
    setListingsLoadingOverlay(true);
    
    // Get all current filter values
    const filters = new URLSearchParams();
    
    // Add all filter parameters
    const areaFilter = document.getElementById('area-filter');
    const customAreaInput = document.getElementById('custom-area-input');
    const areaValue = areaFilter ? (areaFilter.value === 'custom' ? customAreaInput?.value : areaFilter.value) : 'all';
    
    const filterParams = {
        'area': areaValue,
        'by_owner': document.getElementById('by-owner-filter').value,
        'bedrooms': document.getElementById('bedrooms-filter').value,
        'min_price': document.getElementById('min-price').value,
        'max_price': document.getElementById('max-price').value,
        'laundry': document.getElementById('laundry-filter').value,
        'pets': document.getElementById('pets-filter').value,
        'outdoor': document.getElementById('outdoor-filter').value,
        'days_filter': document.getElementById('days-filter').value,
        'offmarket_month_start': document.getElementById('offmarket-month-start').value,
        'offmarket_month_end': document.getElementById('offmarket-month-end').value,
        'rent_stabilized': document.getElementById('rent-stabilized-filter').value
    };

    
    // Add parameters, but always include area filter for clarity
    for (const [key, value] of Object.entries(filterParams)) {
        if (key === 'area') {
            // Always send area filter to be explicit about what we want
            filters.append(key, value || 'all');
        } else if (value && value !== 'all' && value !== '') {
            filters.append(key, value);
        }
    }

    
    fetch(`/api/listings?${filters.toString()}`)
        .then(response => response.json())
        .then(data => {
            setListingsLoadingOverlay(false);
            window.allListings = data;
            
            // Apply client-side filters
            const filteredData = applyClientSideFilters(data);
            window.clientFilteredListings = filteredData;
            
            // If no results and we have restrictive filters, offer to reset them
            if (filteredData.length === 0 && (filters.toString() !== '' || document.getElementById('last-off-market-filter')?.value !== 'all')) {
                const resetFiltersBtn = document.createElement('button');
                resetFiltersBtn.textContent = 'Reset All Filters';
                resetFiltersBtn.style.cssText = `
                    background: #dc2626; color: white; border: none; padding: 8px 16px; 
                    border-radius: 4px; margin: 10px 0; cursor: pointer; font-size: 14px;
                `;
                resetFiltersBtn.onclick = () => {
                    // Clear all saved filters
                    const filterIds = [
                        'area-filter', 'custom-area-input', 'min-price', 'max-price', 'bedrooms-filter', 'laundry-filter', 'pets-filter',
                        'outdoor-filter', 'days-filter', 'offmarket-month-start', 'offmarket-month-end', 'by-owner-filter', 'rent-stabilized-filter', 'last-off-market-filter'
                    ];
                    filterIds.forEach(id => {
                        localStorage.removeItem('filter_' + id);
                        const el = document.getElementById(id);
                        if (el) {
                            el.value = el.tagName === 'SELECT' ? 'all' : '';
                        }
                    });
                    
                    // Reset month filters to show all months
                    document.getElementById('offmarket-month-start').value = 1;
                    document.getElementById('offmarket-month-end').value = 12;
                    
                    // Reset last off market filter to show all
                    document.getElementById('last-off-market-filter').value = 'all';
                    
                    showToast('Filters reset. Refreshing listings...', 2000);
                    setTimeout(() => fetchListings(), 500);
                };
                
                // Add the button to the listings section
                const listingsSection = document.querySelector('.listings-section h2');
                if (listingsSection && !document.getElementById('reset-filters-btn')) {
                    resetFiltersBtn.id = 'reset-filters-btn';
                    listingsSection.parentNode.insertBefore(resetFiltersBtn, listingsSection.nextSibling);
                }
            } else {
                // Remove reset button if we have results
                const resetBtn = document.getElementById('reset-filters-btn');
                if (resetBtn) resetBtn.remove();
            }
            
            displayListings(filteredData);
            updateStats(filteredData);
            updateMap();
        })
        .catch(error => {
            setListingsLoadingOverlay(false);
            showToast('Error loading listings. Please try again.', 5000);
        });
}

// Show toast notification
function showToast(message, duration = 3500) {
    const toast = document.getElementById('toast-notification');
    if (!toast) return;
    toast.textContent = message;
    toast.classList.add('show');
    toast.classList.remove('hide');
    toast.style.display = 'block';
    setTimeout(() => {
        toast.classList.add('hide');
        toast.classList.remove('show');
        setTimeout(() => {
            toast.style.display = 'none';
        }, 350);
    }, duration);
}

// Show/hide listings loading overlay
function setListingsLoadingOverlay(visible) {
    const overlay = document.getElementById('listings-loading-overlay');
    if (!overlay) return;
    if (visible) {
        overlay.classList.add('active');
    } else {
        overlay.classList.remove('active');
    }
}

// Scraper functionality
async function runScraper() {
    const button = document.getElementById('run-scraper-btn');
    const stopButton = document.getElementById('stop-scraper-btn');
    
    // Get form values from individual filter elements
    const areaFilter = document.getElementById('area-filter');
    const customAreaInput = document.getElementById('custom-area-input');
    const minPrice = document.getElementById('min-price');
    const maxPrice = document.getElementById('max-price');
    const bedroomsFilter = document.getElementById('bedrooms-filter');
    const laundryFilter = document.getElementById('laundry-filter');
    const petsFilter = document.getElementById('pets-filter');
    const outdoorFilter = document.getElementById('outdoor-filter');
    const daysFilter = document.getElementById('days-filter');
    const offmarketMonthStart = document.getElementById('offmarket-month-start');
    const offmarketMonthEnd = document.getElementById('offmarket-month-end');
    const byOwnerFilter = document.getElementById('by-owner-filter');
    const rentStabilizedFilter = document.getElementById('rent-stabilized-filter');
    
    // Build config object from filter values
    const config = {
        area: areaFilter ? (areaFilter.value === 'custom' ? customAreaInput?.value : areaFilter.value) : 'west village',
        min_price: minPrice?.value || '0',
        max_price: maxPrice?.value || '100000',
        bedrooms: bedroomsFilter?.value || 'all',
        laundry: laundryFilter?.value || 'all',
        pets: petsFilter?.value || 'all',
        outdoor: outdoorFilter?.value || 'all',
        days: daysFilter?.value || 'all',
        offmarket_month_start: offmarketMonthStart?.value || '1',
        offmarket_month_end: offmarketMonthEnd?.value || '12',
        by_owner: byOwnerFilter?.value || 'all',
        rent_stabilized: rentStabilizedFilter?.value || 'all'
    };

    
    // Update button states and show progress
    button.disabled = true;
    button.textContent = 'Starting...';
    stopButton.disabled = false;
    stopButton.style.display = 'inline-block';
    
    // Show progress bar immediately
    const progress = document.getElementById('scraper-progress');
    const progressBar = document.getElementById('progress-bar');
    const progressText = document.getElementById('progress-text');
    const progressDetail = document.getElementById('progress-detail');
    
    progress.style.display = 'block';
    progressBar.style.width = '0%';
    progressText.textContent = 'Starting...';
    progressDetail.textContent = 'Initializing scraper...';
    
    try {
        const response = await fetch('/api/run-scraper', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(config)
        });
        
        if (response.ok) {
            const result = await response.json();
            showToast('Scraper started successfully!', 3000);
            
            // Start polling for status updates
            pollScraperStatus({
                onRunning: (status) => {
                    button.textContent = `Running...`;
                },
                onDone: (status) => {
                    button.disabled = false;
                    button.textContent = 'Run Scraper';
                    stopButton.disabled = true;
                    stopButton.style.display = 'none';
                    showToast('Scraper completed successfully! Refreshing listings...', 5000);
                    
                            // Don't automatically reset area filter - let user choose
        // They may want to keep their area filter to see only their searched area
                    
                    // Force refresh listings immediately and after a delay to ensure data is loaded
                    fetchListings();
                    setTimeout(() => {
                        fetchListings();
                    }, 1000);
                    setTimeout(() => {
                        fetchListings();
                    }, 3000);
                },
                onStopped: (status) => {
                    button.disabled = false;
                    button.textContent = 'Run Scraper';
                    stopButton.disabled = true;
                    stopButton.style.display = 'none';
                    showToast('Scraper stopped.', 3000);
                    
                    // Refresh listings even if stopped
                    setTimeout(() => {
                        fetchListings();
                    }, 2000);
                },
                onError: (error) => {
                    button.disabled = false;
                    button.textContent = 'Run Scraper';
                    stopButton.disabled = true;
                    stopButton.style.display = 'none';
                    showToast('Scraper error: ' + error, 5000);
                }
            });
            
        } else {
            const error = await response.json();
            showToast('Error starting scraper: ' + (error.error || 'Unknown error'), 5000);
            
            // Reset button states and hide progress
            button.disabled = false;
            button.textContent = 'Run Scraper';
            stopButton.disabled = true;
            stopButton.style.display = 'none';
            progress.style.display = 'none';
        }
    } catch (error) {
        showToast('Network error: ' + error.message, 5000);
        
        // Reset button states and hide progress
        button.disabled = false;
        button.textContent = 'Run Scraper';
        stopButton.disabled = true;
        stopButton.style.display = 'none';
        progress.style.display = 'none';
    }
}

async function stopScraper() {
    const button = document.getElementById('stop-scraper-btn');
    const runButton = document.getElementById('run-scraper-btn');
    
    button.disabled = true;
    button.textContent = 'Stopping...';
    
    try {
        const response = await fetch('/api/stop-scraper', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            }
        });
        
        if (response.ok) {
            showToast('Stop signal sent to scraper', 3000);
            
            // Continue polling to detect when it actually stops
            pollScraperStatus({
                onStopped: (status) => {
                    button.disabled = true;
                    button.style.display = 'none';
                    runButton.disabled = false;
                    runButton.textContent = 'Run Scraper';
                    showToast('Scraper stopped successfully', 3000);
                },
                onDone: (status) => {
                    button.disabled = true;
                    button.style.display = 'none';
                    runButton.disabled = false;
                    runButton.textContent = 'Run Scraper';
                    showToast('Scraper completed before stop', 3000);
                },
                onError: (error) => {
                    button.disabled = true;
                    button.style.display = 'none';
                    runButton.disabled = false;
                    runButton.textContent = 'Run Scraper';
                }
            });
        } else {
            const error = await response.json();
            showToast('Error stopping scraper: ' + (error.error || 'Unknown error'), 5000);
            button.disabled = false;
            button.textContent = 'Stop Scraper';
        }
    } catch (error) {
        showToast('Network error: ' + error.message, 5000);
        button.disabled = false;
        button.textContent = 'Stop Scraper';
    }
}

// Polls /api/scraper-status every 3 seconds until status is 'idle' or 'error'
function pollScraperStatus({ onDone, onError, onRunning, onStopped }) {
    const button = document.getElementById('run-scraper-btn');
    const stopButton = document.getElementById('stop-scraper-btn');
    const progress = document.getElementById('scraper-progress');
    const progressBar = document.getElementById('progress-bar');
    const progressText = document.getElementById('progress-text');
    const progressDetail = document.getElementById('progress-detail');
    let polling = true;

    async function checkStatus() {
        try {
            const resp = await fetch('/api/scraper-status');
            const data = await resp.json();
            const status = data.status;
            
            if (status === 'running' || status === 'starting') {
                // Show progress elements and keep them visible
                progress.style.display = 'block';
                
                // Update progress information
                if (data.progress_percent !== undefined) {
                    progressBar.style.width = `${data.progress_percent}%`;
                    progressText.textContent = `${Math.round(data.progress_percent)}%`;
                } else {
                    progressBar.style.width = '0%';
                    progressText.textContent = status === 'starting' ? 'Starting...' : 'Processing...';
                }
                
                // Update progress detail message
                if (data.display_message) {
                    progressDetail.textContent = data.display_message;
                } else if (data.message) {
                    progressDetail.textContent = data.message;
                } else {
                    progressDetail.textContent = status === 'starting' ? 'Initializing scraper...' : 'Running...';
                }
                
                // Update button text
                button.textContent = status === 'starting' ? 'Starting...' : 'Running...';
                
                if (onRunning) onRunning(data);
                if (polling) setTimeout(checkStatus, 2000); // Check more frequently for better responsiveness
            } else if (status === 'completed' || status === 'idle') {
                polling = false;
    
                if (onDone) onDone();
                resetScraperButtons();
            } else if (status === 'stopped') {
                polling = false;
    
                if (onStopped) onStopped();
                resetScraperButtons();
            } else if (status === 'error') {
                polling = false;
                if (onError) onError(data.message || 'Unknown error');
                resetScraperButtons();
            } else {
                // Unknown status, treat as error
                polling = false;
                if (onError) onError('Unknown status: ' + status);
                resetScraperButtons();
            }
        } catch (e) {
            polling = false;
            if (onError) onError('Network error: ' + e.message);
            resetScraperButtons();
        }
    }
    
    function resetScraperButtons() {
        button.disabled = false;
        button.textContent = '🚀 RUN SCRAPER';
        button.style.display = 'inline-block';
        stopButton.style.display = 'none';
        stopButton.disabled = false;
        stopButton.textContent = '🛑 STOP';
        
        // Hide progress after a delay to show completion
        setTimeout(() => { 
            progress.style.display = 'none';
            progressBar.style.width = '0%';
            progressText.textContent = '';
            progressDetail.textContent = '';
        }, 3000);
    }
    
    checkStatus();
}

function panToListing(listingId) {
    if (!window.mapInitialized) return;
    
    const marker = window.mapMarkersById[listingId];
    if (marker) {
        map.setView(marker.getLatLng(), 16, { animate: true });
        marker.openPopup();
    }
}