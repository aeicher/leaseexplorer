# StreetEasier - NYC Rental Finder

A web application that scrapes rental listings to help you find NYC apartments before they hit the market.

## Features

- 🏠 Real-time rental listing scraper
- 🗺️ Interactive map view with price markers
- 🔍 Advanced filtering (price, bedrooms, amenities, etc.)
- 📊 Rental market statistics
- 🎯 Early access to off-market listings
- 📱 Mobile-responsive design

## Setup

### Prerequisites

- Python 3.8+
- Chrome browser (for web scraping)

### Installation

1. Clone the repository:
```bash
git clone <your-repo-url>
cd leaseexplorer
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
```bash
export LOCATIONIQ_API_KEY="your_locationiq_api_key_here"
```

Get your free LocationIQ API key from: https://locationiq.com/

4. Run the application:
```bash
python server.py
```

5. Open http://localhost:5000 in your browser

## Usage

### Web Interface
- Visit the web interface to view and filter rental listings
- Use the map to see listing locations
- Apply filters to narrow down results

### Scraping New Data
Use the web interface buttons or run the scraper directly:

```bash
python scraper.py --area "west village" --min-price 2000 --max-price 5000
```

### Available Areas
- Manhattan neighborhoods (West Village, East Village, SoHo, etc.)
- Brooklyn neighborhoods (Williamsburg, DUMBO, Park Slope, etc.)
- Queens neighborhoods (LIC, Astoria, etc.)

## Configuration

### Environment Variables
- `LOCATIONIQ_API_KEY`: API key for geocoding addresses

### Scraper Options
- `--area`: Target neighborhood
- `--min-price`: Minimum rent price
- `--max-price`: Maximum rent price
- `--bedrooms`: Number of bedrooms
- `--workers`: Number of concurrent workers

## Legal Notice

This tool is for educational and personal use only. Please respect the terms of service of any websites you scrape and use responsibly.

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License

This project is licensed under the MIT License. 