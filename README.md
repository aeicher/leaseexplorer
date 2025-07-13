# Lease Explorer - NYC rental finder

## What this does for you

**🏠 Gets there first** - Scrapes listings real-time so you see apartments before they hit the market, based off of their last off-market date  
**🗺️ Shows you where stuff actually is** - Interactive map with price markers 
**🔍 Filters that work** - Listed by owner (to try and avoid broker fees), price, bedrooms, amenities, pet-friendly - what matters to you  

## Getting started

### What you need first
- Python 3.8 or newer (check with `python --version`)
- Chrome browser (the scraper needs it to work its magic)

### Setting it up

**Step 1: Get the code**
```bash
git clone <your-repo-url>
cd leaseexplorer
```

**Step 2: Install the dependencies**
```bash
pip install -r requirements.txt
```

**Step 3: Get your API key**
You'll need a free LocationIQ API key to turn addresses into map coordinates. Grab one from https://locationiq.com/ (it's free and takes 30 seconds).

Then tell the app about it:
```bash
export LOCATIONIQ_API_KEY="your_locationiq_api_key_here"
```

**Step 4: Fire it up**
```bash
python server.py
```

**Step 5: Start hunting**
Open http://localhost:5000 and start finding your next apartment!

## How to use it

### The web interface
You'll see all the listings on a map with prices, and you can filter by whatever matters to you. Click on any listing to see details, photos, and contact info.

### Running the scraper manually
Want to target a specific area? Hit the scraper directly:

```bash
python scraper.py --area "west village" --min-price 2000 --max-price 5000
```

This is perfect for when you know exactly what neighborhood you want and don't want to wade through listings in areas you'd never live in.

### Areas that work
I've tested this on most of the popular neighborhoods:

**Manhattan:** West Village, East Village, SoHo, Tribeca, Chelsea, Hell's Kitchen, Upper East Side, Upper West Side, Financial District, and more

**Brooklyn:** Williamsburg, DUMBO, Park Slope, Bushwick, Bed-Stuy, Crown Heights, Prospect Heights, Brooklyn Heights

**Queens:** Long Island City, Astoria, Sunnyside, Forest Hills

If there's a neighborhood missing that you care about, let me know and I can add it.

## Customizing your search

### Environment settings
- `LOCATIONIQ_API_KEY`: Your geocoding API key (required for the map to work)

### Scraper options
When you run the scraper manually, you can get specific:

- `--area`: Which neighborhood to focus on
- `--min-price`: Don't waste time on places you can't afford
- `--max-price`: Don't waste time on places that are overpriced
- `--bedrooms`: Studios, 1BR, 2BR, whatever you need
- `--workers`: How many concurrent scrapers to run (more = faster, but be nice to the servers)

## The fine print

This tool is for personal use and educational purposes only. I built it to help people find apartments, not to overwhelm sites with requests. Please be respectful and don't use this to spam landlords or create fake inquiries.

## Want to contribute?

I'm always looking to make this better! Found a bug? Want to add a new neighborhood? Have an idea for a better filter?

1. Fork the repo
2. Create a branch for your feature
3. Make your changes
4. Test it out (especially if you're adding new scraping targets)
5. Submit a pull request

If you're planning something big, open an issue first so we can chat about it. I'm pretty responsive and love hearing ideas from other apartment hunters.

## A few tips from someone who's been there

- **Set up realistic filters** - Don't filter so aggressively that you miss out on some good listings
- **Have your documents ready** - Bank statements, references, application fee - have it all ready to go

## License

MIT License.