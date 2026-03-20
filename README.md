# Bid Intelligence Platform

Scrapes public construction/infrastructure bid opportunities across the US and stores them in Supabase.

## Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
cp .env.example .env  # fill in your Supabase credentials
```

## Run the scraper

```bash
python scrapers/planetbids/scraper.py
```

Output is saved to `scrapers/planetbids/raw_output.json`.

To scrape all bids (not just the first 100), set `TARGET_BIDS = None` in `scraper.py`.

## How it works

PlanetBids portals are Ember.js SPAs backed by a JSON API at `api-external.prod.planetbids.com`.
The scraper loads the portal in a headless browser and captures API responses as they fire
during natural infinite-scroll pagination.

## Structure

```
scrapers/planetbids/   # PlanetBids portal scraper (infinite scroll, API intercept)
db/                    # Supabase client + upsert logic (coming next)
models/                # Pydantic schema models (coming next)
```