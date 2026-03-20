# Bid Intelligence Platform

Scrapes public construction/infrastructure bid opportunities across the US and stores them in Supabase.

## Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
playwright install chromium
cp .env.example .env  # then fill in your Supabase credentials
```

## Run the scraper

```bash
python scrapers/planetbids/scraper.py
```

## Structure

```
scrapers/planetbids/   # PlanetBids portal scraper
db/                    # Supabase client + upsert logic
models/                # Pydantic schema models
validation/            # Validation pipeline
```# bid-intelligence
