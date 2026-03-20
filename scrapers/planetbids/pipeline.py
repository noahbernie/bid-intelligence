"""
PlanetBids → Supabase pipeline

Scrapes a single bid (all tabs) from a PlanetBids portal and writes
all data to Supabase. Use this to test the end-to-end pipeline on one bid
before running the full scraper.

Usage:
    python scrapers/planetbids/pipeline.py
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from playwright.async_api import async_playwright, Page

from scrapers.planetbids.mapper import (
    map_agency,
    map_job,
    map_job_details,
    map_job_line_items,
    map_job_media,
    map_companies_from_prospective_bidders,
    map_bids_from_prospective_bidders,
    map_bids_from_results,
    map_award,
)
from db.upsert import (
    upsert_source,
    upsert_agency,
    upsert_job,
    upsert_job_details,
    upsert_job_line_items,
    upsert_job_media,
    upsert_company,
    upsert_bids,
    upsert_award,
    create_scrape_log,
    update_scrape_log,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PORTAL_ID = 17950
TEST_BID_ID = 139043

# Maps our capture key → URL fragment to match
WATCHED_FRAGMENTS = {
    "agencies":             "/papi/agencies/",
    "bid-details":          "/papi/bid-details/",
    "bid-files":            "/papi/bid-downloadable-files",
    "bid-prospective-bidders": "/papi/bid-prospective-bidders",
    "bid-addenda":          "/papi/bid-addenda",
    "bid-results":          "/papi/bid-results",
    "bid-line-items":       "/papi/bid-line-items",
}

# Tab endpoints that require a live session but work via page.request after page load
# Format: (capture_key, path_template) — {bid_id} will be substituted
TAB_API_PATHS = [
    ("bid-prospective-bidders", "/papi/bid-prospective-bidders?bid_id={bid_id}"),
    ("bid-line-items",          "/papi/bid-line-items?bid_id={bid_id}"),
    ("bid-addenda",             "/papi/bid-addenda?bid_id={bid_id}"),
    ("bid-results",             "/papi/bid-results?bid_id={bid_id}"),
]


# ---------------------------------------------------------------------------
# Fetch — load the page once, capture session headers, fetch all tab APIs
# ---------------------------------------------------------------------------

async def fetch_bid_data(page: Page, portal_id: int, bid_id: int) -> dict:
    """Load the bid detail page, capture session headers, then fetch all tab APIs."""
    captured: dict[str, dict] = {}
    session_hdrs: dict[str, str] = {}

    async def on_request(req):
        if "/papi/bid-details/" in req.url and not session_hdrs:
            session_hdrs.update(req.headers)

    async def on_response(response):
        if "api-external.prod.planetbids.com/papi/" not in response.url:
            return
        if response.status != 200:
            return
        for key, fragment in WATCHED_FRAGMENTS.items():
            if fragment in response.url:
                try:
                    captured[key] = await response.json()
                    print(f"    Captured: {key}")
                except Exception:
                    pass

    page.on("request", on_request)
    page.on("response", on_response)

    base = f"https://vendors.planetbids.com/portal/{portal_id}/bo/bo-detail/{bid_id}"
    await page.goto(base, wait_until="domcontentloaded", timeout=30000)

    for _ in range(20):
        if "bid-details" in captured:
            break
        await page.wait_for_timeout(500)

    api_base = "https://api-external.prod.planetbids.com"
    for key, path_tpl in TAB_API_PATHS:
        url = f"{api_base}{path_tpl.format(bid_id=bid_id)}"
        try:
            resp = await page.request.get(url, headers=session_hdrs)
            if resp.ok:
                captured[key] = await resp.json()
                count = len(captured[key].get("data", []))
                print(f"    Captured: {key} ({count} records)")
        except Exception as e:
            print(f"    Warning {key}: {e}")

    page.remove_listener("request", on_request)
    page.remove_listener("response", on_response)

    def first_obj(data: dict) -> dict:
        d = data.get("data", {}) if isinstance(data, dict) else {}
        if isinstance(d, list):
            return d[0] if d else {}
        return d

    def first_list(data: dict) -> list:
        d = data.get("data", []) if isinstance(data, dict) else []
        return d if isinstance(d, list) else []

    return {
        "agency":     first_obj(captured.get("agencies", {})),
        "detail":     first_obj(captured.get("bid-details", {})),
        "line_items": first_list(captured.get("bid-line-items", {})),
        "files":      first_list(captured.get("bid-files", {})),
        "bidders":    first_list(captured.get("bid-prospective-bidders", {})),
        "addenda":    first_list(captured.get("bid-addenda", {})),
        "award":      first_list(captured.get("bid-results", {})),
    }


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

async def run_pipeline(portal_id: int, bid_id: int):
    from models.schema import Source, Company, ScrapeLog

    print(f"\n=== PlanetBids Pipeline: portal={portal_id}, bid={bid_id} ===\n")

    source_url = f"https://vendors.planetbids.com/portal/{portal_id}/bo/bo-detail/{bid_id}"

    # --- Source ---
    source_id = upsert_source(Source(
        name="PlanetBids",
        base_url="https://vendors.planetbids.com",
        scrape_frequency="weekly",
        is_active=True,
    ))
    print(f"  Source: PlanetBids → {source_id}")

    # --- Scrape Log (start) ---
    log_id = create_scrape_log(ScrapeLog(
        source_id=source_id,
        started_at=datetime.now(timezone.utc),
        status="running",
    ))
    print(f"  Scrape log: {log_id}\n")

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled"],
            )
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            )
            await context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )
            await context.route("**fonts.googleapis.com**", lambda r: r.abort())
            await context.route("**fonts.gstatic.com**", lambda r: r.abort())

            page = await context.new_page()
            raw = await fetch_bid_data(page, portal_id, bid_id)
            await browser.close()

        counts = {"jobs_new": 0, "companies_new": 0, "bids_new": 0, "awards_new": 0}

        # --- Agency ---
        agency_data = raw["agency"]
        if not agency_data:
            raise RuntimeError("No agency data captured.")

        print("Mapping & upserting agency...")
        agency_model = map_agency(agency_data, portal_id)
        agency_id = upsert_agency(agency_model)
        print(f"  Agency: {agency_model.name} → {agency_id}")

        # --- Job ---
        bid_detail_data = raw["detail"]
        if not bid_detail_data:
            raise RuntimeError("No bid detail captured.")

        print("Mapping & upserting job...")
        job_model = map_job(bid_detail_data, portal_id, agency_id)
        job_id = upsert_job(job_model)
        counts["jobs_new"] += 1
        print(f"  Job: {job_model.title[:60]} → {job_id}")

        # --- Job Details ---
        print("Mapping & upserting job details...")
        details_id = upsert_job_details(map_job_details(bid_detail_data, job_id, len(raw["addenda"])))
        print(f"  Details → {details_id}")

        # --- Line Items ---
        if raw["line_items"]:
            print(f"Upserting {len(raw['line_items'])} line items...")
            upsert_job_line_items(map_job_line_items(raw["line_items"], job_id))
            print("  Done.")
        else:
            print("  No line items.")

        # --- Documents ---
        if raw["files"]:
            print(f"Upserting {len(raw['files'])} documents...")
            upsert_job_media(map_job_media(raw["files"], job_id))
            print("  Done.")
        else:
            print("  No documents.")

        # --- Companies + Bids (prospective bidders list) ---
        company_id_map: dict = {}
        if raw["bidders"]:
            print(f"Upserting {len(raw['bidders'])} prospective bidders...")
            companies = map_companies_from_prospective_bidders(raw["bidders"])
            for b, c in zip(raw["bidders"], companies):
                cid = upsert_company(c)
                counts["companies_new"] += 1
                vendor_name = b.get("attributes", {}).get("vendorName")
                if vendor_name:
                    company_id_map[vendor_name] = cid
            print(f"  Done ({len(companies)} companies).")

            bids = map_bids_from_prospective_bidders(raw["bidders"], job_id, company_id_map, source_url)
            if bids:
                upsert_bids(bids)
                counts["bids_new"] += len(bids)
                print(f"  Stored {len(bids)} bid submissions (status=bidder).")
            else:
                print("  No bidder-status entries yet (bid may still be open).")
        else:
            print("  No prospective bidders.")

        # --- Bid Results (actual submitted bids with amounts/ranks) ---
        if raw["award"]:
            # Ensure companies from results are in the map
            for entry in raw["award"]:
                vname = entry.get("attributes", {}).get("vendorName")
                if vname and vname not in company_id_map:
                    company_id_map[vname] = upsert_company(Company(name=vname))

            result_bids = map_bids_from_results(raw["award"], job_id, company_id_map, source_url)
            if result_bids:
                upsert_bids(result_bids)
                counts["bids_new"] += len(result_bids)
                print(f"  Stored {len(result_bids)} bid results (with amounts/ranks).")

            # --- Award (winning bid) ---
            award_entry = raw["award"][0]
            a = award_entry.get("attributes", {})
            vendor_name = a.get("vendorName")
            award_date = a.get("awardedDate") or a.get("date")
            company_id = company_id_map.get(vendor_name)
            if vendor_name and not company_id:
                company_id = upsert_company(Company(name=vendor_name))

            print("Upserting award...")
            award_id = upsert_award(map_award(award_entry, job_id, company_id, award_date))
            counts["awards_new"] += 1
            print(f"  Award → {award_id}")
        else:
            print("  No award data (bid may still be open).")

        update_scrape_log(log_id,
            completed_at=datetime.now(timezone.utc),
            status="complete",
            jobs_found=1,
            **counts,
        )
        print(f"\n=== Done ===")
        print(f"  Agency: {agency_id}")
        print(f"  Job:    {job_id}")

    except Exception as e:
        update_scrape_log(log_id,
            completed_at=datetime.now(timezone.utc),
            status="failed",
            errors_json={"error": str(e)},
        )
        raise


if __name__ == "__main__":
    asyncio.run(run_pipeline(PORTAL_ID, TEST_BID_ID))