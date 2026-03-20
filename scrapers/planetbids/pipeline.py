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
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from playwright.async_api import async_playwright, Page

from scrapers.planetbids.mapper import (
    map_agency,
    map_job,
    map_job_details,
    map_job_line_items,
    map_job_media,
    map_companies_from_prospective_bidders,
    map_award,
)
from db.upsert import (
    upsert_agency,
    upsert_job,
    upsert_job_details,
    upsert_job_line_items,
    upsert_job_media,
    upsert_company,
    upsert_award,
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

# Ember SPA tab routes (appended to the bo-detail URL)
TAB_ROUTES = [
    "/documents",
    "/plan-holders",
    "/addenda",
    "/bid-results",
    "/line-items",
]


# ---------------------------------------------------------------------------
# Fetch — load the page once, then SPA-navigate each tab
# ---------------------------------------------------------------------------

async def fetch_bid_data(page: Page, portal_id: int, bid_id: int) -> dict:
    """
    Load the bid detail page, wait for the Ember app to fully render,
    then click each tab to trigger its lazy-loaded API call.
    """
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
    print(f"\n=== PlanetBids Pipeline: portal={portal_id}, bid={bid_id} ===\n")

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

    # --- Agency ---
    agency_data = raw["agency"]
    if not agency_data:
        print("ERROR: No agency data captured.")
        return

    print("\nMapping & upserting agency...")
    agency_model = map_agency(agency_data, portal_id)
    agency_id = upsert_agency(agency_model)
    print(f"  Agency: {agency_model.name} → {agency_id}")

    # --- Job ---
    bid_detail_data = raw["detail"]
    if not bid_detail_data:
        print("ERROR: No bid detail captured.")
        return

    print("Mapping & upserting job...")
    job_model = map_job(bid_detail_data, portal_id, agency_id)
    job_id = upsert_job(job_model)
    print(f"  Job: {job_model.title[:60]} → {job_id}")

    # --- Job Details ---
    print("Mapping & upserting job details...")
    details_model = map_job_details(bid_detail_data, job_id, len(raw["addenda"]))
    details_id = upsert_job_details(details_model)
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

    # --- Prospective Bidders / Companies ---
    if raw["bidders"]:
        print(f"Upserting {len(raw['bidders'])} prospective bidders...")
        companies = map_companies_from_prospective_bidders(raw["bidders"])
        for c in companies:
            upsert_company(c)
        print(f"  Done ({len(companies)} companies).")
    else:
        print("  No prospective bidders.")

    # --- Award ---
    if raw["award"]:
        award_entry = raw["award"][0]
        a = award_entry.get("attributes", {})
        vendor_name = a.get("vendorName")
        award_date = a.get("awardedDate") or a.get("date")

        company_id = None
        if vendor_name:
            from models.schema import Company
            company_id = upsert_company(Company(name=vendor_name))

        print("Upserting award...")
        award_id = upsert_award(map_award(award_entry, job_id, company_id, award_date))
        print(f"  Award → {award_id}")
    else:
        print("  No award data (bid may still be open).")

    print(f"\n=== Done ===")
    print(f"  Agency: {agency_id}")
    print(f"  Job:    {job_id}")


if __name__ == "__main__":
    asyncio.run(run_pipeline(PORTAL_ID, TEST_BID_ID))