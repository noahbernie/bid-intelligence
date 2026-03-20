"""
PlanetBids Portal Scraper

Loads the portal in a headless browser, then scrolls through the infinite-scroll
bid list to trigger all API pages. Captures each API response as it fires.

Usage:
    python scrapers/planetbids/scraper.py
"""

import asyncio
import json
from pathlib import Path
from playwright.async_api import async_playwright

PORTAL_ID = 17950
PORTAL_URL = f"https://vendors.planetbids.com/portal/{PORTAL_ID}/bo/bo-search"
OUTPUT_FILE = Path(__file__).parent / "raw_output.json"
TARGET_BIDS = 100   # set to None to scrape all 1510


async def scrape():
    all_bids = []
    total_in_portal = 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 900},
        )
        # Block fonts — they hang indefinitely
        await context.route("**fonts.googleapis.com**", lambda r: r.abort())
        await context.route("**fonts.gstatic.com**", lambda r: r.abort())

        page = await context.new_page()

        async def on_response(response):
            nonlocal total_in_portal
            if "papi/bids" in response.url and response.status == 200:
                body = await response.json()
                bids = body.get("data", [])
                all_bids.extend(bids)
                total_in_portal = body["meta"]["totalBids"]
                page_url_suffix = response.url.split("page=")[-1][:3]
                print(f"  Page {page_url_suffix.split('&')[0]}: +{len(bids)} bids → total {len(all_bids)} / {total_in_portal}")

        page.on("response", on_response)

        print(f"Loading portal {PORTAL_ID}...")
        await page.goto(PORTAL_URL, wait_until="domcontentloaded", timeout=30000)

        try:
            await page.wait_for_selector("table", timeout=15000)
        except Exception:
            print("  Warning: table not found, page may not have loaded correctly.")

        # Give the initial page load time to fire
        await page.wait_for_timeout(2000)

        target = TARGET_BIDS or total_in_portal or 9999
        scroll_attempts_without_new = 0

        print(f"Scrolling to collect {target} bids...")
        while len(all_bids) < target:
            prev_count = len(all_bids)

            # Scroll the page body and any scrollable containers in the list
            await page.evaluate("""
                () => {
                    window.scrollTo(0, document.body.scrollHeight);
                    document.querySelectorAll(
                        '[class*="scroll"], [class*="list"], [class*="result"], tbody, [class*="container"]'
                    ).forEach(el => { el.scrollTop = el.scrollHeight; });
                }
            """)

            # Wait for the next API page to load
            await page.wait_for_timeout(2500)

            if len(all_bids) == prev_count:
                scroll_attempts_without_new += 1
                if scroll_attempts_without_new >= 3:
                    print("  No new bids after 3 scrolls — reached end of list.")
                    break
            else:
                scroll_attempts_without_new = 0

        await browser.close()

    print(f"\nDone. Collected {len(all_bids)} bids (portal total: {total_in_portal}).")
    output = {"portal_id": PORTAL_ID, "total_in_portal": total_in_portal, "bids": all_bids}
    OUTPUT_FILE.write_text(json.dumps(output, indent=2))
    print(f"Saved to {OUTPUT_FILE}")

    print("\n--- SAMPLE (first 5) ---")
    for bid in all_bids[:5]:
        a = bid.get("attributes", {})
        print(f"  [{a.get('stageStr')}] {a.get('title')} | Due: {str(a.get('bidDueDate', ''))[:10]}")


if __name__ == "__main__":
    asyncio.run(scrape())