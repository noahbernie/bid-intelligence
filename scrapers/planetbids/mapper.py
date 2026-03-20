"""
PlanetBids API response → Pydantic schema models

Each function takes raw JSON from a specific API endpoint and returns
the corresponding model(s) ready to upsert into Supabase.
"""

from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from models.schema import Agency, Job, JobDetails, JobLineItem, JobMedia, Company, Award, Bid

PORTAL_URL_BASE = "https://vendors.planetbids.com/portal"
BID_TYPE_MAP = {1: "Bid", 2: "RFI", 4: "RFP", 8: "RFQ", 16: "RFQual", 32: "RFO"}
STAGE_MAP = {1: "open", 2: "open", 3: "open", 4: "closed", 5: "cancelled", 6: "awarded"}


def _parse_dt(value: str | None) -> Optional[datetime]:
    if not value:
        return None
    try:
        # PlanetBids format: "2026-03-23 14:00:00.000"
        return datetime.strptime(value[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def map_agency(agency_data: dict, portal_id: int) -> Agency:
    a = agency_data.get("attributes", {})
    return Agency(
        name=a.get("companyName", ""),
        website=a.get("companyWebsite"),
        location_state=a.get("companyStateName"),
        external_portal_id=str(portal_id),
    )


def map_job(bid_detail: dict, portal_id: int, agency_id: str) -> Job:
    a = bid_detail.get("attributes", {})
    bid_id = a.get("bidId")
    stage_id = a.get("stageId", 0)

    return Job(
        job_owner=agency_id,
        title=a.get("title", ""),
        scope_of_work=a.get("scope") or a.get("details") or None,
        estimated_value=a.get("estimatedValue") or None,
        posted_date=_parse_dt(a.get("issueDate")),
        bid_due_date=_parse_dt(a.get("bidDueDate")),
        project_start_date=_parse_dt(a.get("startDate")) if a.get("startDate") else None,
        location_city=a.get("city"),
        location_state="California",   # derived from agency — will generalise later
        status=STAGE_MAP.get(stage_id, "open"),
        source_url=f"{PORTAL_URL_BASE}/{portal_id}/bo/bo-detail/{bid_id}",
        source_platform="planetbids",
        invitation_num=a.get("invitationNum"),
        bid_type=BID_TYPE_MAP.get(a.get("bidType", 0)),
        category_codes=a.get("categoryIds"),
    )


def map_job_details(bid_detail: dict, job_id: str, addenda_count: int = 0) -> JobDetails:
    a = bid_detail.get("attributes", {})

    # Parse contact — PlanetBids stores "Name 619-555-1234" in one field
    contact_raw = a.get("contactNameAndPhone", "")
    contact_name, contact_phone = _split_contact(contact_raw)

    return JobDetails(
        job_id=job_id,
        agency_contact_name=contact_name,
        agency_contact_email=a.get("contactEmail"),
        agency_contact_phone=contact_phone,
        prebid_meeting_required=bool(a.get("preBidMeeting")),
        prebid_meeting_date=_parse_dt(a.get("preBidMeetingDate")),
        prebid_meeting_location=a.get("preBidMtgLocation") or None,
        bond_required=bool(a.get("bidBond", 0)),
        bond_pct=float(a.get("bidBond")) if a.get("bidBond") else None,
        liquidated_damages_per_day=float(a.get("liquidatedDamages")) if a.get("liquidatedDamages") else None,
        addenda_count=addenda_count,
        bid_platform="planetbids",
        bid_submission_method=a.get("bidResponseFormatStr"),
        parsed_fields_json={
            "awardType": a.get("awardType"),
            "onlineQAndA": a.get("onlineQAndA"),
            "onlineQAndACutoffDate": a.get("onlineQAndACutoffDate"),
            "notes": a.get("notes"),
            "cooperativeBid": a.get("cooperativeBid"),
            "piggybackable": a.get("piggybackable"),
        },
        scraped_at=datetime.now(timezone.utc),
    )


def map_job_line_items(line_items_data: list, job_id: str) -> list[JobLineItem]:
    result = []
    for item in line_items_data:
        a = item.get("attributes", {})
        result.append(JobLineItem(
            job_id=job_id,
            item_number=str(a.get("itemOrdinal", "")),
            item_code=a.get("itemCode"),
            description=a.get("itemDesc"),
            quantity=a.get("quantity"),
            unit_of_measure=a.get("unitOfMeasure"),
            agency_unit_price=a.get("unitPrice"),
            section=a.get("itemGroup"),
        ))
    return result


def map_job_media(files_data: list, job_id: str) -> list[JobMedia]:
    result = []
    for f in files_data:
        a = f.get("attributes", {})
        server_path = a.get("serverFullPath", "")
        server_file = a.get("serverFilename", "")
        # Construct full download URL
        file_url = f"https://{server_path}{server_file}" if server_path and server_file else None
        result.append(JobMedia(
            job_id=job_id,
            file_name=a.get("filename") or a.get("fileTitle"),
            file_url=file_url,
            file_type=_guess_file_type(a.get("filename", "")),
            uploaded_at=_parse_dt(a.get("uploadedDate")),
        ))
    return result


# PlanetBids returns state as an integer ID; map to 2-letter abbreviation
_STATE_ID_MAP = {
    1: "AL", 2: "AK", 3: "AZ", 4: "AR", 5: "CA", 6: "CO", 7: "CT", 8: "DE",
    9: "FL", 10: "GA", 11: "HI", 12: "ID", 13: "IL", 14: "IN", 15: "IA",
    16: "KS", 17: "KY", 18: "LA", 19: "ME", 20: "MD", 21: "MA", 22: "MI",
    23: "MN", 24: "MS", 25: "MO", 26: "MT", 27: "NE", 28: "NV", 29: "NH",
    30: "NJ", 31: "NM", 32: "NY", 33: "NC", 34: "ND", 35: "OH", 36: "OK",
    37: "OR", 38: "PA", 39: "RI", 40: "SC", 41: "SD", 42: "TN", 43: "TX",
    44: "UT", 45: "VT", 46: "VA", 47: "WA", 48: "WV", 49: "WI", 50: "WY",
    51: "DC", 52: "CA",  # 52 observed for CA in PlanetBids data
}

# PlanetBids prospective-bidder status integer → label
_BIDDER_STATUS_MAP = {0: "plan_holder", 1: "bidder", 2: "bidder"}


def map_companies_from_prospective_bidders(bidders_data: list) -> list[Company]:
    result = []
    for b in bidders_data:
        a = b.get("attributes", {})
        if not a.get("vendorName"):
            continue
        state_raw = a.get("state")
        state = _STATE_ID_MAP.get(state_raw) if isinstance(state_raw, int) else state_raw
        result.append(Company(
            name=a.get("vendorName", ""),
            email=a.get("vendorEmail"),
            phone=a.get("phone"),
            location_city=a.get("city"),
            location_state=state,
        ))
    return result


def map_bids_from_prospective_bidders(bidders_data: list, job_id: str, company_id_map: dict, source_url: str = "") -> list[Bid]:
    """
    Stores all prospective bidders as bid records.
    Only includes entries whose status maps to 'bidder'.
    company_id_map: {vendorName -> company_id} built after upserting companies.
    """
    result = []
    for b in bidders_data:
        a = b.get("attributes", {})
        status_int = a.get("status")
        status_label = _BIDDER_STATUS_MAP.get(status_int, "plan_holder")
        if status_label != "bidder":
            continue
        vendor_name = a.get("vendorName")
        company_id = company_id_map.get(vendor_name)
        if not company_id:
            continue
        result.append(Bid(
            job_id=job_id,
            company_id=company_id,
            contact_name=a.get("contactName"),
            contact_email=a.get("vendorEmail"),
            bidder_external_id=str(a["vendorId"]) if a.get("vendorId") else None,
            source="planetbids",
            source_url=source_url,
        ))
    return result


def map_bids_from_results(results_data: list, job_id: str, company_id_map: dict, source_url: str = "") -> list[Bid]:
    """Map bid-results entries (actual submitted bids with amounts/ranks) into Bid records."""
    result = []
    for r in results_data:
        a = r.get("attributes", {})
        vendor_name = a.get("vendorName")
        company_id = company_id_map.get(vendor_name)
        if not company_id:
            continue
        result.append(Bid(
            job_id=job_id,
            company_id=company_id,
            contact_name=a.get("contactName"),
            contact_email=a.get("vendorEmail"),
            bidder_external_id=str(a["vendorId"]) if a.get("vendorId") else None,
            total_bid_amount=a.get("amount"),
            rank=a.get("rank"),
            working_days_bid=a.get("workingDays"),
            submitted_date=_parse_dt(a.get("submittedDate") or a.get("date")),
            pct_subcontracted=a.get("pctSubcontracted"),
            subcontracted_dollar_amount=a.get("subcontractedAmount"),
            source="planetbids",
            source_url=source_url,
        ))
    return result


def map_award(award_data: dict, job_id: str, company_id: str | None, award_date: str | None) -> Award:
    a = award_data.get("attributes", {})
    return Award(
        job_id=job_id,
        company_id=company_id,
        awarded_amount=a.get("amount"),
        awarded_date=_parse_dt(award_date),
        status="awarded",
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _split_contact(raw: str) -> tuple[str | None, str | None]:
    """Split 'Janet Polite 619-236-7017' into (name, phone)."""
    if not raw:
        return None, None
    parts = raw.rsplit(" ", 1)
    if len(parts) == 2 and any(c.isdigit() for c in parts[1]):
        return parts[0].strip(), parts[1].strip()
    return raw.strip(), None


def _guess_file_type(filename: str) -> str | None:
    if not filename:
        return None
    ext = filename.rsplit(".", 1)[-1].lower()
    return ext if ext else None