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

from models.schema import Agency, Job, JobDetails, JobLineItem, JobMedia, Company, Award

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


def map_companies_from_prospective_bidders(bidders_data: list) -> list[Company]:
    result = []
    for b in bidders_data:
        a = b.get("attributes", {})
        if not a.get("vendorName"):
            continue
        result.append(Company(
            name=a.get("vendorName", ""),
            email=a.get("vendorEmail"),
            phone=a.get("phone"),
            location_city=a.get("city"),
            location_state=str(a.get("state", "")),
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