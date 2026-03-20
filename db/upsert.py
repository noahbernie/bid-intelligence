"""
Idempotent upsert functions for each Supabase table.

Each function takes a Pydantic model (or list), serialises it, and upserts
using the natural conflict key so re-running never creates duplicates.
Returns the row's UUID so callers can chain foreign keys.
"""

from __future__ import annotations
from typing import Optional

from db.client import get_client
from models.schema import Agency, Job, JobDetails, JobLineItem, JobMedia, Company, Award, ScrapeLog


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _dump(model) -> dict:
    """Serialise a Pydantic model, dropping None values and the id field."""
    data = model.model_dump(mode="json", exclude_none=True)
    data.pop("id", None)
    return data


def _get_id(response) -> str:
    return response.data[0]["id"]


# ---------------------------------------------------------------------------
# Agency
# ---------------------------------------------------------------------------

def upsert_agency(agency: Agency) -> str:
    """Upsert by external_portal_id. Returns the agency UUID."""
    db = get_client()
    data = _dump(agency)
    res = (
        db.table("agencies")
        .upsert(data, on_conflict="external_portal_id")
        .execute()
    )
    return _get_id(res)


# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------

def upsert_job(job: Job) -> str:
    """Upsert by source_url (unique per bid). Returns the job UUID."""
    db = get_client()
    data = _dump(job)
    res = (
        db.table("jobs")
        .upsert(data, on_conflict="source_url")
        .execute()
    )
    return _get_id(res)


# ---------------------------------------------------------------------------
# JobDetails
# ---------------------------------------------------------------------------

def upsert_job_details(details: JobDetails) -> str:
    """Upsert by job_id (one row per job). Returns the details UUID."""
    db = get_client()
    data = _dump(details)
    res = (
        db.table("job_details")
        .upsert(data, on_conflict="job_id")
        .execute()
    )
    return _get_id(res)


# ---------------------------------------------------------------------------
# JobLineItem
# ---------------------------------------------------------------------------

def upsert_job_line_items(items: list[JobLineItem]) -> list[str]:
    """Upsert a list of line items. Returns list of UUIDs."""
    if not items:
        return []
    db = get_client()
    rows = [_dump(i) for i in items]
    res = (
        db.table("job_line_items")
        .upsert(rows, on_conflict="job_id,item_number")
        .execute()
    )
    return [r["id"] for r in res.data]


# ---------------------------------------------------------------------------
# JobMedia
# ---------------------------------------------------------------------------

def upsert_job_media(media_list: list[JobMedia]) -> list[str]:
    """Upsert a list of media/attachments. Returns list of UUIDs."""
    if not media_list:
        return []
    db = get_client()
    rows = [_dump(m) for m in media_list]
    res = (
        db.table("job_media")
        .upsert(rows, on_conflict="job_id,file_url")
        .execute()
    )
    return [r["id"] for r in res.data]


# ---------------------------------------------------------------------------
# Company
# ---------------------------------------------------------------------------

def upsert_company(company: Company) -> str:
    """Upsert by name + location_state. Returns the company UUID."""
    db = get_client()
    data = _dump(company)
    res = (
        db.table("companies")
        .upsert(data, on_conflict="name,location_state")
        .execute()
    )
    return _get_id(res)


# ---------------------------------------------------------------------------
# Award
# ---------------------------------------------------------------------------

def upsert_award(award: Award) -> str:
    """Upsert by job_id (one award per job). Returns the award UUID."""
    db = get_client()
    data = _dump(award)
    res = (
        db.table("awards")
        .upsert(data, on_conflict="job_id")
        .execute()
    )
    return _get_id(res)


# ---------------------------------------------------------------------------
# ScrapeLog
# ---------------------------------------------------------------------------

def create_scrape_log(log: ScrapeLog) -> str:
    """Insert a new scrape log row. Returns the log UUID."""
    db = get_client()
    data = _dump(log)
    res = db.table("scrape_logs").insert(data).execute()
    return _get_id(res)


def update_scrape_log(log_id: str, **fields) -> None:
    """Patch an existing scrape log (e.g. mark complete, update counts)."""
    db = get_client()
    db.table("scrape_logs").update(fields).eq("id", log_id).execute()