import os
import json
import datetime
import logging
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Depends, Query, UploadFile, File, Form, HTTPException
from pydantic import BaseModel
from google.cloud import bigquery

logger = logging.getLogger(__name__)

from src.services.bigquery_service import (
    get_bq_client, 
    require_bq_client,
    query_emea_delivery_data, 
    query_emea_pipeline_data,
    query_org_options,
    extract_ldap,
    fetch_projects_by_ldap,
    fetch_accounts_by_ldap
)
from src.services.manager_directory import get_manager_name

# Default organisational scope. The roster is everyone whose management chain
# contains this ldap, at any depth and in any region. Pass org_ldap=ALL to see
# the whole EMEA pool instead.
DEFAULT_ORG_LDAP = "guptaashutosh"


def _is_auth_error(exc: Exception) -> bool:
    """True when BigQuery rejected the caller's credential.

    Used to decide between "your session died, sign in again" (401) and "the
    query or the service broke" (fall back to the cached snapshot). Matching on
    the class name as well as the text keeps this working across google-auth /
    google-api-core versions without importing their private exception trees.
    """
    code = getattr(exc, "code", None)
    if code in (401, 403):
        return True
    if getattr(exc, "response", None) is not None:
        if getattr(exc.response, "status_code", None) in (401, 403):
            return True

    name = type(exc).__name__
    if name in {"Unauthorized", "Unauthenticated", "Forbidden", "RefreshError", "DefaultCredentialsError"}:
        return True

    text = str(exc).lower()
    return any(marker in text for marker in (
        "invalid authentication credentials",
        "invalid credentials",
        "access token",
        "invalid_grant",
        "unauthorized",
        "401",
        "permission denied",
    ))


def resolve_org_ldap(value: Optional[str]) -> Optional[str]:
    """Normalise the org-scope selection.

    Accepts a single ldap or a comma-separated list (the scope picker is a
    multi-select). Returns a comma-separated, de-duplicated, lowercased string
    that query_emea_delivery_data() splits again, or None for "no org filter".

      None / ''                -> the default org
      'ALL' (anywhere in list) -> None, i.e. the whole EMEA pool
      'a,b,a'                  -> 'a,b'
    """
    v = (value or "").strip()
    if not v:
        return DEFAULT_ORG_LDAP
    tokens = [t.strip().lower() for t in v.split(",")]
    tokens = [t for t in tokens if t]
    if not tokens:
        return DEFAULT_ORG_LDAP
    # "All EMEA" is a superset of any individual leader, so if it is ticked the
    # other selections cannot narrow anything and are ignored.
    if any(t == "all" for t in tokens):
        return None
    return ",".join(dict.fromkeys(tokens))

router = APIRouter(tags=["Dashboard"])

# Standard billable capacity per week.
# NOTE: no role in the source data is ever the bare string "Manager" - the
# previous `role == "Manager"` checks therefore never matched and every manager
# was measured against a 40h week.
#
# The reduced 16h capacity is for *people managers* who still carry billable
# work. In this dataset that is "Manager (Billable CON/SCE)" (21 people).
# It deliberately does NOT include job families that merely contain the word
# manager - "Technical Account Manager" (133), "Technical Success Account
# Manager" (10), "Cloud Program Manager" (7) - who are not reduced-capacity
# people managers. Matching those would shrink the capacity denominator by
# ~3,600 h/wk and materially overstate utilisation.
MANAGER_WEEKLY_CAPACITY = 16.0
STANDARD_WEEKLY_CAPACITY = 40.0


def standard_capacity(role: Optional[str]) -> float:
    """LAST-RESORT weekly capacity guess, used only when the source has no
    work_hours for this person (i.e. no row in the anchor week).

    This used to be the primary rule and gave 16h to any role starting with
    "manager". It was wrong. In this org every "Manager (Billable CON/SCE)"
    has work_hours = 40 in the source and is scheduled ~39h of real delivery,
    so the 16h denominator turned a normal 20h project into "100% allocated"
    and capped their visible load at 16h. Prefer capacity_from_row().
    """
    return MANAGER_WEEKLY_CAPACITY if str(role or "").strip().lower().startswith("manager") else STANDARD_WEEKLY_CAPACITY


def capacity_from_row(row: Dict[str, Any]) -> float:
    """This person's real weekly capacity, from the source.

    work_hours is the contracted availability for the week (40, or 37.5 for
    part-time). Falls back to the role guess only when it is missing, and
    rejects absurd values so a data glitch cannot silently divide by ~0.
    """
    try:
        wh = float(row.get("work_hours") or 0)
    except (ValueError, TypeError):
        wh = 0.0
    if 0 < wh <= 80:
        return wh
    return standard_capacity(row.get("role"))


def build_dashboard_payload(delivery_rows: List[Dict[str, Any]], pipeline_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Transforms delivery and pipeline records into the full frontend dashboard state."""
    resource_map: Dict[str, Dict[str, Any]] = {}
    customer_map: Dict[str, Dict[str, Any]] = {}
    today = datetime.date.today()

    total_scheduled_hrs = 0.0
    fully_staffed_count = 0
    bench_count = 0
    partial_count = 0

    hubs = {
        "EMEA": {"people": 0, "allocs": 0, "hours": 0.0, "staffed": 0, "partial": 0, "bench": 0},
        "GSD": {"people": 0, "allocs": 0, "hours": 0.0, "staffed": 0, "partial": 0, "bench": 0}
    }

    for row in delivery_rows:
        region_raw = (row.get("region") or "EMEA").upper()
        if "NORTHAM" in region_raw or "AMER" in region_raw:
            continue
        cc = str(row.get("cost_center") or "").strip().upper()
        cc_name = str(row.get("cost_center_name") or "").upper()
        # Use the region/hub the SQL actually derived. These were previously
        # hardcoded to "EMEA", which discarded the derivation and left the
        # GSD side of every hub split permanently empty.
        reg = row.get("region") or "Unknown"
        hub_key = "GSD" if str(row.get("hub") or "").upper() == "GSD" else "EMEA"

        res_name = row.get("resource_name") or "Unknown Engineer"
        res_id = row.get("resource_id") or res_name
        ldap = extract_ldap(row.get("ldap") or row.get("email"), fallback=res_name)
        mgr_raw = row.get("manager_ldap") or ""
        mgr_ldap = mgr_raw.split("@")[0].strip() if mgr_raw else ""
        mgr_name = row.get("manager_name") or get_manager_name(mgr_ldap)
        role = row.get("role") or "Consultant"
        practice = row.get("practice") or "Cloud Delivery"
        is_ooo = str(row.get("is_ooo") or "").lower() == "true"
        
        has_project = bool(row.get("project_id"))
        proj_id = str(row.get("project_id") or "")
        proj_name = row.get("project_name") or ""
        # Clean name for grouping: project_name has the assignment id appended.
        base_proj_name = row.get("base_project_name") or proj_name
        acc_name = (row.get("account_name") or "").strip()

        # Real people from the source. Empty string means genuinely unassigned;
        # these used to fall back to the invented strings "Delivery Lead" and
        # "PSO Lead", which rendered on screen as if they were real names.
        # The source stores the EM / engineering manager as an LDAP; the SQL
        # resolves it against the people directory and falls back to the raw
        # ldap when the person is not in the delivery resource table.
        em_name = (row.get("engagement_manager_name") or "").strip()
        em_ldap = (row.get("engagement_manager_ldap") or "").strip()
        engm_name = (row.get("pso_engineering_manager") or "").strip()
        engm_ldap = (row.get("pso_engineering_manager_ldap") or "").strip()
        pm_person = (row.get("project_manager") or "").strip()
        pm_person_ldap = (row.get("project_manager_ldap") or "").strip()
        # Account-level Delivery Executive, derived in SQL. "" for the 16 of 30
        # accounts with no DE signal at all - the UI must say "Unassigned".
        de_name = (row.get("delivery_executive") or "").strip()
        de_ldap = (row.get("delivery_executive_ldap") or "").strip()

        # Assignment-level hours ONLY. This must not fall back to the person's
        # total hours: doing so invented a phantom "Delivery Project" for every
        # unassigned person and made the bench look staffed.
        try:
            hrs = float(row.get("proj_scheduled_hours") or 0)
        except (ValueError, TypeError):
            hrs = 0.0

        # Real dates only. Previously these defaulted to 2026-01-01/2026-12-31,
        # which fabricated a 12-month runway for rows with no project.
        raw_start = row.get("project_start_date")
        raw_end = row.get("project_end_date")
        start_date = str(raw_start)[:10] if raw_start else ""
        end_date = str(raw_end)[:10] if raw_end else ""

        runway_days = None
        if end_date:
            try:
                end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
                runway_days = (end_dt - today).days
            except Exception:
                runway_days = None

        cap = capacity_from_row(row)
        # TRUE share of this person's week, not clamped to 100. Clamping here
        # meant that someone with three projects at 20h/20h/16h against a
        # (wrongly inferred) 16h capacity showed "100%" on all three, which
        # read as "fully allocated to each customer" in the portfolio.
        alloc_pct = round((hrs / cap) * 100) if cap > 0 else 0

        # Key by resource_id, not display name: two people who share a full
        # name would otherwise be merged into one record, silently dropping
        # the second person's capacity and hours from every KPI.
        res_key = res_id
        if res_key not in resource_map:
            resource_map[res_key] = {
                "id": res_id,
                "name": res_name,
                "ldap": ldap,
                "manager_ldap": mgr_ldap,
                "manager_name": mgr_name,
                "role": role,
                "cost_center": cc,
                "cost_center_name": row.get("cost_center_name") or "",
                "region": reg,
                "hub": hub_key,
                "languages": ["English"],
                "practice": practice,
                "skills": practice,
                "is_ooo": is_ooo,
                "capacity_hours": cap,
                "weekly_hours": float(row.get("scheduled_timecard_hours") or 0),
                "assignments": []
            }
        elif mgr_ldap and not resource_map[res_key].get("manager_ldap"):
            resource_map[res_key]["manager_ldap"] = mgr_ldap
            resource_map[res_key]["manager_name"] = mgr_name

        # Only a real, named project counts as an assignment.
        if has_project:
            resource_map[res_key]["assignments"].append({
                "project": proj_name or "Cloud Transformation",
                "account": acc_name or "Unassigned Account",
                "weekly_hours": hrs,
                "hours": hrs / 5.0,
                "start": start_date,
                "end": end_date,
                "allocation_pct": alloc_pct,
                "runway_days": runway_days,
                "type": "delivery"
            })

        if has_project and acc_name and acc_name != "Strategic Partner":
            if acc_name not in customer_map:
                customer_map[acc_name] = {
                    "account_name": acc_name,
                    # Account-level owner. There is NO "delivery executive"
                    # column anywhere in the source; engagement_manager is the
                    # closest real thing and, once scoped to this org's
                    # projects, resolves to exactly one person per account.
                    # Left empty when the source has none - the UI says
                    # "Unassigned". This used to be the literal hardcoded
                    # string "Delivery Executive" on all 30 accounts.
                    "engagement_manager": "",
                    "engagement_manager_ldap": "",
                    # Account owner shown in the UI. Derived - see the
                    # account_de CTE. "" means no DE signal exists for this
                    # account, which is true for 16 of 30.
                    "delivery_executive": "",
                    "delivery_executive_ldap": "",
                    "engineering_managers": [],
                    "total_hours": 0.0,
                    "projects": [],
                    "EMEA": [],
                    "GSD": []
                }

            acct = customer_map[acc_name]
            if em_name and not acct["engagement_manager"]:
                acct["engagement_manager"] = em_name
                acct["engagement_manager_ldap"] = em_ldap
            if de_name and not acct["delivery_executive"]:
                acct["delivery_executive"] = de_name
                acct["delivery_executive_ldap"] = de_ldap
            if engm_name and engm_name not in acct["engineering_managers"]:
                acct["engineering_managers"].append(engm_name)

            # ---- project level -------------------------------------------
            # Group on project_id, NOT on project_name: project_name carries an
            # appended assignment id, which splits 47 real projects into 153.
            proj = next((p for p in acct["projects"] if p["project_id"] == proj_id), None)
            if proj is None:
                proj = {
                    "project_id": proj_id,
                    "project_name": base_proj_name,
                    "project_manager": pm_person,          # "" = unassigned at source
                    "project_manager_ldap": pm_person_ldap,
                    "engagement_manager": em_name,
                    "engagement_manager_ldap": em_ldap,
                    "engineering_manager": engm_name,
                    "start": start_date,
                    "end": end_date,
                    "total_hours": 0.0,
                    "people": []
                }
                acct["projects"].append(proj)
            elif pm_person and not proj["project_manager"]:
                proj["project_manager"] = pm_person
                proj["project_manager_ldap"] = pm_person_ldap

            proj["total_hours"] = round(proj["total_hours"] + hrs, 1)
            pp = next((x for x in proj["people"] if x["ldap"] == ldap), None)
            if pp:
                pp["hours"] = round(pp["hours"] + hrs, 1)
                pp["allocation_pct"] = max(0, round((pp["hours"] / cap) * 100)) if cap > 0 else 0
            else:
                proj["people"].append({
                    "name": res_name,
                    "ldap": ldap,
                    "role": role,
                    "hub": hub_key,
                    "hours": round(hrs, 1),
                    "capacity_hours": cap,
                    "allocation_pct": max(0, round((hrs / cap) * 100)) if hrs > 0 and cap > 0 else 0
                })

            # ---- account level (unchanged shape, used by the summary pills) --
            if hub_key not in acct:
                acct[hub_key] = []

            existing_p = next((p for p in acct[hub_key] if p["name"] == res_name), None)
            if existing_p:
                existing_p["hours"] = round(existing_p.get("hours", 0.0) + hrs, 1)
                # "hours" here is a WEEKLY figure, divided by this person's real
                # weekly capacity from the source. NOT clamped to 100: this is
                # the share of their week that this ONE customer takes, and
                # clamping made every multi-customer person look 100% dedicated
                # to each of them.
                existing_p["allocation_pct"] = max(0, round((existing_p["hours"] / cap) * 100)) if cap > 0 else 0
            else:
                p_pct = max(0, round((hrs / cap) * 100)) if hrs > 0 and cap > 0 else 0
                acct[hub_key].append({
                    "name": res_name,
                    "ldap": ldap,
                    "role": role,
                    "hours": round(hrs, 1),
                    "capacity_hours": cap,
                    "allocation_pct": p_pct
                })
            acct["total_hours"] = round(acct["total_hours"] + hrs, 1)

    # resource_map is keyed by resource_id, so build a name index for the
    # portfolio backfill below.
    resource_by_name: Dict[str, Dict[str, Any]] = {}
    for _r in resource_map.values():
        resource_by_name.setdefault(_r["name"], _r)

    for c in customer_map.values():
        c["total_hours"] = round(c["total_hours"], 1)
        for hub_k in ["EMEA", "GSD"]:
            for p in c.get(hub_k, []):
                # Each person's own weekly capacity, taken from the source
                # (work_hours: 40, or 37.5 part-time). Previously these three
                # denominators were hardcoded to 40.0, then briefly to a role
                # guess that invented a 16h week for managers.
                p_cap = float(p.get("capacity_hours") or 40.0) or 40.0
                if p.get("allocation_pct") is None or p.get("allocation_pct") == 0:
                    pHrs = p.get("hours", 0.0)
                    # Not clamped to 100: this is the share of the person's week
                    # that THIS ONE account takes. Clamping made somebody split
                    # 20h/20h/16h across three projects read as "100% allocated"
                    # to every one of their customers.
                    if pHrs > 0:
                        p["allocation_pct"] = round((pHrs / p_cap) * 100)
                    elif p["name"] in resource_by_name:
                        r_match = resource_by_name[p["name"]]
                        r_cap = float(r_match.get("capacity_hours") or p_cap) or p_cap
                        acc_ass = [a for a in r_match.get("assignments", []) if a.get("account") == c["account_name"]]
                        if acc_ass:
                            sum_hrs = sum(a.get("weekly_hours", 0.0) for a in acc_ass)
                            p["hours"] = round(sum_hrs, 1)
                            p["allocation_pct"] = round((sum_hrs / r_cap) * 100)
                        elif r_match.get("scheduled_hours_uncapped", r_match.get("weekly_hours", 0.0)) > 0:
                            r_raw = float(r_match.get("scheduled_hours_uncapped") or r_match.get("weekly_hours") or 0.0)
                            p["allocation_pct"] = round((r_raw / r_cap) * 100)


    valid_customers = {}
    for acc_k, c_obj in customer_map.items():
        if acc_k and acc_k != "Strategic Partner" and (c_obj.get("total_hours", 0.0) > 0 or len(c_obj.get("EMEA", [])) > 0 or len(c_obj.get("GSD", [])) > 0):
            valid_customers[acc_k] = c_obj
    customer_map = valid_customers

    resources_list = list(resource_map.values())
    total_cap = 0.0
    for r in resources_list:
        std = r.get("capacity_hours") or standard_capacity(r.get("role"))
        r["capacity_hours"] = std
        total_cap += std

        # Resource's weekly delivery hours:
        r_hrs = float(r.get("weekly_hours") or 0.0)
        if r_hrs == 0.0 and r["assignments"]:
            r_hrs = sum(a["weekly_hours"] for a in r["assignments"])

        # Cap at capacity. 82 people were scheduled beyond 100% (max 205%),
        # which added ~760 phantom hours to the organisation-wide total and
        # pushed the utilisation KPI above what is actually deliverable.
        r["scheduled_hours_uncapped"] = round(r_hrs, 1)
        r["is_over_scheduled"] = r_hrs > std
        r["weekly_hours"] = round(min(r_hrs, std), 1)

        total_scheduled_hrs += r["weekly_hours"]
        pct = min(100, round((r["weekly_hours"] / std) * 100)) if std > 0 else 0
        r["allocation_pct"] = pct

        # Roll up under the person's real hub. This was hardcoded to "EMEA",
        # which zeroed out the GSD side of every hub breakdown.
        hub_key = "GSD" if str(r.get("hub") or "").upper() == "GSD" else "EMEA"
        hubs[hub_key]["people"] += 1
        hubs[hub_key]["allocs"] += len(r["assignments"])
        hubs[hub_key]["hours"] += r["weekly_hours"]

        if r["weekly_hours"] == 0 or pct == 0:
            bench_count += 1
            hubs[hub_key]["bench"] += 1
        elif pct >= 85:
            fully_staffed_count += 1
            hubs[hub_key]["staffed"] += 1
        else:
            partial_count += 1
            hubs[hub_key]["partial"] += 1

    total_scheduled_hrs = round(total_scheduled_hrs, 1)
    # No invented default: if there is no capacity there is no utilisation.
    overall_util = round((total_scheduled_hrs / total_cap) * 100, 1) if total_cap > 0 else 0.0

    # Helper: Map EMEA countries to canonical sub-regions
    def _map_emea_subregion(country: str, sub_reg_raw: str = "") -> str:
        c = (country or "").strip().lower()
        sr = (sub_reg_raw or "").strip().lower()
        if any(k in c or k in sr for k in ["germany", "deutschland", "austria", "switzerland", "dach"]):
            return "Central Europe (DACH)"
        if any(k in c or k in sr for k in ["united kingdom", "uk", "ireland", "britain"]):
            return "UK & Ireland"
        if any(k in c or k in sr for k in ["sweden", "norway", "denmark", "finland", "netherlands", "belgium", "luxembourg", "nordic", "benelux"]):
            return "Nordics & Benelux"
        return "Southern Europe & MEA"

    def _get_horizon_bucket(close_date_str: str, today_date: datetime.date):
        try:
            dt = datetime.datetime.strptime(str(close_date_str)[:10], "%Y-%m-%d").date()
            diff = (dt - today_date).days
        except Exception:
            return "Unknown Close Date", "Unknown", 6

        # A close date in the past is a slipped deal, not one closing this week.
        # Previously every negative diff fell through to "1 Week (Next 7d)",
        # which labelled thousands of past-dated opportunities as imminent.
        if diff < 0:
            return "Overdue (past close date)", "Overdue", 0

        if diff <= 7:
            return "1 Week (Next 7d)", "1 Week", 1
        elif diff <= 14:
            return "2 Weeks (8–14d)", "2 Weeks", 2
        elif diff <= 21:
            return "3 Weeks (15–21d)", "3 Weeks", 3
        elif diff <= 30:
            return "4 Weeks (22–30d)", "4 Weeks", 4
        else:
            return "8+ Weeks (31d+)", "8+ Weeks", 5

    workloads = []
    opportunities = []
    total_pipeline_val = 0.0
    total_booked_val = 0.0
    open_pipeline_val = 0.0
    unique_pipeline_accounts = set()
    open_opportunities_count = 0

    reg_capture = {
        "Central Europe (DACH)": {"counts": 0, "services_amount": 0.0, "total_acv": 0.0},
        "UK & Ireland": {"counts": 0, "services_amount": 0.0, "total_acv": 0.0},
        "Nordics & Benelux": {"counts": 0, "services_amount": 0.0, "total_acv": 0.0},
        "Southern Europe & MEA": {"counts": 0, "services_amount": 0.0, "total_acv": 0.0},
    }

    health_counts = {
        "Proposal & Negotiation": 0,
        "Tech Evaluation": 0,
        "Discovery & Qualify": 0,
        "Signed & Delivery": 0
    }

    strategy_counts = {
        "Delivery Center (GDC)": {"count": 0, "revenue": 0.0},
        "Field PSO Consulting": {"count": 0, "revenue": 0.0},
        "Specialized & Other": {"count": 0, "revenue": 0.0}
    }

    horizon_data = {
        "1 Week": {"name": "1 Week (Next 7d)", "bucket": "1 Week", "opportunity_count": 0, "total_acv": 0.0, "weighted_acv": 0.0, "unweighted_fte": 0.0, "demand_fte": 0.0, "accounts": []},
        "2 Weeks": {"name": "2 Weeks (8–14d)", "bucket": "2 Weeks", "opportunity_count": 0, "total_acv": 0.0, "weighted_acv": 0.0, "unweighted_fte": 0.0, "demand_fte": 0.0, "accounts": []},
        "3 Weeks": {"name": "3 Weeks (15–21d)", "bucket": "3 Weeks", "opportunity_count": 0, "total_acv": 0.0, "weighted_acv": 0.0, "unweighted_fte": 0.0, "demand_fte": 0.0, "accounts": []},
        "4 Weeks": {"name": "4 Weeks (22–30d)", "bucket": "4 Weeks", "opportunity_count": 0, "total_acv": 0.0, "weighted_acv": 0.0, "unweighted_fte": 0.0, "demand_fte": 0.0, "accounts": []},
        "8+ Weeks": {"name": "8+ Weeks (31d+)", "bucket": "8+ Weeks", "opportunity_count": 0, "total_acv": 0.0, "weighted_acv": 0.0, "unweighted_fte": 0.0, "demand_fte": 0.0, "accounts": []},
    }

    for p in pipeline_rows:
        val = float(p.get("total_sale_price_usd") or 0.0)
        total_pipeline_val += val
        # "Strategic Partner" was a placeholder for a missing account_name. It
        # read like a real customer, and it was counted in unique_customers.
        acc_name = (p.get("account_name") or "").strip()
        has_named_account = bool(acc_name)
        if not has_named_account:
            acc_name = "(Unnamed account)"
        stage_raw = str(p.get("stage_name") or "")
        stage_simp = str(p.get("stage_simplified") or "")
        c_date = str(p.get("close_date") or "")
        is_signed = "04" in stage_raw or "signed" in stage_simp.lower() or "won" in stage_raw.lower()
        
        if is_signed:
            total_booked_val += val
            health_counts["Signed & Delivery"] += 1
        elif "03" in stage_raw:
            health_counts["Proposal & Negotiation"] += 1
        elif "02" in stage_raw:
            health_counts["Tech Evaluation"] += 1
        else:
            health_counts["Discovery & Qualify"] += 1

        is_dc = str(p.get("dc_attached")).lower() == "true" or "delivery center" in str(p.get("offering") or "").lower()
        if is_dc:
            strategy_counts["Delivery Center (GDC)"]["count"] += 1
            strategy_counts["Delivery Center (GDC)"]["revenue"] += val
        elif any(k in str(p.get("offering") or "").lower() for k in ["consult", "standard", "deploy", "services"]):
            strategy_counts["Field PSO Consulting"]["count"] += 1
            strategy_counts["Field PSO Consulting"]["revenue"] += val
        else:
            strategy_counts["Specialized & Other"]["count"] += 1
            strategy_counts["Specialized & Other"]["revenue"] += val

        country = p.get("country") or "Unknown"
        sub_reg = _map_emea_subregion(country, p.get("project_sub_region"))
        reg_capture[sub_reg]["counts"] += 1
        reg_capture[sub_reg]["total_acv"] += val
        if is_dc:
            reg_capture[sub_reg]["services_amount"] += val

        prob = int(float(p.get("probability") or 0))
        if prob == 0:
            if "03" in stage_raw: prob = 50
            elif "02" in stage_raw: prob = 30
            elif is_signed: prob = 100
            else: prob = 20

        raw_cat = str(p.get("forecast_category") or "PIPELINE").upper()
        if "COMMIT" in raw_cat or is_signed:
            cat = "COMMIT"
        elif "BEST" in raw_cat or "UPSIDE" in raw_cat:
            cat = "UPSIDE"
        else:
            cat = "PIPELINE"

        _, horizon_code, _ = _get_horizon_bucket(c_date, today)

        c_hrs = float(p.get("consultant_hours_purchased") or 0.0) + float(p.get("sce_hours_purchased") or 0.0)
        if 0 < c_hrs <= 2000:
            req_fte = max(1.0, round(c_hrs / 160.0, 1))
        elif val > 0:
            req_fte = max(1.0, min(12.0, round(val / 75_000.0, 1)))
        else:
            req_fte = 2.0
        weighted_fte = round(req_fte * (prob / 100.0), 1)

        is_open = not is_signed
        if is_open:
            open_pipeline_val += val
            open_opportunities_count += 1
            # Only count real, named accounts toward the distinct customer KPI.
            if has_named_account:
                unique_pipeline_accounts.add(acc_name)

            if c_date:
                try:
                    dt = datetime.datetime.strptime(c_date[:10], "%Y-%m-%d").date()
                    if dt >= today and horizon_code in horizon_data:
                        h = horizon_data[horizon_code]
                        h["opportunity_count"] += 1
                        h["total_acv"] += val
                        h["weighted_acv"] += val * (prob / 100.0)
                        h["unweighted_fte"] = round(h["unweighted_fte"] + req_fte, 1)
                        h["demand_fte"] = round(h["demand_fte"] + weighted_fte, 1)
                        if acc_name not in h["accounts"] and len(h["accounts"]) < 4:
                            h["accounts"].append(acc_name)
                except Exception:
                    pass

        status_badge = "Delivered" if is_signed else ("Missing Date" if not c_date else "In-Flight")
        workloads.append({
            "workload_id": p.get("workload_id") or p.get("opportunity_id") or "WL-100",
            "workload_solution": p.get("solution") or "Cloud Infrastructure",
            "account_name": acc_name,
            "region": "EMEA",
            "sub_region": f"{sub_reg} ({country})" if country != "Unknown" else sub_reg,
            "status": status_badge,
            # pso_pipeline carries no delivery-side owner, so there is nobody
            # real to name here. This used to say "PSO Delivery Lead", which
            # rendered as though it were a person. The frontend skips the badge
            # when this is empty.
            "program_manager": "",
            "implementation_led_by": "Delivery Center (GDC)" if is_dc else "Field PSO",
            "partner": p.get("offering") or "Google Cloud PSO",
            "services_revenue": f"${round(val):,}" if val > 0 else "$0",
            "schedule_date": c_date[:10] if c_date else "2026-12-31"
        })

        opportunities.append({
            "opportunity_id": p.get("opportunity_id") or "006...",
            "opportunity_name": p.get("opp_name") or "Cloud Modernization Engagement",
            "account_name": acc_name,
            "horizon_bucket": horizon_code,
            "close_date": c_date[:10] if c_date else "2026-12-31",
            "category": cat,
            "stage": stage_raw or "02 - Solution Dev",
            "probability_pct": prob,
            "required_resources": req_fte,
            "weighted_demand_fte": weighted_fte,
            "acv_formatted": f"${(val / 1_000_000):.2f}M" if val >= 1_000_000 else (f"${int(val / 1000):,}k" if val > 0 else "$0"),
            "weighted_acv_formatted": f"${(val * (prob / 100.0) / 1_000_000):.2f}M" if val >= 1_000_000 else (f"${int(val * (prob / 100.0) / 1000):,}k" if val > 0 else "$0"),
            "workload_solution": p.get("solution") or "Cloud Solutions",
            "industry": country if country != "Unknown" else "EMEA"
        })

    # Available engineering supply comparison (Bench + Roll-offs from delivery)
    roll_offs_by_horizon = {"1 Week": 0, "2 Weeks": 0, "3 Weeks": 0, "4 Weeks": 0, "8+ Weeks": 0}
    for r in resources_list:
        max_end = ""
        for a in r.get("assignments", []):
            a_end = a.get("end", "")
            if a_end and a_end > max_end:
                max_end = a_end
        if max_end:
            try:
                dt = datetime.datetime.strptime(max_end[:10], "%Y-%m-%d").date()
                diff = (dt - today).days
                if 0 <= diff <= 7: roll_offs_by_horizon["1 Week"] += 1
                elif 8 <= diff <= 14: roll_offs_by_horizon["2 Weeks"] += 1
                elif 15 <= diff <= 21: roll_offs_by_horizon["3 Weeks"] += 1
                elif 22 <= diff <= 30: roll_offs_by_horizon["4 Weeks"] += 1
                elif diff >= 31: roll_offs_by_horizon["8+ Weeks"] += 1
            except Exception:
                pass

    cum_supply = bench_count
    buckets_list = []
    total_forward_weighted_acv = 0.0
    total_forward_demand_fte = 0.0

    for b_key in ["1 Week", "2 Weeks", "3 Weeks", "4 Weeks", "8+ Weeks"]:
        b_info = horizon_data[b_key]
        cum_supply += roll_offs_by_horizon.get(b_key, 0)
        net_bal = round(cum_supply - b_info["demand_fte"], 1)
        total_forward_weighted_acv += b_info["weighted_acv"]
        total_forward_demand_fte += b_info["demand_fte"]

        buckets_list.append({
            "name": b_info["name"],
            "bucket": b_info["bucket"],
            "opportunity_count": b_info["opportunity_count"],
            "total_acv": b_info["total_acv"],
            "total_acv_formatted": f"${(b_info['total_acv'] / 1_000_000):.2f}M",
            "weighted_acv": b_info["weighted_acv"],
            "weighted_acv_formatted": f"${(b_info['weighted_acv'] / 1_000_000):.2f}M",
            "available_supply_fte": cum_supply,
            "unweighted_fte": b_info["unweighted_fte"],
            "demand_fte": b_info["demand_fte"],
            "net_balance_fte": net_bal,
            "status": "SURPLUS" if net_bal >= 0 else "DEFICIT",
            "accounts": b_info["accounts"]
        })

    demand_timeline = {
        "total_forward_weighted_acv": f"${(total_forward_weighted_acv / 1_000_000):.2f}M",
        "total_forward_demand_fte": round(total_forward_demand_fte, 1),
        "buckets": buckets_list
    }

    tot_reg_val = sum(r["total_acv"] for r in reg_capture.values()) or 1.0
    regional_capture_list = [
        {
            "region": reg_name,
            "counts": r["counts"],
            "services_amount": f"${(r['services_amount'] / 1_000_000):.2f}M",
            "total_acv": f"${(r['total_acv'] / 1_000_000):.2f}M",
            "pct": round((r["total_acv"] / tot_reg_val) * 100, 1)
        }
        for reg_name, r in reg_capture.items()
    ]

    tot_health = sum(health_counts.values()) or 1
    health_colors = {
        "Proposal & Negotiation": "bg-emerald-500",
        "Tech Evaluation": "bg-indigo-500",
        "Discovery & Qualify": "bg-amber-400",
        "Signed & Delivery": "bg-purple-600"
    }
    workload_health_list = [
        {
            "status": stage_k,
            "count": cnt,
            "pct": round((cnt / tot_health) * 100, 1),
            "color": health_colors.get(stage_k, "bg-slate-500")
        }
        for stage_k, cnt in health_counts.items()
    ]

    tot_strat_cnt = sum(s["count"] for s in strategy_counts.values()) or 1
    strat_colors = {
        "Delivery Center (GDC)": "bg-indigo-600",
        "Field PSO Consulting": "bg-emerald-500",
        "Specialized & Other": "bg-amber-500"
    }
    delivery_strategy_list = [
        {
            "led_by": strat_k,
            "count": s_val["count"],
            "revenue": f"${(s_val['revenue'] / 1_000_000):.2f}M",
            "pct": round((s_val["count"] / tot_strat_cnt) * 100, 1),
            "color": strat_colors.get(strat_k, "bg-slate-500")
        }
        for strat_k, s_val in strategy_counts.items()
    ]

    customers_list = list(customer_map.values())

    # Never invent revenue. If there is no booked value in the period, say so
    # rather than displaying a fraction of pipeline as though it were real.
    # (This previously rendered `total_pipeline_val * 0.25` as PS Engagement.)
    gtm_summary = {
        "ps_engagement_formatted": f"${(total_booked_val / 1_000_000):.2f}M" if total_booked_val > 0 else "—",
        "pipeline_acv_formatted": f"${(open_pipeline_val / 1_000_000):.2f}M" if open_pipeline_val > 0 else "—",
        # These count distinct *pipeline* accounts / *open* opportunities. The
        # previous `or` fallbacks silently substituted a different metric
        # (delivery customers / all pipeline rows) when the real count was 0.
        "unique_customers": len(unique_pipeline_accounts),
        "total_workloads": len(workloads),
        "active_opportunities": open_opportunities_count
    }

    # The single week all delivery figures describe.
    reporting_week = ""
    for row in delivery_rows:
        wk = str(row.get("week_ending") or "")[:10]
        if wk and wk > reporting_week:
            reporting_week = wk

    return {
        "kpis": {
            "team_size": len(resources_list),
            "customers": len(customers_list),
            "fully_staffed": fully_staffed_count,
            "bench_count": bench_count,
            "partial_count": partial_count,
            "allocated_hrs": int(total_scheduled_hrs),
            "utilization_pct": overall_util
        },
        "hubs": hubs,
        "resources": resources_list,
        "customerPortfolio": customers_list,
        "gtm": {
            "summary": gtm_summary,
            "regional_capture": regional_capture_list,
            "workload_health": workload_health_list,
            "delivery_strategy": delivery_strategy_list,
            "demand_timeline": demand_timeline,
            "workloads": workloads,
            "opportunities": opportunities
        },
        # Previously "raw_data": delivery_rows shipped ~1,300 raw rows to every
        # browser purely so the UI could derive one date. Send the date.
        "reporting_week": reporting_week,
        "delivery_row_count": len(delivery_rows),
        "pipeline_row_count": len(pipeline_rows)
    }

def get_fallback_payload(start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
    """Loads the cached snapshot of BigQuery delivery & pipeline data, filtered by date range.

    IMPORTANT: this is *stale* data. The returned payload is tagged with
    data_source="cached_snapshot" so the UI can say so out loud.
    """
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    delivery_file = os.path.join(base_dir, "data", "emea_delivery.json")
    pipeline_file = os.path.join(base_dir, "data", "emea_pipeline.json")
    delivery_rows = []
    pipeline_rows = []
    snapshot_mtime = None
    if os.path.exists(delivery_file):
        try:
            with open(delivery_file, "r") as f:
                delivery_rows = json.load(f)
            snapshot_mtime = datetime.datetime.fromtimestamp(
                os.path.getmtime(delivery_file)
            ).strftime("%Y-%m-%d %H:%M")
        except Exception as e:
            logger.error(f"Error loading delivery fallback json: {e}")
    if os.path.exists(pipeline_file):
        try:
            with open(pipeline_file, "r") as f:
                pipeline_rows = json.load(f)
        except Exception as e:
            logger.error(f"Error loading pipeline fallback json: {e}")

    # Always apply a date window. Previously this whole block was skipped when
    # no explicit dates were supplied - which is the default page load - so the
    # snapshot was served completely unfiltered and included opportunities with
    # close dates as far out as 2035.
    clean_start = str(start_date).strip() if (start_date and str(start_date).strip()) else None
    clean_end = str(end_date).strip() if (end_date and str(end_date).strip()) else None

    today = datetime.date.today()
    # Mirror the defaults used by the live BigQuery queries.
    effective_start = clean_start or (today - datetime.timedelta(days=90)).strftime("%Y-%m-%d")
    effective_end = clean_end or (today + datetime.timedelta(days=14)).strftime("%Y-%m-%d")

    delivery_rows = [
        r for r in delivery_rows
        if str(r.get("project_start_date") or "2020-01-01")[:10] <= effective_end
        and (str(r.get("week_ending") or r.get("project_end_date") or "2099-01-01")[:10] >= effective_start)
    ]

    # Pipeline looks further forward than delivery (deals close in the future).
    pipe_max_end = max(effective_end, (today + datetime.timedelta(days=365)).strftime("%Y-%m-%d"))
    pipeline_rows = [
        p for p in pipeline_rows
        if effective_start <= str(p.get("close_date") or "")[:10] <= pipe_max_end
    ]

    payload = build_dashboard_payload(delivery_rows, pipeline_rows)
    payload["data_source"] = "cached_snapshot"
    payload["is_stale"] = True
    payload["snapshot_taken_at"] = snapshot_mtime
    # The snapshot was captured before manager_hierarchy_user_names was
    # selected, so there is nothing to filter on. Say so instead of labelling
    # an unscoped roster with the requested manager's name.
    payload["org_ldap"] = "ALL"
    payload["org_name"] = "All EMEA"
    payload["org_scope_applied"] = False
    payload["pipeline_scope"] = "EMEA-wide"
    return payload

@router.get("/dashboard/data")
@router.get("/data")
def get_dashboard_data(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    org_ldap: Optional[str] = Query(None),
    # Strict: an anonymous caller gets 401, never the cached snapshot.
    bq_client: bigquery.Client = Depends(require_bq_client),
):
    clean_start = str(start_date).strip() if (start_date and str(start_date).strip()) else None
    clean_end = str(end_date).strip() if (end_date and str(end_date).strip()) else None
    org = resolve_org_ldap(org_ldap)
    try:
        delivery_rows = query_emea_delivery_data(bq_client, start_date=clean_start, end_date=clean_end, org_ldap=org)
        # NOTE: pso_pipeline carries no manager hierarchy, so the pipeline stays
        # EMEA-wide even when the roster is scoped to one org. The frontend
        # labels this so the supply-vs-demand gap is not read as like-for-like.
        pipeline_rows = query_emea_pipeline_data(bq_client, start_date=clean_start, end_date=clean_end)
        payload = build_dashboard_payload(delivery_rows, pipeline_rows)
        payload["data_source"] = "bigquery"
        payload["is_stale"] = False
        payload["org_ldap"] = org or "ALL"
        payload["org_name"] = get_manager_name(org) if org else "All EMEA"
        payload["org_scope_applied"] = bool(org)
        payload["pipeline_scope"] = "EMEA-wide"
        return payload
    except Exception as e:
        # An auth failure must NOT degrade to the cached snapshot. Otherwise the
        # 401 gate above is cosmetic: any non-empty string in the header gets
        # past it, BigQuery rejects the credential here, and the caller is
        # handed the stale roster anyway. Only genuine query/infra failures may
        # fall back.
        if _is_auth_error(e):
            logger.warning(f"Rejecting request: BigQuery refused the caller's token. {e}")
            raise HTTPException(
                status_code=401,
                detail="Your Google session is no longer valid. Sign in again.",
            )
        # This used to fail silently: the UI kept saying "Live Delivery Pool"
        # while rendering a stale snapshot. The payload now carries the reason.
        logger.warning(f"BigQuery failed, using fallback data. Error: {e}")
        payload = get_fallback_payload(start_date=clean_start, end_date=clean_end)
        payload["data_source_error"] = str(e)[:300]
        return payload

@router.get("/dashboard/org-options")
@router.get("/org-options")
def get_org_options(bq_client: bigquery.Client = Depends(require_bq_client)):
    """Managers available for the org-scope dropdown, largest org first.

    Derived live from manager_hierarchy_user_names so new managers appear
    without a code change.
    """
    try:
        options = query_org_options(bq_client, root_ldap=DEFAULT_ORG_LDAP)
        # The root must always be selectable even if the headcount threshold or
        # a data gap dropped it, otherwise the default scope is unreachable.
        if not any(o.get("ldap") == DEFAULT_ORG_LDAP for o in options):
            options.insert(0, {
                "ldap": DEFAULT_ORG_LDAP,
                "name": get_manager_name(DEFAULT_ORG_LDAP) or DEFAULT_ORG_LDAP,
                "headcount": 0,
            })
        return {"status": "ok", "default": DEFAULT_ORG_LDAP, "options": options}
    except Exception as e:
        logger.warning(f"org-options query failed: {e}")
        # Never block the UI on this: fall back to just the default entry.
        return {
            "status": "fallback",
            "default": DEFAULT_ORG_LDAP,
            "error": str(e)[:300],
            "options": [{
                "ldap": DEFAULT_ORG_LDAP,
                "name": get_manager_name(DEFAULT_ORG_LDAP) or DEFAULT_ORG_LDAP,
                "headcount": 0,
            }],
        }


@router.post("/refresh-bq")
def refresh_bq(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    org_ldap: Optional[str] = Query(None),
    # Strict: returns the same full payload as /data, so it needs the same gate.
    bq_client: bigquery.Client = Depends(require_bq_client),
):
    clean_start = str(start_date).strip() if (start_date and str(start_date).strip()) else None
    clean_end = str(end_date).strip() if (end_date and str(end_date).strip()) else None
    org = resolve_org_ldap(org_ldap)
    try:
        delivery_rows = query_emea_delivery_data(bq_client, start_date=clean_start, end_date=clean_end, org_ldap=org)
        pipeline_rows = query_emea_pipeline_data(bq_client, start_date=clean_start, end_date=clean_end)
        payload = build_dashboard_payload(delivery_rows, pipeline_rows)
        payload["data_source"] = "bigquery"
        payload["is_stale"] = False
        payload["org_ldap"] = org or "ALL"
        payload["org_name"] = get_manager_name(org) if org else "All EMEA"
        payload["org_scope_applied"] = bool(org)
        payload["pipeline_scope"] = "EMEA-wide"
        return {"status": "ok", "data": payload}
    except Exception as e:
        if _is_auth_error(e):
            logger.warning(f"Rejecting refresh: BigQuery refused the caller's token. {e}")
            raise HTTPException(
                status_code=401,
                detail="Your Google session is no longer valid. Sign in again.",
            )
        logger.warning(f"BigQuery refresh failed: {e}")
        payload = get_fallback_payload(start_date=clean_start, end_date=clean_end)
        payload["data_source_error"] = str(e)[:300]
        # status must stay "fallback" so the UI does not claim a successful sync.
        return {"status": "fallback", "error": str(e)[:300], "data": payload}

@router.get("/users/{ldap}/projects")
def get_user_projects(
    ldap: str,
    bq_client: bigquery.Client = Depends(require_bq_client),
):
    """Fetches active/scheduled projects for an LDAP (or email)."""
    try:
        return fetch_projects_by_ldap(ldap, bq_client)
    except Exception as e:
        logger.error(f"Failed to fetch projects for ldap {ldap}: {e}")
        return []

@router.get("/users/{ldap}/accounts")
def get_user_accounts(
    ldap: str,
    bq_client: bigquery.Client = Depends(require_bq_client),
):
    """Fetches unique accounts for an LDAP (or email)."""
    try:
        return fetch_accounts_by_ldap(ldap, bq_client)
    except Exception as e:
        logger.error(f"Failed to fetch accounts for ldap {ldap}: {e}")
        return []

@router.post("/upload")
async def upload_staffing_sheet(
    file: UploadFile = File(...),
    sheet_name: Optional[str] = Form(None),
    # Returns the full dashboard payload, so it needs the same gate as /data.
    bq_client: bigquery.Client = Depends(require_bq_client),
):
    """
    Accepts staffing sheets (.csv, .xlsx, .xls) to validate or sync data.
    Returns the refreshed dashboard payload.
    """
    filename = file.filename or "uploaded_staffing_sheet"
    contents = await file.read()
    logger.info(f"Received uploaded staffing sheet: {filename}, size: {len(contents)} bytes, label: {sheet_name}")
    try:
        delivery_rows = query_emea_delivery_data(bq_client)
        pipeline_rows = query_emea_pipeline_data(bq_client)
        return build_dashboard_payload(delivery_rows, pipeline_rows)
    except Exception as e:
        logger.warning(f"BigQuery query failed after upload, using fallback: {e}")
        return get_fallback_payload()

class AgentChatRequest(BaseModel):
    message: str
    active_tab: Optional[str] = "General"

@router.post("/agent/chat")
@router.post("/chat")
def agent_chat_endpoint(
    req: AgentChatRequest,
    # The assistant answers questions about the roster, so it is just another
    # read of the same data and needs the same gate. The frontend previously
    # sent no token here at all, which meant every answer came from the stale
    # snapshot rather than the live org-scoped numbers on screen.
    bq_client: bigquery.Client = Depends(require_bq_client),
):
    """Answers user inquiries regarding staffing, workloads, bench, pipeline, and customer accounts."""
    msg = (req.message or "").strip().lower()

    # Previously this ALWAYS read the stale snapshot, so the assistant could
    # contradict the dashboard the user was looking at. Use the same source.
    try:
        delivery_rows = query_emea_delivery_data(bq_client)
        pipeline_rows = query_emea_pipeline_data(bq_client)
        payload = build_dashboard_payload(delivery_rows, pipeline_rows)
        payload["data_source"] = "bigquery"
    except Exception as e:
        logger.warning(f"Agent chat: BigQuery failed, using fallback. Error: {e}")
        payload = get_fallback_payload()

    kpis = payload.get("kpis", {})
    resources = payload.get("resources", [])
    customers = payload.get("customerPortfolio", [])
    gtm = payload.get("gtm", {})

    def _alloc_pct(r: Dict[str, Any]) -> float:
        cap = r.get("capacity_hours") or standard_capacity(r.get("role"))
        hrs = float(r.get("weekly_hours") or 0)
        return (hrs / cap) * 100 if cap > 0 else 0.0

    # 1. Bench / Availability / Capacity queries
    if any(k in msg for k in ["bench", "available", "free cap", "unassigned", "availability"]):
        # Must match the KPI definitions in build_dashboard_payload exactly:
        # bench = 0%, fully staffed = >=85%, partial = everything between.
        # The old rule here was `0 < weekly_hours < 35`, which reported 201
        # partial engineers while the KPI tile said 183.
        bench_list = [r for r in resources if float(r.get("weekly_hours") or 0) == 0]
        partial_list = [r for r in resources if float(r.get("weekly_hours") or 0) > 0 and _alloc_pct(r) < 85]

        reply_lines = [
            "### 🛡️ Available Bench & Capacity Overview",
            f"Currently, there are **{len(bench_list)} engineers** with 100% bench capacity, and **{len(partial_list)} engineers** on partial allocation (<85%).",
            "",
            "**Key Available Talent:**"
        ]
        for r in bench_list[:5]:
            reply_lines.append(f"* **{r.get('name')}** (`@{r.get('ldap')}`) — {r.get('role')}, *{r.get('practice')}*")
        
        if len(bench_list) > 5:
            reply_lines.append(f"*...and {len(bench_list) - 5} more engineers on the bench.*")
        
        actions = [
            {"label": "📊 View Staffing Board", "tab": "board"},
            {"label": "📋 View Bench in Roster", "tab": "roster", "filter": "Bench"}
        ]
        return {"reply": "\n".join(reply_lines), "actions": actions}

    # 2. Individual person lookup (e.g. Yashwant, Rani, etc.)
    # Matching is deliberately strict: the previous rule matched on any first
    # name longer than 3 characters appearing anywhere in the message, so a
    # question like "what is our pipeline in Milan?" could be hijacked by an
    # engineer called Mila. Require a whole-word match on the full name or ldap.
    import re as _re

    def _mentions(needle: str) -> bool:
        if not needle:
            return False
        return _re.search(r"(?<![a-z0-9])" + _re.escape(needle) + r"(?![a-z0-9])", msg) is not None

    matched_person = None
    for r in resources:
        r_name = (r.get("name") or "").lower().strip()
        r_ldap = (r.get("ldap") or "").lower().strip()
        if _mentions(r_name) or _mentions(r_ldap):
            matched_person = r
            break

    if matched_person:
        r = matched_person
        hrs = r.get("weekly_hours", 0)
        std = r.get("capacity_hours") or standard_capacity(r.get("role"))
        pct = min(100, round((hrs / std) * 100)) if std > 0 else 0
        ass = r.get("assignments", [])
        
        status_str = "🟢 Fully Staffed" if pct >= 85 else ("🟡 Partial Allocation" if pct > 0 else "🔴 100% Bench")
        lines = [
            f"### 👤 Resource Profile: **{r.get('name')}** (`@{r.get('ldap')}`)",
            f"* **Role**: {r.get('role')}",
            f"* **Practice / Domain**: {r.get('practice') or 'Cloud Delivery'}",
            f"* **Region / Hub**: {r.get('region', 'EMEA')} ({r.get('hub', 'EMEA')})",
            f"* **Scheduled Hours**: **{hrs} hrs/wk** ({pct}% capacity) — {status_str}",
            "",
            f"**Active Assignments ({len(ass)}):**"
        ]
        if ass:
            for a in ass:
                lines.append(f"* **{a.get('project')}** ({a.get('account')}) — {a.get('weekly_hours', 40)} hrs/wk (Roll-off: `{a.get('end', 'Active')}`, {a.get('runway_days', 90)}d runway)")
        else:
            lines.append("*No active client delivery assignments (Available for staffing).*")
            
        actions = [
            {"label": f"👤 View {r.get('name')} in Roster", "tab": "roster", "filter": r.get('ldap') or r.get('name')},
            {"label": "⏱️ Check Runway", "tab": "timeline"}
        ]
        return {"reply": "\n".join(lines), "actions": actions}

    # 3. GTM, Pipeline, ACV, Sales, Workloads
    if any(k in msg for k in ["pipeline", "gtm", "sales", "acv", "workload", "revenue", "deals", "opportunity"]):
        gtm_summary = gtm.get("summary", {})
        regional = gtm.get("regional_capture", [])
        lines = [
            "### 💼 EMEA GTM & Sales Pipeline Insights",
            f"* **PS Engagement Value**: **{gtm_summary.get('ps_engagement_formatted') or '—'}**",
            f"* **Total Pipeline ACV**: **{gtm_summary.get('pipeline_acv_formatted') or '—'}**",
            f"* **Active Workloads**: **{gtm_summary.get('total_workloads', 0)}** across **{gtm_summary.get('unique_customers', 0)}** accounts",
            f"* **Live Opportunities**: **{gtm_summary.get('active_opportunities', 0)}** deals in flight",
            "",
            "**Regional Revenue Capture:**"
        ]
        for reg in regional:
            lines.append(f"* **{reg.get('region')}**: {reg.get('services_amount')} PS ({reg.get('total_acv')} ACV)")
            
        actions = [
            {"label": "💼 Open GTM Pipeline", "tab": "gtm"},
            {"label": "📊 View Workloads Table", "tab": "gtm", "subtab": "workloads"}
        ]
        return {"reply": "\n".join(lines), "actions": actions}

    # 4. Customer Portfolio / Accounts
    if any(k in msg for k in ["customer", "account", "client", "portfolio"]):
        top_custs = sorted(customers, key=lambda c: c.get("total_hours", 0), reverse=True)[:5]
        lines = [
            f"### 🏢 Customer Portfolio ({len(customers)} Accounts)",
            f"The EMEA delivery team is currently deployed across **{len(customers)} strategic client accounts**.",
            "",
            "**Top Client Engagements by Volume:**"
        ]
        for c in top_custs:
            # Was: "PM: {program_manager} | DE: {delivery_executive}" - both of
            # those were placeholder strings, so the assistant confidently
            # reported "Delivery Lead" and "Delivery Executive" as real people.
            em = c.get("engagement_manager") or "Unassigned"
            projs = c.get("projects") or []
            pms = sorted({p.get("project_manager") for p in projs if p.get("project_manager")})
            pm_txt = ", ".join(pms) if pms else "Unassigned"
            lines.append(
                f"* **{c.get('account_name')}** — **{c.get('total_hours')} hrs/wk** "
                f"across {len(projs)} project(s) | EM: {em} | PM: {pm_txt}"
            )
            
        actions = [
            {"label": "🏢 Open Customer Portfolio", "tab": "portfolio"},
            {"label": "📈 Executive Overview", "tab": "exec"}
        ]
        return {"reply": "\n".join(lines), "actions": actions}

    # 5. Roll-off / Runway
    if any(k in msg for k in ["roll-off", "rolloff", "runway", "ending", "timeline", "expir"]):
        lines = [
            "### ⏱️ Roll-off & Runway Status",
            "Assignments are tracked into 6 granular roll-off horizons (Current Week 0-7d through Month 2+).",
            "* Focus on upcoming roll-offs to extend contracts or reallocate talent early.",
            "* Use the Timeline tab to inspect individual engagement burn-down rates."
        ]
        actions = [
            {"label": "⏱️ Open Roll-off Timeline", "tab": "timeline"},
            {"label": "⚡ View Executive Spotlight", "tab": "exec"}
        ]
        return {"reply": "\n".join(lines), "actions": actions}

    # 6. Utilization / General KPIs
    if any(k in msg for k in ["utilization", "rate", "kpi", "hours", "team size", "headcount", "staffed"]):
        lines = [
            "### 📊 EMEA Delivery Operational KPIs",
            f"* **Total Delivery Team**: **{kpis.get('team_size', len(resources))} engineers**",
            f"* **Overall Utilization**: **{kpis.get('utilization_pct', 60.0)}%**",
            f"* **Total Scheduled Delivery Hours**: **{kpis.get('allocated_hrs', 0):,} hrs/wk**",
            f"* **Fully Staffed (≥85%)**: **{kpis.get('fully_staffed', 0)}**",
            f"* **Partial Allocation (<85%)**: **{kpis.get('partial_count', 0)}**",
            f"* **Available Bench**: **{kpis.get('bench_count', 0)}**"
        ]
        actions = [
            {"label": "📊 Executive Dashboard", "tab": "exec"},
            {"label": "🎯 Capacity & Skill Matrix", "tab": "matrix"}
        ]
        return {"reply": "\n".join(lines), "actions": actions}

    # 7. Default smart overview
    lines = [
        "### ✨ EMEA 360 AI Assistant",
        f"I have analyzed our live EMEA delivery organization (**{len(resources)} engineers**, **{len(customers)} client accounts**, and **{gtm.get('summary', {}).get('total_workloads', 0)} workloads**).",
        "",
        "You can ask me questions like:",
        "* *'Who is currently on the bench?'*",
        "* *'Show details for Rani Singh or Yashwant Mahawar'*",
        "* *'What are our top customer accounts?'*",
        "* *'What is our total sales pipeline and ACV?'*",
        "* *'What is our current team utilization rate?'*"
    ]
    actions = [
        {"label": "👉 Executive Dashboard", "tab": "exec"},
        {"label": "📋 Staffing Roster", "tab": "roster"},
        {"label": "💼 GTM Pipeline", "tab": "gtm"}
    ]
    return {"reply": "\n".join(lines), "actions": actions}


