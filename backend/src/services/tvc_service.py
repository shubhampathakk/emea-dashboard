"""Live TVC roster, read from the Core EMEA Active TVCs Google Sheet.

WHY A SHEET AND NOT BIGQUERY
----------------------------
There is no assigned-TVC information anywhere in concord-prod.service_cloudbi.
Verified exhaustively: `scheduled_vs_actual_utilization` (all 73 columns) and
`weekly_schedules` (all 17) carry no worker-type flag; no role in the utilization
table contains "Flex"; `weekly_resource_requests.held_resource_name` is populated
on 56 people out of 277k rows; `pso_timecards_schedules_margin` has 0 rows; and
`projects.dc_flex_*_hours` - which would be exactly right - is 0.0 on every one
of this org's projects. The `DC Flex` marker only exists on the *demand* side
(requests), never on the supply side. So the sheet is the only source of truth
for who is actually staffed, and it is maintained by hand and refreshed weekly.

AUTHENTICATION
--------------
Cloud Run's metadata server mints the runtime service account a token scoped to
`cloud-platform`, and the Sheets API rejects that scope outright (it wants
API_DRIVE / API_WISE). So a plain `google.auth.default()` token does NOT work,
even though the service account has been granted view access to the sheet.

Two strategies are therefore attempted in order:

  1. Ask ADC directly for a sheets-scoped token. This succeeds for a service
     account key or a user credential that already holds the Drive scope.
  2. Self-impersonation via the IAM Credentials API: the runtime service account
     calls generateAccessToken on *itself* asking for the sheets scope. This is
     the documented way to obtain a Drive/Sheets-scoped token on Cloud Run and
     requires roles/iam.serviceAccountTokenCreator on itself.

Whichever succeeds is remembered, so the fallback costs one extra call once.

FAILURE POLICY
--------------
A sheet outage must never take the dashboard down. Every failure is caught, the
last good result is served if one exists, and the payload carries an explicit
status so the UI can say "TVC data unavailable" instead of silently showing
zero TVCs everywhere - which would read as "no contractors on this project" and
be actively misleading.
"""
import datetime
import json
import logging
import os
import re
import threading
import time
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

SHEET_ID = os.getenv("TVC_SHEET_ID", "14yn3f2fK12yhQXdTVnd-cgtvAu5RJT4xv_pjMQFKHoY")
SHEET_TAB = os.getenv("TVC_SHEET_TAB", "Sep 03")
SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets.readonly"
CACHE_TTL_SECONDS = int(os.getenv("TVC_CACHE_TTL", "600"))
HTTP_TIMEOUT = 20

# A project cell can name more than one project. The separator is a comma
# surrounded by whitespace. Splitting on a bare "," is WRONG and was verified to
# corrupt real project names that legitimately contain one:
#   "Merck & Co., Inc. [Merck] GCP Environment Enhancements CON x 1711260"
#   "Booking.com PS|Landing Zone (Create, Manage, Migrate) CON x 318990 [CR]"
#   "[BIF] ICA Gruppen AB  Looker health check - PSO CON X 45,516.00"
_PROJECT_SPLIT = re.compile(r"\s+,\s+")

# The sheet's maintainer appends change-request markers that are not part of the
# project name in the source system, e.g. the sheet writes
#   "AMADEUS BIG BET #ThinkBig 24 billable part5 - ACP+SSP Rollout CON x 300000 [CR]"
# where `projects.project_name` is the same string without the trailing " [CR]".
# Stripping it is only ever used as a SECOND pass, and only when it resolves to
# exactly one project, because some real names genuinely end in a bracket tag
# (e.g. "Bayer AG CS - AI incubator CON x 239790 [BIF]" matches as-is).
_TRAILING_TAG = re.compile(r"\s*\[[^\[\]]{0,24}\]\s*$")

_lock = threading.Lock()
_cache: Dict[str, Any] = {"fetched_at": 0.0, "payload": None}
_token_strategy: Optional[str] = None


def normalize_project_name(name: Optional[str]) -> str:
    """Casefolded, whitespace-collapsed key for matching a project by name.

    Whitespace collapsing matters: several real names carry a double space
    ("Lloyds Bank PLC  PSO - Agentic Mobilisation...") and hand-typed sheet
    entries do not reliably reproduce it.
    """
    if not name:
        return ""
    return re.sub(r"\s+", " ", str(name)).strip().casefold()


def strip_trailing_tag(name: Optional[str]) -> str:
    """`normalize_project_name` with one trailing [BRACKET] tag removed."""
    base = re.sub(r"\s+", " ", str(name or "")).strip()
    return _TRAILING_TAG.sub("", base).strip().casefold()


# --------------------------------------------------------------------------
# credentials
# --------------------------------------------------------------------------
def _token_via_adc() -> Optional[str]:
    """A sheets-scoped token straight from ADC, if ADC can mint one."""
    import google.auth
    from google.auth.transport.requests import Request as GARequest

    creds, _ = google.auth.default(scopes=[SHEETS_SCOPE])
    creds.refresh(GARequest())
    return creds.token


def _token_via_self_impersonation() -> Optional[str]:
    """Ask IAM Credentials for a sheets-scoped token for our own identity.

    Cloud Run hands us a cloud-platform token, which Sheets refuses. The service
    account can mint itself a differently-scoped one provided it holds
    roles/iam.serviceAccountTokenCreator on itself.
    """
    import google.auth
    from google.auth.transport.requests import Request as GARequest

    creds, _ = google.auth.default()
    creds.refresh(GARequest())

    sa_email = os.getenv("TVC_IMPERSONATE_SA") or getattr(creds, "service_account_email", None)
    if not sa_email or sa_email == "default":
        # The metadata server reports "default" rather than the real address.
        try:
            resp = requests.get(
                "http://metadata.google.internal/computeMetadata/v1/"
                "instance/service-accounts/default/email",
                headers={"Metadata-Flavor": "Google"},
                timeout=5,
            )
            if resp.ok:
                sa_email = resp.text.strip()
        except Exception:  # pragma: no cover - only runs on GCP
            pass
    if not sa_email or sa_email == "default":
        raise RuntimeError("cannot determine the runtime service account address")

    url = (
        "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/"
        f"{urllib.parse.quote(sa_email)}:generateAccessToken"
    )
    resp = requests.post(
        url,
        headers={"Authorization": f"Bearer {creds.token}"},
        json={"scope": [SHEETS_SCOPE], "lifetime": "3600s"},
        timeout=HTTP_TIMEOUT,
    )
    if not resp.ok:
        raise RuntimeError(f"generateAccessToken {resp.status_code}: {resp.text[:300]}")
    return resp.json().get("accessToken")


def _sheets_token(user_token: Optional[str] = None) -> Tuple[str, str]:
    """Returns (token, strategy_name).

    The caller's own OAuth token wins when present. Corp Drive will not
    share a google.com document with the runtime service account - Drive
    answers 404, i.e. no ACL entry at all - so the service-account paths
    below only work for a sheet that has genuinely been shared with it.
    They are kept as a fallback rather than removed, so that pointing
    TVC_SHEET_ID at a service-account-owned sheet keeps working.
    """
    global _token_strategy

    if user_token:
        return user_token, "user"

    strategies = [("adc", _token_via_adc), ("impersonation", _token_via_self_impersonation)]
    if _token_strategy == "impersonation":
        strategies.reverse()

    errors = []
    for name, fn in strategies:
        try:
            token = fn()
            if token:
                _token_strategy = name
                return token, name
        except Exception as exc:  # noqa: BLE001 - want the message, not the type
            errors.append(f"{name}: {exc}")
    raise RuntimeError("no sheets-scoped credential available (" + "; ".join(errors) + ")")


# --------------------------------------------------------------------------
# fetch + parse
# --------------------------------------------------------------------------
def _a1_range(tab: str) -> str:
    """A1 notation for a whole sheet.

    A bare tab name is only valid A1 when it has no spaces; "Sep 03" must be
    written 'Sep 03'. An apostrophe inside the name is escaped by doubling it,
    per the A1 spec - relevant the day someone names a tab "Dave's copy".
    """
    return "'" + str(tab).replace("'", "''") + "'"


def _fetch_rows(user_token: Optional[str] = None) -> List[List[str]]:
    token, strategy = _sheets_token(user_token)
    url = (
        f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}/values/"
        f"{urllib.parse.quote(_a1_range(SHEET_TAB))}?majorDimension=ROWS"
    )
    resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=HTTP_TIMEOUT)
    if resp.status_code in (401, 403):
        raise RuntimeError(
            f"Sheets returned {resp.status_code} (credential={strategy}). Either the "
            "signed-in user cannot open the sheet, or their token predates the "
            "spreadsheets.readonly scope being added - signing out and back in "
            f"re-mints it. Detail: {resp.text[:200]}"
        )
    if resp.status_code == 400:
        raise RuntimeError(
            f"Sheets rejected the range for tab '{SHEET_TAB}'. Detail: {resp.text[:200]}"
        )
    if resp.status_code == 404:
        raise RuntimeError(f"spreadsheet {SHEET_ID} not found or not visible")
    if not resp.ok:
        raise RuntimeError(f"Sheets API {resp.status_code}: {resp.text[:240]}")
    return resp.json().get("values", []) or []


def parse_tvc_rows(rows: List[List[str]]) -> Dict[str, Any]:
    """Turn raw sheet rows into a project-name -> [tvc] index.

    Split out from the fetch so it can be unit-tested against the downloaded
    .xlsx without any network or credentials.
    """
    if not rows:
        return {"by_project": {}, "total_tvcs": 0, "assigned_tvcs": 0,
                "unassigned_tvcs": 0, "pairs": 0}

    header = [str(h or "").strip() for h in rows[0]]
    idx = {h: i for i, h in enumerate(header) if h}

    def cell(row: List[str], col: str) -> str:
        i = idx.get(col)
        if i is None or i >= len(row):
            return ""
        return str(row[i] or "").strip()

    by_project: Dict[str, List[Dict[str, str]]] = {}
    total = assigned = pairs = 0

    for row in rows[1:]:
        if not any(str(c or "").strip() for c in row):
            continue
        username = cell(row, "Username")
        if not username:
            continue
        total += 1

        raw_projects = cell(row, "Projects")
        if not raw_projects:
            continue
        assigned += 1

        person = {
            "username": username,
            "partner": cell(row, "Partner"),
            "level": cell(row, "Level"),
            "role": cell(row, "Role"),
            "sub_practice": cell(row, "Sub-Practice"),
            "skills": cell(row, "Skills"),
            "google_poc": cell(row, "Google POC"),
            "status": cell(row, "Status"),
            "mask_id": cell(row, "Mask ID"),
            "region": cell(row, "Region"),
        }

        for piece in _PROJECT_SPLIT.split(raw_projects):
            piece = piece.strip()
            if not piece:
                continue
            pairs += 1
            by_project.setdefault(normalize_project_name(piece), []).append(
                dict(person, project_label=piece)
            )

    for people in by_project.values():
        people.sort(key=lambda p: (p.get("partner", ""), p.get("username", "")))

    return {
        "by_project": by_project,
        "total_tvcs": total,
        "assigned_tvcs": assigned,
        "unassigned_tvcs": total - assigned,
        "pairs": pairs,
    }


def get_tvc_index(force: bool = False, user_token: Optional[str] = None) -> Dict[str, Any]:
    """Cached TVC index. Never raises.

    The cache is process-wide and NOT per-user. That is deliberate: the sheet is
    a single shared operational roster, identical for every viewer, and anyone
    who can load this dashboard already sees the full staffing roster. Caching
    per token would multiply sheet reads by the number of viewers for no gain.
    """
    now = time.time()
    with _lock:
        cached = _cache.get("payload")
        fresh = cached and (now - float(_cache.get("fetched_at") or 0)) < CACHE_TTL_SECONDS
        if fresh and not force:
            return cached

    try:
        parsed = parse_tvc_rows(_fetch_rows(user_token))
        payload = dict(
            parsed,
            ok=True,
            error=None,
            tab=SHEET_TAB,
            sheet_id=SHEET_ID,
            token_strategy=_token_strategy,
            fetched_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        )
        with _lock:
            _cache["payload"] = payload
            _cache["fetched_at"] = now
        logger.info(
            "TVC sheet '%s' loaded: %d TVCs, %d assigned, %d projects (%s)",
            SHEET_TAB, payload["total_tvcs"], payload["assigned_tvcs"],
            len(payload["by_project"]), _token_strategy,
        )
        return payload
    except Exception as exc:  # noqa: BLE001 - the dashboard must still render
        logger.warning("TVC sheet fetch failed: %s", exc)
        with _lock:
            stale = _cache.get("payload")
        if stale:
            # Better a slightly old roster, clearly labelled, than a silent zero.
            return dict(stale, ok=False, error=str(exc), is_stale=True)
        return {
            "ok": False, "error": str(exc), "is_stale": False, "tab": SHEET_TAB,
            "sheet_id": SHEET_ID, "by_project": {}, "total_tvcs": 0,
            "assigned_tvcs": 0, "unassigned_tvcs": 0, "pairs": 0,
            "token_strategy": _token_strategy, "fetched_at": None,
        }


def lookup_project(index: Dict[str, Any], project_name: Optional[str]) -> List[Dict[str, str]]:
    """TVCs on `project_name`, or [] when the project has none.

    Absence is meaningful here: the sheet lists every active Core-EMEA TVC, so a
    project that does not appear genuinely has no TVC assigned.
    """
    by_project = (index or {}).get("by_project") or {}
    if not by_project or not project_name:
        return []

    hit = by_project.get(normalize_project_name(project_name))
    if hit:
        return hit

    # Second pass: the sheet appends change-request tags the source name lacks.
    # Only accept it when it is unambiguous.
    stripped = strip_trailing_tag(project_name)
    if stripped and stripped != normalize_project_name(project_name):
        hit = by_project.get(stripped)
        if hit:
            return hit
    candidates = [v for k, v in by_project.items() if strip_trailing_tag(k) == stripped and stripped]
    if len(candidates) == 1:
        return candidates[0]
    return []
