import datetime
import logging
import os
import re
from typing import Optional, Dict, Any, List
from fastapi import Header, HTTPException, Request
from google.cloud import bigquery
from google.oauth2.credentials import Credentials
from src.services.manager_directory import get_manager_name

logger = logging.getLogger(__name__)

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "concord-prod")
BIGQUERY_SCOPE = "https://www.googleapis.com/auth/bigquery"

def extract_ldap(raw_val: Optional[str], fallback: str = "") -> str:
    """Extracts clean ldap from email or raw string."""
    if not raw_val:
        return fallback.lower().replace(" ", "") if fallback else "unknown"
    val = str(raw_val).strip()
    if ":" in val:
        val = val.split(":")[-1].strip()
    # If email format, strip domain
    if "@" in val:
        val = val.split("@")[0].strip()
    return val.lower() or (fallback.lower().replace(" ", "") if fallback else "unknown")

def _extract_user_token(
    request: Request,
    authorization: Optional[str],
    x_user_oauth_token: Optional[str],
    x_google_oauth_token: Optional[str],
) -> Optional[str]:
    """Pulls the caller's Google OAuth access token out of the request."""
    token = (
        x_user_oauth_token
        or x_google_oauth_token
        or request.headers.get("x-user-oauth-token")
        or request.headers.get("X-User-OAuth-Token")
        or request.headers.get("x-google-oauth-token")
        or request.headers.get("X-Google-OAuth-Token")
    )

    if not token and authorization and authorization.startswith("Bearer "):
        auth_token = authorization.split("Bearer ")[1].strip()
        if auth_token != "MOCK_TOKEN":
            token = auth_token

    return token.strip() if token and token.strip() else None


def get_bq_client(
    request: Request,
    authorization: Optional[str] = Header(None, alias="Authorization"),
    x_user_oauth_token: Optional[str] = Header(None, alias="X-User-OAuth-Token"),
    x_google_oauth_token: Optional[str] = Header(None, alias="X-Google-OAuth-Token"),
) -> bigquery.Client:
    """Returns BigQuery client using the user's OAuth token."""
    token = _extract_user_token(request, authorization, x_user_oauth_token, x_google_oauth_token)

    if token:
        logger.info("Using explicit OAuth token from Header for BigQuery.")
        user_credentials = Credentials(
            token=token,
            scopes=[BIGQUERY_SCOPE],
        )
        return bigquery.Client(project=PROJECT_ID, credentials=user_credentials)

    logger.info("No OAuth token provided. Falling back to ADC.")
    return bigquery.Client(project=PROJECT_ID)


def require_bq_client(
    request: Request,
    authorization: Optional[str] = Header(None, alias="Authorization"),
    x_user_oauth_token: Optional[str] = Header(None, alias="X-User-OAuth-Token"),
    x_google_oauth_token: Optional[str] = Header(None, alias="X-Google-OAuth-Token"),
) -> bigquery.Client:
    """Same as get_bq_client, but REFUSES anonymous callers.

    Routes that return the full dataset must use this. With get_bq_client an
    unauthenticated request fell through to ADC; the runtime service account has
    no access to concord-prod, so the query failed and the route answered from
    the cached snapshot. The practical effect was that anyone who could reach
    the URL got a stale copy of the whole roster without signing in. Serving 401
    forces the browser back to the sign-in card.
    """
    token = _extract_user_token(request, authorization, x_user_oauth_token, x_google_oauth_token)
    if not token:
        raise HTTPException(
            status_code=401,
            detail="Sign in with Google to load live BigQuery data.",
        )

    user_credentials = Credentials(token=token, scopes=[BIGQUERY_SCOPE])
    return bigquery.Client(project=PROJECT_ID, credentials=user_credentials)


def get_user_token(
    request: Request,
    authorization: Optional[str] = Header(None, alias="Authorization"),
    x_user_oauth_token: Optional[str] = Header(None, alias="X-User-OAuth-Token"),
    x_google_oauth_token: Optional[str] = Header(None, alias="X-Google-OAuth-Token"),
) -> Optional[str]:
    """The caller's raw OAuth access token, or None.

    Needed because the TVC sheet is read as the signed-in user rather than as
    the runtime service account: corp Drive will not share a google.com
    document with an external @developer.gserviceaccount.com identity. The
    token already carries the Sheets scope (the frontend requests it alongside
    BigQuery), so it can be forwarded straight to the Sheets API.

    Returning None rather than raising keeps this usable on routes that
    tolerate anonymous callers; the TVC fetch simply reports unavailable.
    """
    return _extract_user_token(request, authorization, x_user_oauth_token, x_google_oauth_token)


# LDAPs explicitly excluded from the roster at the request of the org owner.
# All six are real, CC1, and inside the default org, so no scope predicate would
# drop them - the exclusion has to be deliberate. Applied to the roster query AND
# to the manager dropdown, because two of them (icebrian, ptokarski) are managers
# and would otherwise still be offered as an org to scope by.
EXCLUDED_LDAPS = (
    "josecoliveira",
    "mehmetalatas",
    "mworonowicz",
    "joaoaz",
    "ptokarski",
    "icebrian",
)

# Matches how ldap is normalised everywhere else here: the source column is
# sometimes a bare ldap and sometimes an email address.
def excluded_ldap_sql(expr: str = "ldap") -> str:
    """`NOT IN` predicate for the exclusion list, against any ldap-ish column.

    Parameterised by column because the manager dropdown has to apply the same
    list twice: once to the PERSON being counted (`ldap`) and once to the
    MANAGER being offered as a scope (`mgr_ldap`). Filtering only the person
    side still leaves an excluded manager in the dropdown, because they appear
    in other people's hierarchy chains.
    """
    return (
        "LOWER(COALESCE(SPLIT({e}, '@')[OFFSET(0)], {e})) NOT IN ({vals})".format(
            e=expr, vals=", ".join("'%s'" % l for l in EXCLUDED_LDAPS)
        )
    )


EXCLUDED_LDAP_SQL = excluded_ldap_sql("ldap")


def query_emea_delivery_data(
    client: bigquery.Client,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    org_ldap: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Fetches delivery resources and weekly schedules filtered by date range.

    org_ldap: when supplied, the roster is everyone whose management chain
    contains that ldap, at any depth, REGARDLESS of region. The chain lives in
    `manager_hierarchy_user_names` as a pipe-delimited string, e.g.
        |shubhampathakk|gauravtaneja|guptaashutosh|raosunil|lynb|...
    so a person reports (indirectly) to X when the chain contains '|X|'.

    When org_ldap is None the legacy EMEA cost-centre/region filter applies.
    Defaults to last 90 days if no date range is provided. Zero LIMIT applied.
    """
    today = datetime.date.today()
    if not end_date or not str(end_date).strip():
        # Include upcoming cycle (+14 days) to guarantee coverage of current week Saturday
        end_date = (today + datetime.timedelta(days=14)).strftime("%Y-%m-%d")
    else:
        end_date = str(end_date).strip()

    if not start_date or not str(start_date).strip():
        start_date = (today - datetime.timedelta(days=90)).strftime("%Y-%m-%d")
    else:
        start_date = str(start_date).strip()

    query = """
    WITH anchor AS (
      -- The single week the dashboard reports on, CLAMPED TO THE REQUESTED
      -- WINDOW. Previously this was pinned to CURRENT_DATE() unconditionally,
      -- so selecting a historical date range moved the roster window but still
      -- reported this week's hours, allocations and OOO against it.
      -- Order of preference:
      --   1. the current week, when it falls inside the requested window
      --   2. the latest week inside the requested window
      --   3. the latest week available (window matches nothing)
      -- With the default window (today-90 .. today+14) this resolves to
      -- exactly the same week as before.
      SELECT COALESCE(
        (SELECT MIN(timecard_week_ending)
           FROM `concord-prod.service_cloudbi.scheduled_vs_actual_utilization`
          WHERE _PARTITIONDATE = (SELECT MAX(_PARTITIONDATE) FROM `concord-prod.service_cloudbi.scheduled_vs_actual_utilization`)
            AND timecard_week_ending >= CURRENT_DATE()
            AND timecard_week_ending BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)),
        (SELECT MAX(timecard_week_ending)
           FROM `concord-prod.service_cloudbi.scheduled_vs_actual_utilization`
          WHERE _PARTITIONDATE = (SELECT MAX(_PARTITIONDATE) FROM `concord-prod.service_cloudbi.scheduled_vs_actual_utilization`)
            AND timecard_week_ending BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)),
        (SELECT MAX(timecard_week_ending)
           FROM `concord-prod.service_cloudbi.scheduled_vs_actual_utilization`
          WHERE _PARTITIONDATE = (SELECT MAX(_PARTITIONDATE) FROM `concord-prod.service_cloudbi.scheduled_vs_actual_utilization`))
      ) AS wk
    ),
    target_resources AS (
      -- Roster membership is established across the whole requested window so
      -- that nobody drops off the headcount for a single quiet week.
      SELECT 
        resource_id,
        ANY_VALUE(full_name) AS resource_name,
        ANY_VALUE(COALESCE(SPLIT(ldap, '@')[OFFSET(0)], ldap)) AS ldap,
        ANY_VALUE(COALESCE(SPLIT(manager_ldap, '@')[OFFSET(0)], manager_ldap)) AS manager_ldap,
        ANY_VALUE(role) AS role,
        ANY_VALUE(cost_center) AS cost_center,
        ANY_VALUE(cost_center_name) AS cost_center_name,
        -- Real region, not a hardcoded 'EMEA'. An org-scoped roster can span
        -- regions (guptaashutosh's org is entirely 'Delivery Center'), and
        -- stamping everyone 'EMEA' mislabels them.
        ANY_VALUE(COALESCE(region, pso_region, 'Unknown')) AS region,
        IF(LOGICAL_OR(COALESCE(region, pso_region, '') LIKE '%Delivery Center%'
                      OR COALESCE(region, pso_region, '') LIKE '%GSD%'
                      OR cost_center = 'CC1'), 'GSD', 'EMEA') AS hub,
        ANY_VALUE(practice) AS practice,
        -- OOO for the ANCHOR WEEK only. Previously this was LOGICAL_OR(OOO)
        -- across the whole ~15-week window, so "ON LEAVE" flagged anyone who
        -- took a day off in the last three months (31 of 86) rather than the
        -- people actually out this week (2).
        LOGICAL_OR(COALESCE(OOO, FALSE)
                   AND timecard_week_ending = (SELECT wk FROM anchor)) AS is_ooo
      FROM `concord-prod.service_cloudbi.scheduled_vs_actual_utilization`
      WHERE __ROSTER_SCOPE__
        AND __LDAP_EXCLUSIONS__
        AND _PARTITIONDATE = (SELECT MAX(_PARTITIONDATE) FROM `concord-prod.service_cloudbi.scheduled_vs_actual_utilization`)
        AND timecard_week_ending BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)
      GROUP BY resource_id
    ),
    pto_weeks AS (
      -- Every week from the anchor forward in which this person has booked
      -- PTO. Source granularity is a WEEK - there is no day-level leave table
      -- anywhere in service_cloudbi, so "on leave until" can only ever be
      -- resolved to a week ending.
      SELECT
        resource_id,
        timecard_week_ending AS wk,
        ROUND(SUM(COALESCE(scheduled_pto_hours, 0)), 1) AS pto_hrs,
        ROUND(MAX(COALESCE(work_hours, 0)), 1) AS cap_hrs
      FROM `concord-prod.service_cloudbi.scheduled_vs_actual_utilization`
      WHERE _PARTITIONDATE = (SELECT MAX(_PARTITIONDATE) FROM `concord-prod.service_cloudbi.scheduled_vs_actual_utilization`)
        AND timecard_week_ending >= (SELECT wk FROM anchor)
      GROUP BY resource_id, wk
      HAVING SUM(COALESCE(scheduled_pto_hours, 0)) > 0
    ),
    pto_runs AS (
      -- Rank the PTO weeks per person. A week belongs to the CURRENT leave run
      -- only if every week between the anchor and it also has PTO, i.e. its
      -- offset from the anchor equals its position in the sequence. Without
      -- this, somebody out this week AND again at Christmas would read as being
      -- on leave until December.
      -- DIV(DATE_DIFF(..., DAY), 7) rather than DATE_DIFF(..., WEEK): the latter
      -- counts week boundaries crossed, which is not the same thing.
      SELECT
        resource_id,
        wk,
        pto_hrs,
        cap_hrs,
        DIV(DATE_DIFF(wk, (SELECT wk FROM anchor), DAY), 7) AS wk_offset,
        ROW_NUMBER() OVER (PARTITION BY resource_id ORDER BY wk) - 1 AS seq
      FROM pto_weeks
    ),
    pto_summary AS (
      -- One row per person. Only populated for people whose run starts in the
      -- anchor week itself, so this can never claim a future holiday is current
      -- leave. Grouped by resource_id, so the join below cannot fan out.
      SELECT
        resource_id,
        MAX(wk) AS pto_through,
        COUNT(*) AS pto_week_count,
        -- Hours in the FINAL week of the run. A full week means they are out
        -- for all of it; 32 of 40 means they are back before it ends, and the
        -- UI must not imply a precision we do not have.
        ARRAY_AGG(pto_hrs ORDER BY wk DESC LIMIT 1)[OFFSET(0)] AS pto_final_week_hours,
        ARRAY_AGG(cap_hrs ORDER BY wk DESC LIMIT 1)[OFFSET(0)] AS pto_final_week_capacity
      FROM pto_runs
      WHERE wk_offset = seq
      GROUP BY resource_id
    ),
    current_load AS (
      -- Person-level scheduled hours for the anchor week ONLY. Previously this
      -- was AVG() across every week in the window, which reported a 15-week
      -- average as if it were a single week's allocation.
      SELECT
        resource_id,
        ROUND(SUM(scheduled_timecard_hours_net), 1) AS scheduled_timecard_hours,
        -- REAL weekly capacity, straight from the source. We used to infer this
        -- from the role string and gave 16h to anything starting with
        -- "manager". That was invented: every "Manager (Billable CON/SCE)" in
        -- this org has work_hours = 40 and is scheduled ~39h of delivery. The
        -- bad 16h denominator made a 20h project read as "100% allocated".
        -- Observed values here are only 40 (78 people) and 37.5 (1 person).
        ROUND(SUM(work_hours), 1) AS work_hours
      FROM `concord-prod.service_cloudbi.scheduled_vs_actual_utilization`
      WHERE _PARTITIONDATE = (SELECT MAX(_PARTITIONDATE) FROM `concord-prod.service_cloudbi.scheduled_vs_actual_utilization`)
        AND timecard_week_ending = (SELECT wk FROM anchor)
      GROUP BY resource_id
    ),
    target_projects AS (
      SELECT 
        project_id,
        ANY_VALUE(project_name) AS project_name,
        -- No COALESCE onto project_name: an assignment with no account is not
        -- evidence of a customer account.
        ANY_VALUE(account_name) AS account_name,
        -- Real people on the project. NULLIF(TRIM(...)) matters: these columns
        -- hold '' far more often than NULL, so a plain COALESCE fell through to
        -- a placeholder and the UI showed "Delivery Lead" on 108 of 163 rows.
        -- Leave them NULL when absent and let the UI say "Unassigned".
        ANY_VALUE(NULLIF(TRIM(engagement_manager), ''))      AS engagement_manager_name,
        ANY_VALUE(NULLIF(TRIM(pso_engineering_manager), '')) AS pso_engineering_manager,
        -- project_manager is 100% populated but 13% of it is the literal string
        -- "Please Update Project Manager" / "No PM" entered at source. Treat
        -- those as unassigned rather than rendering them as a person's name.
        ANY_VALUE(
          CASE
            WHEN LOWER(TRIM(project_manager)) LIKE '%please update%' THEN NULL
            WHEN LOWER(TRIM(project_manager)) LIKE 'no pm%'          THEN NULL
            ELSE NULLIF(TRIM(project_manager), '')
          END
        ) AS project_manager,
        ANY_VALUE(NULLIF(TRIM(project_manager_ldap), '')) AS project_manager_ldap,
        -- Stored as STRING in the source; parse defensively.
        ANY_VALUE(SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(CAST(project_start_date AS STRING), 1, 10))) AS project_start_date,
        ANY_VALUE(SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(CAST(project_end_date AS STRING), 1, 10))) AS project_end_date,
        ANY_VALUE(project_status) AS project_status
      FROM `concord-prod.service_cloudbi.projects`
      -- MUST pin the partition. This table keeps ~1,074 daily snapshots of the
      -- same ~110k projects (60M rows, back to 2023-09). Without this filter
      -- ANY_VALUE() picks an arbitrary historical version of project_end_date,
      -- so runway/roll-off dates and the "already ended" filter were computed
      -- against dates that could be years out of date.
      WHERE _PARTITIONDATE = (SELECT MAX(_PARTITIONDATE) FROM `concord-prod.service_cloudbi.projects`)
      GROUP BY project_id
    ),
    week_assignments AS (
      -- Assignments scheduled in the anchor week only.
      SELECT 
        ws.resource_id,
        ws.project_id,
        COALESCE(NULLIF(ws.assignment_id, ''), ws.project_id) AS assignment_id,
        ROUND(SUM(ws.scheduled_timecard_hours), 1) AS scheduled_timecard_hours
      FROM `concord-prod.service_cloudbi.weekly_schedules` ws
      WHERE ws._PARTITIONDATE = (SELECT MAX(_PARTITIONDATE) FROM `concord-prod.service_cloudbi.weekly_schedules`)
        AND ws.schedule_week_ending = (SELECT wk FROM anchor)
        AND ws.scheduled_timecard_hours > 0
      GROUP BY ws.resource_id, ws.project_id, assignment_id
    ),
    active_assignments AS (
      -- Drop work on projects that have already ended. Doing this here (rather
      -- than in the final WHERE) keeps people whose only project has finished
      -- on the roster, correctly showing as unassigned instead of vanishing.
      SELECT
        a.resource_id,
        a.project_id,
        a.assignment_id,
        a.scheduled_timecard_hours,
        p.project_name,
        p.account_name,
        p.engagement_manager_name,
        p.pso_engineering_manager,
        p.project_manager,
        p.project_manager_ldap,
        p.project_start_date,
        p.project_end_date,
        -- RAG health flag ('' / Green / Yellow / Red). NOT a lifecycle status.
        p.project_status
      FROM week_assignments a
      LEFT JOIN target_projects p ON a.project_id = p.project_id
      WHERE p.project_end_date IS NULL OR p.project_end_date >= CURRENT_DATE()
    ),
    all_people AS (
      -- Global ldap -> full name directory (~3k people, all regions), used to
      -- resolve manager display names dynamically. This replaces a hardcoded
      -- dictionary, so a newly appointed manager shows up automatically.
      -- The key is lowercased so the same CTE can also resolve the
      -- engagement/engineering manager ldaps carried on the project record,
      -- which are not guaranteed to match the resource table's casing.
      -- Lowercasing inside the GROUP BY (rather than in the JOIN predicate)
      -- keeps the key unique, so none of these joins can fan out and
      -- duplicate a person's hours.
      SELECT
        LOWER(COALESCE(SPLIT(ldap, '@')[OFFSET(0)], ldap)) AS ldap,
        ANY_VALUE(full_name) AS full_name
      FROM `concord-prod.service_cloudbi.scheduled_vs_actual_utilization`
      WHERE _PARTITIONDATE = (SELECT MAX(_PARTITIONDATE) FROM `concord-prod.service_cloudbi.scheduled_vs_actual_utilization`)
        AND full_name IS NOT NULL
        AND ldap IS NOT NULL
      GROUP BY 1
    ),
    -- ================= Delivery Executive (account level) =================
    -- There is no "delivery_executive" column anywhere in the source. The DE is
    -- recoverable because `role` carries the literal value 'Delivery Executive'
    -- (103 people hold it). Two signals, in priority order:
    --
    --   A. projects.engagement_manager for the account, WHERE that ldap is a
    --      Delivery Executive. engagement_manager is a mixed bag - it holds
    --      delivery leads, engineering managers and DEs depending on the
    --      project - so the role filter is what makes it meaningful.
    --   B. otherwise, whoever is actually STAFFED on the account with
    --      resource_role = 'Delivery Executive', ranked by hours.
    --
    -- Validated against known ground truth: Unilever -> thomasazais
    -- (Thomas Azais). Covers 14 of the 30 in-scope accounts; the rest have no
    -- DE signal at all and must render as "Unassigned".
    de_people AS (
      SELECT DISTINCT LOWER(COALESCE(SPLIT(ldap, '@')[OFFSET(0)], ldap)) AS ldap
      FROM `concord-prod.service_cloudbi.scheduled_vs_actual_utilization`
      WHERE _PARTITIONDATE = (SELECT MAX(_PARTITIONDATE) FROM `concord-prod.service_cloudbi.scheduled_vs_actual_utilization`)
        AND role = 'Delivery Executive'
        AND ldap IS NOT NULL
    ),
    scope_accounts AS (
      -- Only resolve a DE for accounts this roster actually touches. Keeps the
      -- two joins below small.
      SELECT DISTINCT account_name
      FROM active_assignments
      WHERE account_name IS NOT NULL AND account_name != ''
    ),
    de_signal_a AS (
      SELECT DISTINCT
        p.account_name,
        LOWER(TRIM(p.engagement_manager)) AS de_ldap
      FROM `concord-prod.service_cloudbi.projects` p
      JOIN de_people d ON LOWER(TRIM(p.engagement_manager)) = d.ldap
      WHERE p._PARTITIONDATE = (SELECT MAX(_PARTITIONDATE) FROM `concord-prod.service_cloudbi.projects`)
        AND p.account_name IN (SELECT account_name FROM scope_accounts)
    ),
    de_signal_b AS (
      -- A 13-week window, not just the anchor week: a DE is often scheduled
      -- intermittently, and a badge that appears and disappears week to week is
      -- worse than one that is stable.
      SELECT
        pa.account_name,
        ws.resource_name AS de_name,
        ROUND(SUM(ws.scheduled_timecard_hours), 1) AS hrs
      FROM `concord-prod.service_cloudbi.weekly_schedules` ws
      JOIN (
        SELECT project_id, ANY_VALUE(account_name) AS account_name
        FROM `concord-prod.service_cloudbi.projects`
        WHERE _PARTITIONDATE = (SELECT MAX(_PARTITIONDATE) FROM `concord-prod.service_cloudbi.projects`)
          AND account_name IN (SELECT account_name FROM scope_accounts)
        GROUP BY project_id
      ) pa ON ws.project_id = pa.project_id
      WHERE ws._PARTITIONDATE = (SELECT MAX(_PARTITIONDATE) FROM `concord-prod.service_cloudbi.weekly_schedules`)
        AND ws.schedule_week_ending BETWEEN DATE_SUB((SELECT wk FROM anchor), INTERVAL 12 WEEK)
                                        AND (SELECT wk FROM anchor)
        AND ws.scheduled_timecard_hours > 0
        AND ws.resource_role = 'Delivery Executive'
      GROUP BY 1, 2
    ),
    de_candidates AS (
      SELECT account_name, de_ldap, CAST(NULL AS STRING) AS de_name, 1 AS pri, 0.0 AS hrs
      FROM de_signal_a
      UNION ALL
      SELECT account_name, CAST(NULL AS STRING) AS de_ldap, de_name, 2 AS pri, hrs
      FROM de_signal_b
    ),
    account_de AS (
      -- Exactly one row per account, so the join below cannot fan out.
      SELECT account_name, de_ldap, de_name
      FROM (
        SELECT c.*,
               ROW_NUMBER() OVER (
                 PARTITION BY account_name
                 ORDER BY pri, hrs DESC, COALESCE(de_ldap, de_name)
               ) AS rn
        FROM de_candidates c
      )
      WHERE rn = 1
    )
    SELECT 
      r.resource_id,
      r.resource_name,
      COALESCE(r.ldap, SPLIT(r.resource_name, ' ')[OFFSET(0)]) AS ldap,
      r.manager_ldap,
      mn.full_name AS manager_name_resolved,
      r.role,
      r.cost_center,
      r.cost_center_name,
      r.region,
      r.hub,
      r.practice,
      r.is_ooo,
      -- Week ending through which this person has booked continuous PTO, and
      -- the hours in that final week. Week-level only; see pto_summary.
      CAST(ps.pto_through AS STRING) AS pto_through,
      ps.pto_week_count AS pto_week_count,
      ps.pto_final_week_hours AS pto_final_week_hours,
      ps.pto_final_week_capacity AS pto_final_week_capacity,
      COALESCE(cl.scheduled_timecard_hours, 0.0) AS scheduled_timecard_hours,
      -- Real weekly capacity for this person. NULL only if they have no row in
      -- the anchor week; the payload builder falls back to 40 in that case.
      cl.work_hours AS work_hours,
      CAST((SELECT wk FROM anchor) AS STRING) AS week_ending,
      a.project_id,
      a.assignment_id,
      a.account_name AS account_name,
      IF(a.project_id IS NOT NULL,
         IF(a.assignment_id IS NOT NULL AND a.assignment_id != '' AND a.assignment_id != a.project_id,
            CONCAT(COALESCE(a.project_name, 'Cloud Transformation'), ' (', a.assignment_id, ')'),
            COALESCE(a.project_name, 'Cloud Transformation')
         ),
         NULL
      ) AS project_name,
      -- The project name WITHOUT the assignment-id suffix. project_name above
      -- appends the assignment id, which is why 47 real projects render as 153
      -- differently-named rows. The drill-down groups on project_id and labels
      -- with this.
      IF(a.project_id IS NOT NULL, COALESCE(a.project_name, 'Cloud Transformation'), NULL) AS base_project_name,
      -- Real people. NULL means genuinely unassigned at source - the UI must
      -- say so rather than substituting a placeholder name.
      -- engagement_manager and pso_engineering_manager hold an LDAP, not a
      -- name, so resolve them through the directory. COALESCE back to the raw
      -- ldap when the person has no row in the resource table (e.g. a
      -- sales-side EM): showing "barshasethi" is honest, inventing a name is
      -- not.
      a.engagement_manager_name AS engagement_manager_ldap,
      COALESCE(emn.full_name, a.engagement_manager_name) AS engagement_manager_name,
      a.pso_engineering_manager AS pso_engineering_manager_ldap,
      COALESCE(egmn.full_name, a.pso_engineering_manager) AS pso_engineering_manager,
      a.project_manager         AS project_manager,
      a.project_manager_ldap    AS project_manager_ldap,
      -- Account-level Delivery Executive. Derived (see account_de above), not a
      -- source column. NULL for the 16 accounts with no DE signal.
      de.de_ldap AS delivery_executive_ldap,
      COALESCE(de.de_name, den.full_name, de.de_ldap) AS delivery_executive,
      CAST(a.project_start_date AS STRING) AS project_start_date,
      CAST(a.project_end_date AS STRING) AS project_end_date,
      -- Project health RAG. Blank at source on the overwhelming majority of
      -- projects (17,226 blank vs 635 Green / 120 Yellow / 41 Red), so NULL
      -- here means "not reported", NOT "healthy". The UI must say so.
      NULLIF(TRIM(a.project_status), '') AS project_status,
      -- NULL when there is no assignment. Previously this fell back to the
      -- person's total hours, inventing a phantom "Delivery Project".
      a.scheduled_timecard_hours AS proj_scheduled_hours
    FROM target_resources r
    LEFT JOIN current_load cl ON r.resource_id = cl.resource_id
    LEFT JOIN pto_summary ps  ON r.resource_id = ps.resource_id
    LEFT JOIN active_assignments a ON r.resource_id = a.resource_id
    LEFT JOIN all_people mn   ON LOWER(r.manager_ldap) = mn.ldap
    LEFT JOIN all_people emn  ON LOWER(a.engagement_manager_name) = emn.ldap
    LEFT JOIN all_people egmn ON LOWER(a.pso_engineering_manager) = egmn.ldap
    LEFT JOIN account_de de   ON a.account_name = de.account_name
    LEFT JOIN all_people den  ON de.de_ldap = den.ldap
    """
    params = [
        bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
        bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
    ]

    # org_ldap accepts a comma-separated list so the UI can offer a multi-select
    # filter. Each leader contributes its own OR'd predicate. Values are bound as
    # query parameters, never interpolated, so this stays injection-safe however
    # many are supplied.
    raw_orgs = [t.strip().lower() for t in (org_ldap or "").split(",")]
    clean_orgs = [t for t in raw_orgs if t and t != "all"]
    # An explicit "all" anywhere in the selection means the whole EMEA pool, so
    # it wins over any individual leader also ticked.
    wants_all = any(t == "all" for t in raw_orgs) or not clean_orgs
    if not wants_all:
        preds = []
        for i, token in enumerate(dict.fromkeys(clean_orgs)):  # de-dup, keep order
            name = f"org_token_{i}"
            preds.append(f"STRPOS(COALESCE(manager_hierarchy_user_names, ''), @{name}) > 0")
            params.append(
                bigquery.ScalarQueryParameter(name, "STRING", f"|{token}|")
            )
        # Union of the selected orgs, then CC1 as a STRICT additional filter.
        #
        # The OR list and the cost-centre test are deliberately at different
        # levels: ANY of the selected leaders, AND CC1. Written flat as
        # "a OR b AND cc1" SQL precedence would bind it as "a OR (b AND cc1)",
        # silently letting non-CC1 people in under the first leader - hence the
        # explicit parentheses around both the union and the whole expression.
        #
        # This applies ONLY to a specific-leader selection. "All EMEA" keeps its
        # original, broader predicate below, which admits CC1 *or* any EMEA
        # cost centre / region.
        roster_scope = "((" + " OR ".join(preds) + ") AND cost_center = 'CC1')"
    else:
        roster_scope = (
            "( cost_center_name LIKE '%EMEA%' OR cost_center = 'CC1'"
            "  OR region = 'EMEA' OR pso_region = 'EMEA' )"
            " AND (cost_center_name NOT LIKE '%JAPAC%' AND cost_center_name NOT LIKE '%LATAM%'"
            "      AND cost_center_name NOT LIKE '%NORTHAM%')"
            " AND (region NOT LIKE '%AMER%' AND region NOT LIKE '%NorthAM%')"
        )
    query = query.replace("__ROSTER_SCOPE__", roster_scope)
    query = query.replace("__LDAP_EXCLUSIONS__", EXCLUDED_LDAP_SQL)

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    results = client.query(query, job_config=job_config).result(timeout=45)
    rows = []
    for row in results:
        d = dict(row)
        d["ldap"] = extract_ldap(d.get("ldap"), fallback=d.get("resource_name", ""))
        mgr_ldap = d.get("manager_ldap")
        if mgr_ldap:
            # Prefer the name resolved live from BigQuery so newly appointed
            # managers appear without a code change. The static catalog is only
            # a fallback for managers absent from the delivery dataset.
            d["manager_name"] = d.pop("manager_name_resolved", None) or get_manager_name(mgr_ldap)
        else:
            d.pop("manager_name_resolved", None)
        rows.append(d)
    return rows

def query_org_options(
    client: bigquery.Client,
    root_ldap: str,
    min_headcount: int = 2,
) -> List[Dict[str, Any]]:
    """Managers inside `root_ldap`'s org that can be used to scope the dashboard.

    Built by unnesting every `manager_hierarchy_user_names` chain, so the list
    stays correct as the org changes - no hardcoded catalog.

    The chain is ordered leaf -> root (e.g. `|shubhampathakk|gauravtaneja|
    guptaashutosh|raosunil|...|`), so a manager is a *descendant* of the root
    exactly when their offset in the chain is <= the root's offset. Anything
    past the root is an ancestor (raosunil, sundar, ...) and is deliberately
    excluded: an unfiltered list returns the entire global PSO org (2,400+
    people under the top-level leaders), which is useless in a dropdown.

    `headcount` is intentionally computed with the same predicate the delivery
    query uses (the manager appears anywhere in the person's chain), so the
    number shown next to an option equals the roster size after selecting it.
    Names are resolved from the people directory where the manager also appears
    as a resource; senior leaders often do not, in which case we fall back to
    MANAGER_CATALOG and finally to the raw ldap.

    CC1 SCOPE. Only CC1 (Delivery Center) people are counted, because selecting
    a leader now applies CC1 as a strict filter. Two consequences, both
    intended:
      * a manager appears here only if they have CC1 people under them - a
        leader with an entirely non-CC1 org is no longer offered, because
        picking them would return an empty dashboard
      * the headcount still equals the resulting roster size exactly
    Note this selects managers OF CC1 people, not managers who are themselves
    CC1 - what matters for a scope filter is who you get when you pick it.
    "All EMEA" is unaffected and keeps its broader predicate.
    """
    query = """
    WITH latest AS (
      SELECT
        resource_id,
        manager_hierarchy_user_names AS chain
      FROM `concord-prod.service_cloudbi.scheduled_vs_actual_utilization`
      WHERE _PARTITIONDATE = (SELECT MAX(_PARTITIONDATE) FROM `concord-prod.service_cloudbi.scheduled_vs_actual_utilization`)
        AND timecard_week_ending BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
                                     AND DATE_ADD(CURRENT_DATE(), INTERVAL 14 DAY)
        AND manager_hierarchy_user_names IS NOT NULL
        AND cost_center = 'CC1'
        AND STRPOS(manager_hierarchy_user_names, @root_token) > 0
        AND __LDAP_EXCLUSIONS__
    ),
    chains AS (
      SELECT
        resource_id,
        TRIM(mgr) AS mgr_ldap,
        off,
        MIN(CASE WHEN TRIM(mgr) = @root_ldap THEN off END)
          OVER (PARTITION BY resource_id) AS root_off
      FROM latest, UNNEST(SPLIT(chain, '|')) AS mgr WITH OFFSET off
      WHERE TRIM(mgr) != ''
    ),
    scoped AS (
      SELECT DISTINCT resource_id, mgr_ldap
      FROM chains
      WHERE root_off IS NOT NULL AND off <= root_off
        -- Excluded people must not be offered as an org to scope BY either.
        -- The filter in `latest` only stops them being counted as someone's
        -- report; they still show up inside other people's hierarchy chains,
        -- which is how ptokarski was still appearing in the dropdown.
        AND __MGR_EXCLUSIONS__
    ),
    people AS (
      SELECT
        COALESCE(SPLIT(ldap, '@')[OFFSET(0)], ldap) AS ldap,
        ANY_VALUE(full_name) AS full_name
      FROM `concord-prod.service_cloudbi.scheduled_vs_actual_utilization`
      WHERE _PARTITIONDATE = (SELECT MAX(_PARTITIONDATE) FROM `concord-prod.service_cloudbi.scheduled_vs_actual_utilization`)
        AND ldap IS NOT NULL
      GROUP BY 1
    )
    SELECT
      s.mgr_ldap AS ldap,
      ANY_VALUE(p.full_name) AS full_name,
      COUNT(DISTINCT s.resource_id) AS headcount
    FROM scoped s
    LEFT JOIN people p ON p.ldap = s.mgr_ldap
    GROUP BY s.mgr_ldap
    HAVING headcount >= @min_headcount
    ORDER BY headcount DESC
    """
    # MUST happen before the query runs. This was missing: the SQL above
    # carries the literal token `__LDAP_EXCLUSIONS__`, so BigQuery rejected
    # every call with "Unrecognized name", get_org_options() swallowed it and
    # returned its single-entry fallback - which looked exactly like "the org
    # only has one manager" rather than like a failure.
    query = query.replace("__LDAP_EXCLUSIONS__", EXCLUDED_LDAP_SQL)
    query = query.replace("__MGR_EXCLUSIONS__", excluded_ldap_sql("mgr_ldap"))
    # Fail loudly rather than repeat the above. An unsubstituted token is a
    # programming error, and the caller's except-clause would otherwise turn it
    # back into a plausible-looking one-manager dropdown.
    if "__" in query and re.search(r"__[A-Z_]+__", query):
        raise RuntimeError(
            "org-options SQL still contains an unsubstituted placeholder: %s"
            % ", ".join(sorted(set(re.findall(r"__[A-Z_]+__", query))))
        )
    root = (root_ldap or "").strip().lower()
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("root_ldap", "STRING", root),
            bigquery.ScalarQueryParameter("root_token", "STRING", f"|{root}|"),
            bigquery.ScalarQueryParameter("min_headcount", "INT64", int(min_headcount)),
        ]
    )
    results = client.query(query, job_config=job_config).result(timeout=45)
    out = []
    for row in results:
        d = dict(row)
        ldap = d.get("ldap")
        out.append({
            "ldap": ldap,
            # Fall back to the static catalog, then to the ldap itself, so the
            # dropdown never shows a blank entry.
            "name": d.get("full_name") or get_manager_name(ldap) or ldap,
            "headcount": int(d.get("headcount") or 0),
        })
    return out


def query_emea_pipeline_data(
    client: bigquery.Client, 
    start_date: Optional[str] = None, 
    end_date: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Fetches EMEA sales pipeline opportunities filtered by close_date range.
    Defaults to last 90 days if no date range is provided. Zero LIMIT applied.
    """
    today = datetime.date.today()
    if not end_date or not str(end_date).strip():
        # Pipeline deals close in future; default to upcoming 365 days for full forward forecasting
        end_date = (today + datetime.timedelta(days=365)).strftime("%Y-%m-%d")
    else:
        end_date = str(end_date).strip()

    if not start_date or not str(start_date).strip():
        start_date = (today - datetime.timedelta(days=90)).strftime("%Y-%m-%d")
    else:
        start_date = str(start_date).strip()

    # NOTE: pso_pipeline is stored at QUOTE-LINE grain - one opportunity can span
    # several rows (1,506 rows for 816 opportunities in EMEA today). Verified grain
    # of the columns we consume:
    #   * total_sale_price_usd            -> LINE level  (varies across lines on 187
    #                                        opps; SUM per opp reconciles to
    #                                        total_sale_price_for_opportunity)  => SUM
    #   * consultant/sce_hours_purchased  -> HEADER level (identical on every line of
    #                                        an opportunity, 0 exceptions)      => ANY_VALUE
    # Collapsing here keeps every downstream consumer (opportunity counts, workload
    # counts, demand FTE) at opportunity grain without needing to dedupe in Python.
    query = """
    WITH src AS (
      SELECT *
      FROM `concord-prod.service_cloudbi.pso_pipeline`
      WHERE pso_region LIKE '%EMEA%'
        AND _PARTITIONDATE = (SELECT MAX(_PARTITIONDATE) FROM `concord-prod.service_cloudbi.pso_pipeline`)
        AND close_date BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)
    )
    SELECT
      opportunity_id,
      ANY_VALUE(opp_name) AS opp_name,
      ANY_VALUE(account_name) AS account_name,
      ANY_VALUE(stage_name) AS stage_name,
      ANY_VALUE(stage_simplified) AS stage_simplified,
      ANY_VALUE(forecast_category) AS forecast_category,
      COALESCE(ANY_VALUE(probability), 0) AS probability,
      'EMEA' AS region,
      COALESCE(ANY_VALUE(country), 'Unknown') AS country,
      COALESCE(ANY_VALUE(project_sub_region), '') AS project_sub_region,
      COALESCE(ANY_VALUE(offering), 'Standard PSO') AS offering,
      COALESCE(LOGICAL_OR(dc_attached), false) AS dc_attached,
      COALESCE(ANY_VALUE(workload_id), opportunity_id) AS workload_id,
      COALESCE(SUM(total_sale_price_usd), 0) AS total_sale_price_usd,
      COALESCE(ANY_VALUE(primary_solution), 'Cloud Solutions') AS solution,
      COALESCE(ANY_VALUE(consultant_hours_purchased), 0) AS consultant_hours_purchased,
      COALESCE(ANY_VALUE(sce_hours_purchased), 0) AS sce_hours_purchased,
      CAST(ANY_VALUE(close_date) AS STRING) AS close_date
    FROM src
    GROUP BY opportunity_id
    ORDER BY close_date DESC
    """
    query = query.replace("__LDAP_EXCLUSIONS__", EXCLUDED_LDAP_SQL)
    try:
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
                bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
            ]
        )
        results = client.query(query, job_config=job_config).result(timeout=45)
        return [dict(row) for row in results]
    except Exception as e:
        logger.warning(f"Pipeline query failed: {e}")
        return []

def query_pipeline_resource_demand(
    client: bigquery.Client,
    horizon_weeks: int = 52,
) -> List[Dict[str, Any]]:
    """Forward resource demand for EMEA Delivery-Center opportunities still in pipeline.

    This answers "how many people is each future project asking for, in which
    skill area, and when" using the real Resource Requests rather than a
    heuristic derived from deal value.

    Two things about this dataset are load-bearing and easy to get wrong:

    1. JOIN ON THE OPPORTUNITY, NOT THE PROJECT. On pre-signature GSD requests
       2,410 of 2,757 rows carry a BLANK (empty-string, never NULL) project_id,
       because the project record does not exist until the deal is signed.
       Joining `weekly_resource_requests` to `projects` on project_id silently
       drops 87% of the demand and makes it look as though pipeline deals have
       no staffing asks at all. Measured coverage of the stage-03 cohort:
       project key 2/110, opportunity key 76/108.

    2. "GSD" IS A PROPERTY OF THE ROLE, NOT THE REGION. request_region is the
       *requesting* region and reads 'Delivery Center' for these rows anyway.
       The Delivery Center roles are `DC Googler - ...` (FTE) and
       `DC Flex - ...` (TVC). `DC Architect - TOC` is excluded: it is the single
       largest DC role by volume and is explicitly out of scope for GSD
       staffing (it never appears in the GSD RR tracker).

    Returned grain is one row per (request, role), carrying its own weekly hour
    spread so the UI can answer "what is the demand in week W" without another
    round-trip. `derived_weekly_request_hours` is the SUM-safe weekly column;
    `actual_total_request_hours` is a header total repeated on every row and
    must never be summed.
    """
    try:
        weeks = int(horizon_weeks)
    except (TypeError, ValueError):
        weeks = 52
    weeks = max(1, min(weeks, 104))

    query = """
    WITH pt_rr AS (
      SELECT MAX(_PARTITIONTIME) AS pt
      FROM `concord-prod.service_cloudbi.weekly_resource_requests`
    ),
    pt_pr AS (
      SELECT MAX(_PARTITIONTIME) AS pt
      FROM `concord-prod.service_cloudbi.projects`
    ),
    -- One row per pipeline opportunity. projects is at project grain and an
    -- opportunity can carry several projects, so collapse before joining or
    -- the weekly hours fan out.
    pipe AS (
      SELECT
        NULLIF(TRIM(eighteen_digit_opp_id), '') AS opp_id,
        ANY_VALUE(opp_name)                     AS opp_name,
        ANY_VALUE(account_name)                 AS account_name,
        ANY_VALUE(opportunity_stage)            AS opportunity_stage,
        ANY_VALUE(opportunity_sub_region)       AS opp_sub_region
      FROM `concord-prod.service_cloudbi.projects`
      WHERE _PARTITIONTIME   = (SELECT pt FROM pt_pr)
        AND project_region   = 'EMEA'
        AND stage_simplified = 'Pipeline'
        AND offering         = 'Delivery Center'
      GROUP BY opp_id
      HAVING opp_id IS NOT NULL
    ),
    rr AS (
      SELECT
        NULLIF(TRIM(opportunity_id), '')        AS opp_id,
        request_name,
        requested_resource_role                 AS role,
        request_week_starting                   AS wk,
        SUM(derived_weekly_request_hours)       AS hrs,
        ANY_VALUE(request_practice)             AS practice,
        ANY_VALUE(primary_skill_certification)  AS skill,
        ANY_VALUE(request_status)               AS request_status,
        ANY_VALUE(approval_status)              AS approval_status,
        ANY_VALUE(staffing_type)                AS staffing_type,
        ANY_VALUE(request_priority)             AS priority,
        ANY_VALUE(request_sub_region)           AS req_sub_region,
        ANY_VALUE(ff_start_date)                AS ff_start,
        ANY_VALUE(ff_end_date)                  AS ff_end,
        ANY_VALUE(NULLIF(TRIM(held_resource_name), ''))      AS held_name,
        ANY_VALUE(NULLIF(TRIM(requested_resource_name), '')) AS named_resource
      FROM `concord-prod.service_cloudbi.weekly_resource_requests`
      WHERE _PARTITIONTIME = (SELECT pt FROM pt_rr)
        AND (requested_resource_role LIKE 'DC Googler%'
          OR requested_resource_role LIKE 'DC Flex%')
        AND requested_resource_role NOT LIKE '%TOC%'
        AND request_status  NOT LIKE 'Cancelled%'
        AND approval_status != 'Rejected'
        AND request_week_starting IS NOT NULL
        AND derived_weekly_request_hours > 0
        -- request_week_starting is Sunday-based, matching BigQuery's default WEEK.
        AND request_week_starting >= DATE_TRUNC(CURRENT_DATE(), WEEK)
        AND request_week_starting <  DATE_ADD(DATE_TRUNC(CURRENT_DATE(), WEEK),
                                              INTERVAL @horizon_weeks WEEK)
      GROUP BY opp_id, request_name, role, wk
    )
    SELECT
      r.request_name,
      r.role,
      p.opp_id,
      p.opp_name,
      p.account_name,
      p.opportunity_stage,
      p.opp_sub_region,
      ANY_VALUE(r.practice)        AS practice,
      ANY_VALUE(r.skill)           AS skill,
      ANY_VALUE(r.request_status)  AS request_status,
      ANY_VALUE(r.approval_status) AS approval_status,
      ANY_VALUE(r.staffing_type)   AS staffing_type,
      ANY_VALUE(r.priority)        AS priority,
      ANY_VALUE(r.req_sub_region)  AS req_sub_region,
      CAST(ANY_VALUE(r.ff_start) AS STRING) AS ff_start,
      CAST(ANY_VALUE(r.ff_end)   AS STRING) AS ff_end,
      ANY_VALUE(r.held_name)       AS held_name,
      ANY_VALUE(r.named_resource)  AS named_resource,
      ROUND(SUM(r.hrs), 1)         AS horizon_hours,
      COUNT(*)                     AS active_weeks,
      -- No ORDER BY inside the aggregate: ordered aggregates have failed
      -- silently against this dataset before. The caller sorts.
      ARRAY_AGG(STRUCT(CAST(r.wk AS STRING) AS w, ROUND(r.hrs, 1) AS h)) AS weeks
    FROM pipe p
    JOIN rr r USING (opp_id)
    GROUP BY r.request_name, r.role, p.opp_id, p.opp_name,
             p.account_name, p.opportunity_stage, p.opp_sub_region
    """

    try:
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("horizon_weeks", "INT64", weeks),
            ]
        )
        results = client.query(query, job_config=job_config).result(timeout=90)
        out: List[Dict[str, Any]] = []
        for row in results:
            d = dict(row)
            spread = [
                {"w": str(w.get("w") or ""), "h": float(w.get("h") or 0.0)}
                for w in (d.get("weeks") or [])
                if w and w.get("w")
            ]
            spread.sort(key=lambda x: x["w"])
            d["weeks"] = spread
            out.append(d)
        return out
    except Exception as e:
        logger.warning(f"Pipeline resource-demand query failed: {e}")
        return []

def fetch_projects_by_ldap(
    ldap: str,
    client: bigquery.Client,
    billing_project_id: str = "concord-prod",
    dataset: str = "service_cloudbi",
) -> List[Dict[str, Any]]:
    """Fetches active/scheduled projects for a resource by LDAP, supporting email extraction."""
    clean_ldap = extract_ldap(ldap)
    query = f"""
        WITH target_resource AS (
            SELECT DISTINCT 
                COALESCE(SPLIT(ldap, '@')[OFFSET(0)], ldap) AS ldap, 
                resource_id
            FROM 
                `{billing_project_id}.{dataset}.scheduled_vs_actual_utilization`
            WHERE 
                LOWER(COALESCE(SPLIT(ldap, '@')[OFFSET(0)], ldap)) = LOWER(@ldap)
                AND _PARTITIONDATE = (SELECT MAX(_PARTITIONDATE) FROM `{billing_project_id}.{dataset}.scheduled_vs_actual_utilization`)
        )
        SELECT DISTINCT
            tr.ldap,
            tr.resource_id,
            ws.resource_name,
            p.project_id,
            p.project_name,
            p.account_name,
            p.project_status,
            p.project_type,
            p.project_start_date,
            p.project_end_date,
            p.vector_account_id,
            p.project_region,
            p.project_practice
        FROM 
            target_resource AS tr
        INNER JOIN 
            `{billing_project_id}.{dataset}.weekly_schedules` AS ws
            ON tr.resource_id = ws.resource_id
        INNER JOIN 
            `{billing_project_id}.{dataset}.projects` AS p
            ON ws.project_id = p.project_id
        WHERE ws._PARTITIONDATE = (SELECT MAX(_PARTITIONDATE) FROM `{billing_project_id}.{dataset}.weekly_schedules`)
        ORDER BY 
            p.project_start_date DESC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("ldap", "STRING", clean_ldap)]
    )
    query_job = client.query(query, job_config=job_config)
    return [dict(row.items()) for row in query_job.result()]

def fetch_accounts_by_ldap(
    ldap: str,
    client: bigquery.Client,
    billing_project_id: str = "concord-prod",
    dataset: str = "service_cloudbi",
) -> List[str]:
    """Fetches list of unique account names associated with a given LDAP."""
    clean_ldap = extract_ldap(ldap)
    query = f"""
        WITH target_resource AS (
            SELECT DISTINCT 
                COALESCE(SPLIT(ldap, '@')[OFFSET(0)], ldap) AS ldap, 
                resource_id
            FROM 
                `{billing_project_id}.{dataset}.scheduled_vs_actual_utilization`
            WHERE 
                LOWER(COALESCE(SPLIT(ldap, '@')[OFFSET(0)], ldap)) = LOWER(@ldap)
                AND _PARTITIONDATE = (SELECT MAX(_PARTITIONDATE) FROM `{billing_project_id}.{dataset}.scheduled_vs_actual_utilization`)
        ),
        ldap_accounts AS (
            SELECT DISTINCT 
                p.vector_account_id
            FROM 
                target_resource AS tr
            INNER JOIN 
                `{billing_project_id}.{dataset}.weekly_schedules` AS ws
                ON tr.resource_id = ws.resource_id
            INNER JOIN 
                `{billing_project_id}.{dataset}.projects` AS p
                ON ws.project_id = p.project_id
            WHERE 
                p.vector_account_id IS NOT NULL
                AND ws._PARTITIONDATE = (SELECT MAX(_PARTITIONDATE) FROM `{billing_project_id}.{dataset}.weekly_schedules`)
        )
        SELECT 
            p.vector_account_id,
            ANY_VALUE(p.account_name) AS account_name,
            COUNT(DISTINCT p.project_id) AS total_account_projects
        FROM 
            `{billing_project_id}.{dataset}.projects` AS p
        INNER JOIN 
            ldap_accounts AS la
            ON p.vector_account_id = la.vector_account_id
        GROUP BY 
            p.vector_account_id
        ORDER BY 
            total_account_projects DESC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("ldap", "STRING", clean_ldap)]
    )
    results = client.query(query, job_config=job_config).result()
    return [row["account_name"] for row in results]

# Backwards compatibility alias
query_emea_dashboard_data = query_emea_delivery_data
