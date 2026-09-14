import datetime
import logging
import os
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
        AND _PARTITIONDATE = (SELECT MAX(_PARTITIONDATE) FROM `concord-prod.service_cloudbi.scheduled_vs_actual_utilization`)
        AND timecard_week_ending BETWEEN PARSE_DATE('%Y-%m-%d', @start_date) AND PARSE_DATE('%Y-%m-%d', @end_date)
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
        ANY_VALUE(COALESCE(engagement_manager, 'Delivery Lead')) AS engagement_manager_name,
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
        p.project_start_date,
        p.project_end_date
      FROM week_assignments a
      LEFT JOIN target_projects p ON a.project_id = p.project_id
      WHERE p.project_end_date IS NULL OR p.project_end_date >= CURRENT_DATE()
    ),
    all_people AS (
      -- Global ldap -> full name directory (~3k people, all regions), used to
      -- resolve manager display names dynamically. This replaces a hardcoded
      -- dictionary, so a newly appointed manager shows up automatically.
      SELECT
        COALESCE(SPLIT(ldap, '@')[OFFSET(0)], ldap) AS ldap,
        ANY_VALUE(full_name) AS full_name
      FROM `concord-prod.service_cloudbi.scheduled_vs_actual_utilization`
      WHERE _PARTITIONDATE = (SELECT MAX(_PARTITIONDATE) FROM `concord-prod.service_cloudbi.scheduled_vs_actual_utilization`)
        AND full_name IS NOT NULL
        AND ldap IS NOT NULL
      GROUP BY 1
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
      IF(a.project_id IS NOT NULL, COALESCE(a.engagement_manager_name, 'PSO Lead'), NULL) AS engagement_manager_name,
      CAST(a.project_start_date AS STRING) AS project_start_date,
      CAST(a.project_end_date AS STRING) AS project_end_date,
      -- NULL when there is no assignment. Previously this fell back to the
      -- person's total hours, inventing a phantom "Delivery Project".
      a.scheduled_timecard_hours AS proj_scheduled_hours
    FROM target_resources r
    LEFT JOIN current_load cl ON r.resource_id = cl.resource_id
    LEFT JOIN active_assignments a ON r.resource_id = a.resource_id
    LEFT JOIN all_people mn ON r.manager_ldap = mn.ldap
    """
    params = [
        bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
        bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
    ]

    clean_org = (org_ldap or "").strip().lower()
    if clean_org and clean_org != "all":
        # Whole management chain, any depth, any region.
        roster_scope = "STRPOS(COALESCE(manager_hierarchy_user_names, ''), @org_token) > 0"
        params.append(
            bigquery.ScalarQueryParameter("org_token", "STRING", f"|{clean_org}|")
        )
    else:
        roster_scope = (
            "( cost_center_name LIKE '%EMEA%' OR cost_center = 'CC1'"
            "  OR region = 'EMEA' OR pso_region = 'EMEA' )"
            " AND (cost_center_name NOT LIKE '%JAPAC%' AND cost_center_name NOT LIKE '%LATAM%'"
            "      AND cost_center_name NOT LIKE '%NORTHAM%')"
            " AND (region NOT LIKE '%AMER%' AND region NOT LIKE '%NorthAM%')"
        )
    query = query.replace("__ROSTER_SCOPE__", roster_scope)

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
        AND STRPOS(manager_hierarchy_user_names, @root_token) > 0
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
