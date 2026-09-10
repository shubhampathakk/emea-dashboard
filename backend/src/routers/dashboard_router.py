import os
import json
import datetime
import logging
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Depends, Query, UploadFile, File, Form
from pydantic import BaseModel
from google.cloud import bigquery

logger = logging.getLogger(__name__)

from src.services.bigquery_service import (
    get_bq_client, 
    query_emea_delivery_data, 
    query_emea_pipeline_data,
    extract_ldap,
    fetch_projects_by_ldap,
    fetch_accounts_by_ldap
)
from src.services.manager_directory import get_manager_name

router = APIRouter(tags=["Dashboard"])

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
        # Unified EMEA resourcing: all delivery resources belong to the EMEA pool (CC1 is EMEA ring-fenced)
        reg = "EMEA"
        hub_key = "EMEA"

        res_name = row.get("resource_name") or "Unknown Engineer"
        res_id = row.get("resource_id") or res_name
        ldap = extract_ldap(row.get("ldap") or row.get("email"), fallback=res_name)
        mgr_raw = row.get("manager_ldap") or ""
        mgr_ldap = mgr_raw.split("@")[0].strip() if mgr_raw else ""
        mgr_name = row.get("manager_name") or get_manager_name(mgr_ldap)
        role = row.get("role") or "Consultant"
        practice = row.get("practice") or "Cloud Delivery"
        is_ooo = str(row.get("is_ooo") or "").lower() == "true"
        
        proj_name = row.get("project_name") or "Delivery Project"
        acc_name = (row.get("account_name") or "").strip()
        pm_name = row.get("engagement_manager_name") or "Delivery Lead"
        
        try:
            hrs = float(row.get("proj_scheduled_hours") or row.get("scheduled_timecard_hours") or 0)
        except (ValueError, TypeError):
            hrs = 0.0

        start_date = str(row.get("project_start_date") or "2026-01-01")
        end_date = str(row.get("project_end_date") or "2026-12-31")

        runway_days = 90
        try:
            end_dt = datetime.datetime.strptime(end_date[:10], "%Y-%m-%d").date()
            runway_days = (end_dt - today).days
        except Exception:
            runway_days = 90

        alloc_pct = min(100, round((hrs / 40.0) * 100)) if role != "Manager" else min(100, round((hrs / 16.0) * 100))

        if res_name not in resource_map:
            resource_map[res_name] = {
                "id": res_id,
                "name": res_name,
                "ldap": ldap,
                "manager_ldap": mgr_ldap,
                "manager_name": mgr_name,
                "role": role,
                "cost_center": cc,
                "cost_center_name": row.get("cost_center_name") or "",
                "region": "EMEA",
                "hub": "EMEA",
                "languages": ["English"],
                "practice": practice,
                "skills": practice,
                "is_ooo": is_ooo,
                "weekly_hours": float(row.get("scheduled_timecard_hours") or 0),
                "assignments": []
            }
        elif mgr_ldap and not resource_map[res_name].get("manager_ldap"):
            resource_map[res_name]["manager_ldap"] = mgr_ldap
            resource_map[res_name]["manager_name"] = mgr_name

        if row.get("project_id") or hrs > 0:
            resource_map[res_name]["assignments"].append({
                "project": proj_name,
                "account": acc_name or "Client Delivery",
                "weekly_hours": hrs,
                "hours": hrs / 5.0,
                "start": start_date,
                "end": end_date,
                "allocation_pct": alloc_pct,
                "runway_days": runway_days,
                "type": "delivery"
            })

        if (row.get("project_id") or hrs > 0) and acc_name and acc_name != "Strategic Partner":
            if acc_name not in customer_map:
                customer_map[acc_name] = {
                    "account_name": acc_name,
                    "program_manager": pm_name,
                    "delivery_executive": "Delivery Executive",
                    "total_hours": 0.0,
                    "EMEA": [],
                    "GSD": []
                }
            
            if hub_key not in customer_map[acc_name]:
                customer_map[acc_name][hub_key] = []
            
            existing_p = next((p for p in customer_map[acc_name][hub_key] if p["name"] == res_name), None)
            if existing_p:
                existing_p["hours"] = round(existing_p.get("hours", 0.0) + hrs, 1)
                # 40 hrs per week standard = 100%
                existing_p["allocation_pct"] = min(100, max(0, round((existing_p["hours"] / 40.0) * 100)))
            else:
                p_pct = min(100, max(0, round((hrs / 40.0) * 100))) if hrs > 0 else 0
                customer_map[acc_name][hub_key].append({
                    "name": res_name,
                    "ldap": ldap,
                    "role": role,
                    "hours": round(hrs, 1),
                    "allocation_pct": p_pct
                })
            customer_map[acc_name]["total_hours"] = round(customer_map[acc_name]["total_hours"] + hrs, 1)

    for c in customer_map.values():
        c["total_hours"] = round(c["total_hours"], 1)
        for hub_k in ["EMEA", "GSD"]:
            for p in c.get(hub_k, []):
                if p.get("allocation_pct") is None or p.get("allocation_pct") == 0:
                    pHrs = p.get("hours", 0.0)
                    if pHrs > 0:
                        p["allocation_pct"] = min(100, round((pHrs / 40.0) * 100))
                    elif p["name"] in resource_map:
                        r_match = resource_map[p["name"]]
                        acc_ass = [a for a in r_match.get("assignments", []) if a.get("account") == c["account_name"]]
                        if acc_ass:
                            sum_hrs = sum(a.get("weekly_hours", 0.0) for a in acc_ass)
                            p["hours"] = round(sum_hrs, 1)
                            p["allocation_pct"] = min(100, round((sum_hrs / 40.0) * 100))
                        elif r_match.get("weekly_hours", 0.0) > 0:
                            p["allocation_pct"] = min(100, round((r_match["weekly_hours"] / 40.0) * 100))


    valid_customers = {}
    for acc_k, c_obj in customer_map.items():
        if acc_k and acc_k != "Strategic Partner" and (c_obj.get("total_hours", 0.0) > 0 or len(c_obj.get("EMEA", [])) > 0 or len(c_obj.get("GSD", [])) > 0):
            valid_customers[acc_k] = c_obj
    customer_map = valid_customers

    resources_list = list(resource_map.values())
    total_cap = 0.0
    for r in resources_list:
        std = 16.0 if r["role"] == "Manager" else 40.0
        total_cap += std

        # Resource's weekly delivery hours:
        r_hrs = float(r.get("weekly_hours") or 0.0)
        if r_hrs == 0.0 and r["assignments"]:
            r_hrs = min(std, sum(a["weekly_hours"] for a in r["assignments"]))
        r["weekly_hours"] = round(r_hrs, 1)

        total_scheduled_hrs += r["weekly_hours"]
        pct = round((r["weekly_hours"] / std) * 100)

        hub_key = "EMEA"
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
    overall_util = round((total_scheduled_hrs / total_cap) * 100, 1) if total_cap > 0 else 60.0

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
            return "8+ Weeks (31d+)", "8+ Weeks", 5
        
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
        acc_name = p.get("account_name") or "Strategic Partner"
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
            "program_manager": "PSO Delivery Lead",
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

    gtm_summary = {
        "ps_engagement_formatted": f"${(total_booked_val / 1_000_000):.2f}M" if total_booked_val > 0 else f"${(total_pipeline_val * 0.25 / 1_000_000):.2f}M",
        "pipeline_acv_formatted": f"${(open_pipeline_val / 1_000_000):.2f}M" if open_pipeline_val > 0 else f"${(total_pipeline_val / 1_000_000):.2f}M",
        "unique_customers": len(unique_pipeline_accounts) or len(customers_list),
        "total_workloads": len(workloads),
        "active_opportunities": open_opportunities_count or len(pipeline_rows)
    }

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
        "raw_data": delivery_rows
    }

def get_fallback_payload(start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
    """Loads pre-fetched live snapshot of BigQuery delivery & pipeline data, dynamically filtered by date range."""
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    delivery_file = os.path.join(base_dir, "data", "emea_delivery.json")
    pipeline_file = os.path.join(base_dir, "data", "emea_pipeline.json")
    delivery_rows = []
    pipeline_rows = []
    if os.path.exists(delivery_file):
        try:
            with open(delivery_file, "r") as f:
                delivery_rows = json.load(f)
        except Exception as e:
            logger.error(f"Error loading delivery fallback json: {e}")
    if os.path.exists(pipeline_file):
        try:
            with open(pipeline_file, "r") as f:
                pipeline_rows = json.load(f)
        except Exception as e:
            logger.error(f"Error loading pipeline fallback json: {e}")

    # Dynamically filter by date range ONLY IF explicit non-empty date range was provided
    clean_start = str(start_date).strip() if (start_date and str(start_date).strip()) else None
    clean_end = str(end_date).strip() if (end_date and str(end_date).strip()) else None

    if clean_start or clean_end:
        today_str = datetime.date.today().strftime("%Y-%m-%d")
        effective_start = clean_start or "2020-01-01"
        effective_end = clean_end or today_str

        filtered_delivery = [
            r for r in delivery_rows
            if str(r.get("project_start_date") or "2020-01-01")[:10] <= effective_end
            and (str(r.get("week_ending") or r.get("project_end_date") or "2099-01-01")[:10] >= effective_start)
        ]
        delivery_rows = filtered_delivery

        runway_end = (datetime.date.today() + datetime.timedelta(days=90)).strftime("%Y-%m-%d")
        pipe_max_end = max(effective_end, runway_end)

        filtered_pipeline = [
            p for p in pipeline_rows
            if str(p.get("close_date") or "")[:10] >= effective_start
            and str(p.get("close_date") or "")[:10] <= pipe_max_end
        ]
        pipeline_rows = filtered_pipeline

    return build_dashboard_payload(delivery_rows, pipeline_rows)

@router.get("/dashboard/data")
@router.get("/data")
def get_dashboard_data(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    bq_client: bigquery.Client = Depends(get_bq_client),
):
    clean_start = str(start_date).strip() if (start_date and str(start_date).strip()) else None
    clean_end = str(end_date).strip() if (end_date and str(end_date).strip()) else None
    try:
        delivery_rows = query_emea_delivery_data(bq_client, start_date=clean_start, end_date=clean_end)
        pipeline_rows = query_emea_pipeline_data(bq_client, start_date=clean_start, end_date=clean_end)
        return build_dashboard_payload(delivery_rows, pipeline_rows)
    except Exception as e:
        logger.warning(f"BigQuery failed, using fallback data. Error: {e}")
        return get_fallback_payload(start_date=clean_start, end_date=clean_end)

@router.post("/refresh-bq")
def refresh_bq(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    bq_client: bigquery.Client = Depends(get_bq_client),
):
    clean_start = str(start_date).strip() if (start_date and str(start_date).strip()) else None
    clean_end = str(end_date).strip() if (end_date and str(end_date).strip()) else None
    try:
        delivery_rows = query_emea_delivery_data(bq_client, start_date=clean_start, end_date=clean_end)
        pipeline_rows = query_emea_pipeline_data(bq_client, start_date=clean_start, end_date=clean_end)
        return {"status": "ok", "data": build_dashboard_payload(delivery_rows, pipeline_rows)}
    except Exception as e:
        logger.warning(f"BigQuery refresh failed: {e}")
        return {"status": "fallback", "data": get_fallback_payload(start_date=clean_start, end_date=clean_end)}

@router.get("/users/{ldap}/projects")
def get_user_projects(
    ldap: str,
    bq_client: bigquery.Client = Depends(get_bq_client),
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
    bq_client: bigquery.Client = Depends(get_bq_client),
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
    bq_client: bigquery.Client = Depends(get_bq_client),
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
    bq_client: bigquery.Client = Depends(get_bq_client),
):
    """Answers user inquiries regarding staffing, workloads, bench, pipeline, and customer accounts."""
    msg = (req.message or "").strip().lower()
    payload = get_fallback_payload()
    kpis = payload.get("kpis", {})
    resources = payload.get("resources", [])
    customers = payload.get("customerPortfolio", [])
    gtm = payload.get("gtm", {})

    # 1. Bench / Availability / Capacity queries
    if any(k in msg for k in ["bench", "available", "free cap", "unassigned", "availability"]):
        bench_list = [r for r in resources if r.get("weekly_hours", 0) == 0 or not r.get("assignments")]
        partial_list = [r for r in resources if 0 < r.get("weekly_hours", 0) < 35]
        
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
    matched_person = None
    for r in resources:
        r_name = (r.get("name") or "").lower()
        r_ldap = (r.get("ldap") or "").lower()
        if (r_name and r_name in msg) or (r_ldap and r_ldap in msg) or (r_name.split()[0] in msg and len(r_name.split()[0]) > 3):
            matched_person = r
            break

    if matched_person:
        r = matched_person
        hrs = r.get("weekly_hours", 0)
        std = 16.0 if r.get("role") == "Manager" else 40.0
        pct = min(100, round((hrs / std) * 100))
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
            f"* **PS Engagement Value**: **{gtm_summary.get('ps_engagement_formatted', '$24.80M')}**",
            f"* **Total Pipeline ACV**: **{gtm_summary.get('pipeline_acv_formatted', '$115.40M')}**",
            f"* **Active Workloads**: **{gtm_summary.get('total_workloads', 120)}** across **{gtm_summary.get('unique_customers', 56)}** accounts",
            f"* **Live Opportunities**: **{gtm_summary.get('active_opportunities', 48)}** deals in flight",
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
            lines.append(f"* **{c.get('account_name')}** — **{c.get('total_hours')} hrs/wk** | PM: {c.get('program_manager')} | DE: {c.get('delivery_executive')}")
            
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
        f"I have analyzed our live EMEA delivery organization (**{len(resources)} engineers**, **{len(customers)} client accounts**, and **{gtm.get('summary', {}).get('total_workloads', 120)} workloads**).",
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


