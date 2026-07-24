from flask import Flask, jsonify, request, send_from_directory

from flask_cors import CORS

from google.cloud import bigquery

from google.oauth2 import service_account

import os

import re

import json

import math

import datetime

import traceback

import pandas as pd

import anthropic

import logging

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

logger = logging.getLogger("datagenie")

creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")

if creds_json:
    _bq_credentials = service_account.Credentials.from_service_account_info(
        json.loads(creds_json),
        scopes=["https://www.googleapis.com/auth/bigquery"]
    )
elif not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
    raise RuntimeError(
        "BigQuery credentials not configured. Set GOOGLE_APPLICATION_CREDENTIALS_JSON "
        "(preferred, JSON content) or GOOGLE_APPLICATION_CREDENTIALS (path to file)."
    )
else:
    _bq_credentials = None  # use GOOGLE_APPLICATION_CREDENTIALS file path

app = Flask(__name__)

CORS(app)

# ── Cross-service URLs ──
# NLQ's derived-metric feature answers questions like "what's my utilization"
# by hitting the SAME real endpoints the dashboards use — this used to be an
# in-process function call when everything lived in one Flask app; now that
# R3ID and Report Generator are separate services, it's a real HTTP call.
# Set these to each service's actual deployed URL once they're live.
R3ID_SERVICE_URL = os.environ.get("R3ID_SERVICE_URL", "").rstrip("/")
REPORT_GENERATOR_SERVICE_URL = os.environ.get("REPORT_GENERATOR_SERVICE_URL", "").rstrip("/")

def _fetch_from_service(base_url, path, params, service_label):
    """GET a JSON endpoint on another Data Genie service. Raises a clear error
    if the URL isn't configured yet, instead of a confusing connection failure."""
    if not base_url:
        raise RuntimeError(
            f"{service_label} service URL is not configured. Set the "
            f"{'R3ID_SERVICE_URL' if service_label == 'R3ID' else 'REPORT_GENERATOR_SERVICE_URL'} "
            "environment variable to that service's deployed URL."
        )
    resp = requests.get(f"{base_url}{path}", params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

@app.route("/")
@app.route("/nlq.html")
def serve_frontend():
    """Serve the DataGenie frontend."""
    return send_from_directory(os.path.dirname(os.path.abspath(__file__)), "nlq.html")

client = bigquery.Client(credentials=_bq_credentials) if _bq_credentials else bigquery.Client()

_anthropic_api_key = os.environ.get("ANTHROPIC_API_KEY")

_anthropic_client = anthropic.Anthropic(api_key=_anthropic_api_key) if _anthropic_api_key else None

if not _anthropic_client:
    logger.warning("ANTHROPIC_API_KEY not set — /analytics/nlq will be unavailable.")

def handle_error(endpoint_name: str, e: Exception):
    """Log exception server-side; return a safe generic error to the client."""
    logger.exception(f"Error in {endpoint_name}: {e}")
    return jsonify({"error": "An internal error occurred. Check server logs."}), 500

@app.errorhandler(Exception)
def handle_unhandled_exception(e):
    """Catch any unhandled exception and return JSON instead of HTML 500."""
    logger.exception(f"Unhandled exception: {e}")
    return jsonify({"error": "An internal error occurred."}), 500

DATABASE_INDEX_SHEET_ID = "1plJGqLHT-9u90ojvAKnrtA0edTmTr45suMV2OnQfx_c"

DATABASE_INDEX_ACCOUNTS_GID = "1162564586"

_user_groups_cache = {"data": None, "fetched_at": None}

_USER_GROUPS_CACHE_TTL_SECONDS = 600  # 10 minutes

def get_user_groups(force_refresh=False):
    """Fetches the Database_Index 'Accounts' tab's System / Team / System UserID
    columns and returns {"bigquery": {team: [userIds]}, "camstar": {team: [userIds]}}.
    Cached in memory for _USER_GROUPS_CACHE_TTL_SECONDS to avoid hitting the
    sheet on every request. No credentials needed — reads the sheet's public
    CSV export (same access level as viewing it in a browser)."""
    now = datetime.datetime.utcnow()
    cached = _user_groups_cache["data"]
    fetched_at = _user_groups_cache["fetched_at"]
    if (not force_refresh and cached is not None and fetched_at is not None
            and (now - fetched_at).total_seconds() < _USER_GROUPS_CACHE_TTL_SECONDS):
        return cached

    csv_url = (f"https://docs.google.com/spreadsheets/d/{DATABASE_INDEX_SHEET_ID}"
               f"/export?format=csv&gid={DATABASE_INDEX_ACCOUNTS_GID}")
    try:
        df = pd.read_csv(csv_url)
    except Exception as e:
        logger.error(f"Failed to fetch Database_Index sheet: {e}")
        return cached if cached is not None else {"bigquery": {}, "camstar": {}}

    df.columns = [str(c).strip() for c in df.columns]
    result = {"bigquery": {}, "camstar": {}}
    for _, row in df.iterrows():
        system = str(row.get("System", "")).strip()
        team = str(row.get("Team", "")).strip()
        user_id = str(row.get("System UserID", "")).strip()
        if system not in ("BigQuery", "Camstar") or not team or not user_id or user_id.lower() == "nan":
            continue
        bucket = "bigquery" if system == "BigQuery" else "camstar"
        result[bucket].setdefault(team, []).append(user_id)

    _user_groups_cache["data"] = result
    _user_groups_cache["fetched_at"] = now
    return result

@app.route("/analytics/user-groups", methods=["GET"])
def user_groups():
    try:
        force_refresh = request.args.get("refresh", "").lower() == "true"
        return jsonify(get_user_groups(force_refresh=force_refresh))
    except Exception as e:
        return handle_error(request.endpoint, e)

PROJECT = "restor3d-data-warehouse"

DATASET = "production3_r3id_public"

def tbl(name):
    return f"`{PROJECT}.{DATASET}.{name}`"

PRODUCT_GROUPS = {
    'Upper Extremity': [
        'Anatomic Shoulder Arthroplasty','Hemiarthroplasty Shoulder',
        'Reverse Shoulder Arthroplasty - Glenoid Baseplate',
        'Reverse Shoulder Arthroplasty - Glenosphere Only',
        'Custom rTSA - Custom Glenoid + Veritas OTS Components',
        'Reverse Total Shoulder Arthroplasty',
        'Reverse Shoulder Arthroplasty - Proximal Humerus',
        'Custom rTSA - Other',
        'Acromion','Clavicle','Shoulder',
        'Hemiarthroplasty Elbow','Total Elbow Arthroplasty','Elbow Fusion',
        'Total Wrist Arthroplasty','Hand/Wrist Fusion',
        'Hemiarthroplasty Hand/Wrist','Carpal Replacement',
        'Corrective Osteotomy Hand/Wrist','Corrective Osteotomy - Arm',
        'Segmental Defect - Arm','Bone Models - Arm','Prosthetic - Arm',
    ],
    'Lower Extremity': [
        'Total Ankle Replacement','PSR','Ankle Fusion',
        'Corrective Osteotomy Ankle','Corrective Osteotomy Foot',
        'Corrective Osteotomy - Leg','Hindfoot Fusion','Midfoot','MPJ/MTP',
        'Total Talus and Other Arthroplasty','Hemitalus',
        'Antibiotic Spacer','Hemiarthroplasty Ankle','Unknown - Foot/Ankle',
        'Segmental Defect - Leg','Prosthetic - Leg',
        'Bone Models - Ankle','Bone Models - Foot',
        'Corrective Osteotomy','Prosthetic','Segmental Defect',
    ],
    'Knee': ['Total Knee Arthroplasty','TKA','Hemiarthroplasty Knee'],
    'Hip': ['Hip Arthroplasty','Hip Hemipelvis','Hip Hemiarthroplasty'],
    'Craniofacial': ['Mandible','Maxilla','General Reconstruction'],
    'Spine': ['Lumbar'],
}

CLEARED_PRODUCTS = [
    'Reverse Shoulder Arthroplasty - Glenoid Baseplate',
    'Reverse Shoulder Arthroplasty - Glenosphere Only',
    'Reverse Total Shoulder Arthroplasty',
    'Total Ankle Replacement',
]

METRIC_MAP = {
    'total_lt':   ("TIMESTAMP_DIFF(s.ship_wrk_comp_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days,0)", "Total LT", "s.ship_wrk_comp_date"),
    'seg_lt':     ("TIMESTAMP_DIFF(sd.seg_review_date, s.first_scan_upload_date, DAY)", "Segmentation LT", "sd.seg_review_date"),
    'surgeon_lt': ("TIMESTAMP_DIFF(sd.surgeon_approval_date, sd.first_psp_review_date, DAY)", "Surgeon Approval LT", "sd.surgeon_approval_date"),
    'digital_lt': ("GREATEST(0, LEAST(TIMESTAMP_DIFF(sd.peer_review_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days,0) - GREATEST(0, COALESCE(TIMESTAMP_DIFF(sd.surgeon_approval_date, sd.first_psp_review_date, DAY), 0)), COALESCE(TIMESTAMP_DIFF(s.ship_wrk_comp_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days,0), TIMESTAMP_DIFF(sd.peer_review_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days,0))))", "Digital Production LT", "sd.first_psp_review_date"),
    'volume':     ("COUNT(DISTINCT f.id)", "Volume", "sd.first_psp_review_date"),
}

def on_hold_cte():
    return f"""
    on_hold_time AS (
        SELECT refId as caseId, SUM(hold_days) as total_hold_days
        FROM (
            SELECT refId,
                CASE
                    WHEN type = 'r3idCaseRemovedFromOnHold'
                        THEN TIMESTAMP_DIFF(CAST(timestamp AS DATETIME), CAST(LAG(timestamp) OVER (PARTITION BY refId ORDER BY timestamp) AS DATETIME), DAY)
                    WHEN type = 'r3idCasePutOnHold'
                        AND LEAD(type) OVER (PARTITION BY refId ORDER BY timestamp) IS NULL
                        THEN TIMESTAMP_DIFF(CAST(CURRENT_TIMESTAMP() AS DATETIME), CAST(timestamp AS DATETIME), DAY)
                    ELSE 0
                END as hold_days
            FROM {tbl('vw_event_log_rank')}
            WHERE type IN ('r3idCasePutOnHold','r3idCaseRemovedFromOnHold')
        )
        GROUP BY refId
    )"""

def on_hold_user_cte():
    return f"""
    on_hold_user AS (
        SELECT refId as caseId, userId as put_on_hold_by, timestamp as put_on_hold_at
        FROM (
            SELECT refId, userId, timestamp,
                ROW_NUMBER() OVER (PARTITION BY refId ORDER BY timestamp DESC) as rn
            FROM {tbl('vw_event_log_rank')}
            WHERE type = 'r3idCasePutOnHold'
        )
        WHERE rn = 1
    )"""

def signoff_cte():
    return f"""
    signoff_dates AS (
        SELECT
            w.caseId,
            MIN(CASE WHEN wm.name = 'Segmentation' AND w.workModuleUserType = 'REVIEWER' AND w.workModuleSignatureType = 'ACCEPT' THEN DATE(CAST(w.createdAt AS TIMESTAMP)) END) as seg_review_date,
            MIN(CASE WHEN wm.name = 'Design Call Prep' AND w.workModuleUserType = 'REVIEWER' AND w.workModuleSignatureType = 'ACCEPT' THEN DATE(CAST(w.createdAt AS TIMESTAMP)) END) as design_call_prep_date,
            MAX(CASE WHEN wm.name = 'Proposed Surgical Plan' AND w.workModuleUserType = 'APPROVER' AND w.workModuleSignatureType = 'ACCEPT' THEN DATE(CAST(w.createdAt AS TIMESTAMP)) END) as surgeon_approval_date,
            MAX(CASE WHEN wm.name = 'Proposed Surgical Plan' AND w.workModuleUserType = 'REVIEWER' AND w.workModuleSignatureType = 'ACCEPT' THEN DATE(CAST(w.createdAt AS TIMESTAMP)) END) as first_psp_review_date,
            MIN(CASE WHEN wm.name = 'Peer Review' AND w.workModuleUserType = 'REVIEWER' AND w.workModuleSignatureType = 'ACCEPT' THEN DATE(CAST(w.createdAt AS TIMESTAMP)) END) as peer_review_date,
            -- True if the most recent PSP APPROVER signoff is a REJECT (surgeon rejected, case sent back for rework).
            -- When true the case should NOT sit in surgeon_approval — it belongs back in psp_design.
            MAX(CASE WHEN wm.name = 'Proposed Surgical Plan' AND w.workModuleUserType = 'APPROVER' AND w.workModuleSignatureType = 'REJECT' THEN w.createdAt END) > MAX(CASE WHEN wm.name = 'Proposed Surgical Plan' AND w.workModuleUserType = 'APPROVER' AND w.workModuleSignatureType = 'ACCEPT' THEN w.createdAt END) as surgeon_last_action_reject
        FROM {tbl('WorkModuleSignoff')} w
        JOIN {tbl('WorkModuleInstance')} wi ON w.workModuleInstanceId = wi.id
        JOIN {tbl('WorkModule')} wm ON wi.workModuleId = wm.id
        WHERE w.deleted = false
        GROUP BY w.caseId
    )"""

def case_select_fields(include_hold_user=False):
    hold_cols = ", hu.put_on_hold_by, hu.put_on_hold_at" if include_hold_user else ""
    return f"""
        f.id, CONCAT(f.count, '_', f.alias) as alias, f.alias as alias_short, f.count as case_number, f.case_category_name, f.phase,
        f.phy_nameFirst, f.phy_nameLast, f.fac_name, f.fac_state,
        f.laterality, f.onHold, f.createdAt,
        s.first_scan_upload_date, s.ship_wrk_comp_date,
        sd.seg_review_date, sd.first_psp_review_date, sd.surgeon_approval_date,
        COALESCE(oh.total_hold_days, 0) as onhold_days{hold_cols},
        CASE WHEN s.ship_wrk_comp_date IS NOT NULL AND s.first_scan_upload_date IS NOT NULL
            THEN TIMESTAMP_DIFF(s.ship_wrk_comp_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days, 0)
            ELSE NULL END as total_lt,
        CASE WHEN sd.seg_review_date IS NOT NULL AND s.first_scan_upload_date IS NOT NULL
            THEN TIMESTAMP_DIFF(sd.seg_review_date, s.first_scan_upload_date, DAY)
            ELSE NULL END as seg_lt,
        CASE WHEN sd.surgeon_approval_date IS NOT NULL AND sd.first_psp_review_date IS NOT NULL
            THEN TIMESTAMP_DIFF(sd.surgeon_approval_date, sd.first_psp_review_date, DAY)
            ELSE NULL END as surgeon_lt,
        CASE WHEN sd.peer_review_date IS NOT NULL AND s.first_scan_upload_date IS NOT NULL
            THEN GREATEST(0, LEAST(
                TIMESTAMP_DIFF(sd.peer_review_date, s.first_scan_upload_date, DAY)
                    - COALESCE(oh.total_hold_days, 0)
                    - GREATEST(0, COALESCE(TIMESTAMP_DIFF(sd.surgeon_approval_date, sd.first_psp_review_date, DAY), 0)),
                COALESCE(
                    TIMESTAMP_DIFF(s.ship_wrk_comp_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days, 0),
                    TIMESTAMP_DIFF(sd.peer_review_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days, 0)
                )
            ))
            ELSE NULL END as digital_lt,
        f.onHoldComment as oh_note,
        cn.last_case_note, cn.last_case_note_author, cn.last_case_note_date,
        cn.last_internal_case_note, cn.last_internal_case_note_author, cn.last_internal_case_note_date,
        cn.last_design_case_note, cn.last_design_case_note_author, cn.last_design_case_note_date"""

def case_joins(include_hold_user=False):
    hold_join = f"\n        LEFT JOIN on_hold_user hu ON f.id = hu.caseId" if include_hold_user else ""
    return f"""
        FROM {tbl('vw_fact_case')} f
        LEFT JOIN {tbl('vw_lkup_stage_log_dates')} s ON f.id = s.caseId
        LEFT JOIN signoff_dates sd ON f.id = sd.caseId
        LEFT JOIN on_hold_time oh ON f.id = oh.caseId
        LEFT JOIN {tbl('vw_lkup_last_case_notes')} cn ON f.id = cn.caseid{hold_join}"""

def fetch_cases_by_ids_query(case_ids, include_hold_user=False):
    """Return a query that fetches full case details for the given IDs.
    Uses raw Case table LEFT JOIN vw_fact_case so COMPLETED phase cases are included."""
    hold_join = f"\n        LEFT JOIN on_hold_user hu ON f.id = hu.caseId" if include_hold_user else ""
    hold_cols = ", hu.put_on_hold_by, hu.put_on_hold_at" if include_hold_user else ""
    ids_str = "','".join(str(i) for i in case_ids)
    return f"""
    WITH {signoff_cte()}, {on_hold_cte()}{',' + on_hold_user_cte() if include_hold_user else ''}
    SELECT
        base.id,
        COALESCE(CONCAT(f.count, '_', f.alias), CAST(base.id AS STRING)) as alias,
        COALESCE(f.alias, CAST(base.id AS STRING)) as alias_short,
        f.count as case_number,
        COALESCE(f.case_category_name, cc.name) as case_category_name,
        COALESCE(f.phase, base.phase) as phase,
        f.phy_nameFirst, f.phy_nameLast, f.fac_name, f.fac_state,
        COALESCE(f.laterality, base.laterality) as laterality,
        COALESCE(f.onHold, base.onHold) as onHold,
        COALESCE(f.createdAt, base.createdAt) as createdAt,
        s.first_scan_upload_date, s.ship_wrk_comp_date,
        sd.seg_review_date, sd.first_psp_review_date, sd.surgeon_approval_date,
        COALESCE(oh.total_hold_days, 0) as onhold_days{hold_cols},
        CASE WHEN s.ship_wrk_comp_date IS NOT NULL AND s.first_scan_upload_date IS NOT NULL
            THEN TIMESTAMP_DIFF(s.ship_wrk_comp_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days, 0)
            ELSE NULL END as total_lt,
        CASE WHEN sd.seg_review_date IS NOT NULL AND s.first_scan_upload_date IS NOT NULL
            THEN TIMESTAMP_DIFF(sd.seg_review_date, s.first_scan_upload_date, DAY)
            ELSE NULL END as seg_lt,
        CASE WHEN sd.surgeon_approval_date IS NOT NULL AND sd.first_psp_review_date IS NOT NULL
            THEN TIMESTAMP_DIFF(sd.surgeon_approval_date, sd.first_psp_review_date, DAY)
            ELSE NULL END as surgeon_lt,
        CASE WHEN sd.peer_review_date IS NOT NULL AND s.first_scan_upload_date IS NOT NULL
            THEN GREATEST(0, LEAST(
                TIMESTAMP_DIFF(sd.peer_review_date, s.first_scan_upload_date, DAY)
                    - COALESCE(oh.total_hold_days, 0)
                    - GREATEST(0, COALESCE(TIMESTAMP_DIFF(sd.surgeon_approval_date, sd.first_psp_review_date, DAY), 0)),
                COALESCE(
                    TIMESTAMP_DIFF(s.ship_wrk_comp_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days, 0),
                    TIMESTAMP_DIFF(sd.peer_review_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days, 0)
                )
            ))
            ELSE NULL END as digital_lt,
        f.onHoldComment as oh_note,
        cn.last_case_note, cn.last_case_note_author, cn.last_case_note_date,
        cn.last_internal_case_note, cn.last_internal_case_note_author, cn.last_internal_case_note_date,
        cn.last_design_case_note, cn.last_design_case_note_author, cn.last_design_case_note_date
    FROM {tbl('Case')} base
    JOIN {tbl('CaseCategory')} cc ON base.caseCategoryId = cc.id
    LEFT JOIN {tbl('vw_fact_case')} f ON base.id = f.id
    LEFT JOIN {tbl('vw_lkup_stage_log_dates')} s ON base.id = s.caseId
    LEFT JOIN signoff_dates sd ON base.id = sd.caseId
    LEFT JOIN on_hold_time oh ON base.id = oh.caseId
    LEFT JOIN {tbl('vw_lkup_last_case_notes')} cn ON base.id = cn.caseid{hold_join}
    WHERE base.id IN ('{ids_str}')
    ORDER BY base.createdAt DESC
    LIMIT 500
    """

def volume_case_joins():
    """For volume queries: bypass vw_fact_case (which excludes COMPLETED cases)
    and query raw Case table so completed cases are counted too."""
    return f"""
        FROM {tbl('Case')} f
        LEFT JOIN {tbl('CaseCategory')} cc ON f.caseCategoryId = cc.id
        LEFT JOIN {tbl('CaseType')} ct ON f.caseTypeId = ct.id
        LEFT JOIN signoff_dates sd ON f.id = sd.caseId
        LEFT JOIN {tbl('vw_lkup_stage_log_dates')} s ON f.id = s.caseId"""

def volume_where_clause(args):
    """Build WHERE clause for volume using raw Case table columns.
    No phase filter — includes COMPLETED cases for accurate volume counting.
    Includes cancelled cases — PSP completion is the milestone; post-PSP cancellation should not affect volume."""
    conditions = ["f.deleted = false"]
    # Product filter — use cc.name (CaseCategory.name) instead of f.case_category_name
    product = args.get('product', '').strip()
    product_group = args.get('product_group', '').strip()
    if product:
        products = [p.strip() for p in product.split(',')]
        joined = "','".join(products)
        conditions.append(f"cc.name IN ('{joined}')" if len(products) > 1 else f"cc.name = '{products[0]}'")
    elif product_group:
        all_prods = []
        for g in [g.strip() for g in product_group.split(',')]:
            all_prods.extend(PRODUCT_GROUPS.get(g, []))
        if all_prods:
            joined = "','".join(all_prods)
            conditions.append(f"cc.name IN ('{joined}')")
    # No product selected — no default filter, returns all products
    # Surgeon filter
    surgeon = args.get('surgeon', '').strip()
    if surgeon:
        conditions.append(f"LOWER(f.alias) LIKE LOWER('%{sanitize(surgeon)}%')")
    # Case type filter
    case_type = args.get('case_type', '').strip()
    if case_type:
        types = [t.strip() for t in case_type.split(',')]
        joined = "','".join(types)
        conditions.append(f"ct.name IN ('{joined}')" if len(types) > 1 else f"ct.name = '{types[0]}'")
    # Step + User filter via WorkModuleSignoff subquery (mirrors build_where_clause logic
    # so the User dropdown and the Designed/Reviewed toggle apply to Volume too).
    step_val = args.get('step', '').strip()
    user_val = args.get('step_user', '').strip()
    if step_val or user_val:
        sub = ["w_flt.deleted = false", "w_flt.workModuleSignatureType = 'ACCEPT'"]
        if step_val:
            steps = [s.strip() for s in step_val.split(',')]
            if len(steps) == 1:
                sub.append(f"wm_flt.name = '{steps[0]}'")
            else:
                joined = "','".join(steps)
                sub.append(f"wm_flt.name IN ('{joined}')")
        if user_val:
            users = [u.strip() for u in user_val.split(',')]
            if len(users) == 1:
                sub.append(f"w_flt.signature = '{users[0]}'")
            else:
                joined = "','".join(users)
                sub.append(f"w_flt.signature IN ('{joined}')")
            user_role = args.get('user_role', '').strip().lower()
            if user_role == 'worker':
                sub.append("w_flt.workModuleUserType = 'WORKER'")
            elif user_role == 'reviewer':
                sub.append("w_flt.workModuleUserType IN ('REVIEWER', 'APPROVER')")
        sub_where = " AND ".join(sub)
        conditions.append(f"""f.id IN (
            SELECT DISTINCT w_flt.caseId
            FROM `{PROJECT}.{DATASET}.WorkModuleSignoff` w_flt
            LEFT JOIN `{PROJECT}.{DATASET}.WorkModuleInstance` wi_flt ON w_flt.workModuleInstanceId = wi_flt.id
            LEFT JOIN `{PROJECT}.{DATASET}.WorkModule` wm_flt ON wi_flt.workModuleId = wm_flt.id
            WHERE {sub_where}
        )""")
    return " AND ".join(conditions)

def format_cases(df):
    return df.astype(str).replace('nan','').replace('NaT','').replace('None','').replace('<NA>','').to_dict(orient='records')

def shorten_product(s):
    return (str(s)
        .replace('Reverse Total Shoulder Arthroplasty','RTSA')
        .replace('Total Ankle Replacement','TAR')
        .replace('Total Knee Arthroplasty','TKA')
        .replace('Total Talus and Other Arthroplasty','Total Talus')
        .replace('Hip Arthroplasty','Hip'))

def build_where_clause(args, alias='f'):
    conditions = [f"{alias}.deleted = false", f"{alias}.Case_In_Take IN ('Case Intake','Restor3d MedEd','Cody Case Intake')"]
    if args.get('product'):
        products = [p.strip() for p in args.get('product').split(',')]
        joined = "','".join(products)
        if len(products) == 1:
            conditions.append(f"{alias}.case_category_name = '{products[0]}'")
        else:
            conditions.append(f"{alias}.case_category_name IN ('{joined}')")
    if args.get('product_group'):
        all_products = []
        for g in [g.strip() for g in args.get('product_group').split(',')]:
            all_products.extend(PRODUCT_GROUPS.get(g, []))
        if all_products:
            joined = "','".join(all_products)
            conditions.append(f"{alias}.case_category_name IN ('{joined}')")
    if args.get('joint'):
        joints = [j.strip() for j in args.get('joint').split(',')]
        joined = "','".join(joints)
        conditions.append(f"{alias}.anatomy_name IN ('{joined}')")
    if args.get('regulatory'):
        regs = [r.strip() for r in args.get('regulatory').split(',')]
        joined = "','".join(CLEARED_PRODUCTS)
        if 'Cleared' in regs and 'Custom' not in regs:
            conditions.append(f"{alias}.case_category_name IN ('{joined}')")
        elif 'Custom' in regs and 'Cleared' not in regs:
            conditions.append(f"{alias}.case_category_name NOT IN ('{joined}')")
    if args.get('surgeon'):
        conditions.append(f"LOWER({alias}.phy_nameLast) LIKE LOWER('%{sanitize(args.get('surgeon'))}%')")
    if args.get('laterality'):
        lats = [l.strip() for l in args.get('laterality').split(',')]
        joined = "','".join(lats)
        conditions.append(f"{alias}.laterality IN ('{joined}')")
    if args.get('facility'):
        conditions.append(f"LOWER({alias}.fac_name) LIKE LOWER('%{sanitize(args.get('facility'))}%')")
    if args.get('state'):
        conditions.append(f"UPPER({alias}.fac_state) = UPPER('{sanitize(args.get('state'))}')")
    for flag, field in {'surgery_scheduled':'isSurgeryScheduled','oncology':'isOncologyCase','trauma':'isTraumaCase','pediatric':'isPediatricCase','preop':'preoperativePlanningOnly','research':'researchCase'}.items():
        if args.get(flag) == 'true':
            conditions.append(f"{alias}.{field} = true")
    # Case type filter
    case_type = args.get('case_type', '').strip()
    if case_type:
        types = [t.strip() for t in case_type.split(',')]
        if len(types) == 1:
            conditions.append(f"{alias}.case_type_name = '{types[0]}'")
        else:
            joined = "','".join(types)
            conditions.append(f"{alias}.case_type_name IN ('{joined}')")
    # Shoulder/ankle case-type selection now goes through the generic case_type filter above —
    # no per-product grouping dicts needed.
    # Step + User filter via WorkModuleSignoff subquery
    step_val = args.get('step', '').strip()
    user_val = args.get('step_user', '').strip()
    if step_val or user_val:
        sub = ["w_flt.deleted = false", "w_flt.workModuleSignatureType = 'ACCEPT'"]
        if step_val:
            steps = [s.strip() for s in step_val.split(',')]
            if len(steps) == 1:
                sub.append(f"wm_flt.name = '{steps[0]}'")
            else:
                joined = "','".join(steps)
                sub.append(f"wm_flt.name IN ('{joined}')")
        if user_val:
            users = [u.strip() for u in user_val.split(',')]
            if len(users) == 1:
                sub.append(f"w_flt.signature = '{users[0]}'")
            else:
                joined = "','".join(users)
                sub.append(f"w_flt.signature IN ('{joined}')")
        sub_where = " AND ".join(sub)
        conditions.append(f"""{alias}.id IN (
            SELECT DISTINCT w_flt.caseId
            FROM `{PROJECT}.{DATASET}.WorkModuleSignoff` w_flt
            LEFT JOIN `{PROJECT}.{DATASET}.WorkModuleInstance` wi_flt ON w_flt.workModuleInstanceId = wi_flt.id
            LEFT JOIN `{PROJECT}.{DATASET}.WorkModule` wm_flt ON wi_flt.workModuleId = wm_flt.id
            WHERE {sub_where}
        )""")
    return " AND ".join(conditions)

def sanitize(value: str) -> str:
    """Strip characters that could be used for SQL injection in LIKE/equality clauses."""
    return re.sub(r"['\";\\]", "", value).strip()

_DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')

def validate_date(value: str) -> str:
    """Return value if it matches YYYY-MM-DD, otherwise return empty string."""
    v = (value or '').strip()
    return v if _DATE_RE.match(v) else ''

def date_range_filter(date_field, date_from, date_to):
    date_from = validate_date(date_from)
    date_to   = validate_date(date_to)
    conds = [f"{date_field} IS NOT NULL"]
    if date_from:
        conds.append(f"CAST({date_field} AS DATETIME) >= CAST('{date_from}' AS DATETIME)")
    if date_to:
        conds.append(f"CAST({date_field} AS DATETIME) <= CAST('{date_to} 23:59:59' AS DATETIME)")
    return " AND ".join(conds)

def outlier_exclusion_sql(avg_digital_lt=None):
    parts = ["(s.ship_wrk_comp_date IS NULL OR TIMESTAMP_DIFF(s.ship_wrk_comp_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days,0) <= 50)"]
    if avg_digital_lt and avg_digital_lt > 0:
        threshold = round(avg_digital_lt * 2, 1)
        parts.append(f"""(sd.peer_review_date IS NULL OR
            TIMESTAMP_DIFF(sd.peer_review_date, s.first_scan_upload_date, DAY) - COALESCE(oh.total_hold_days,0)
            - GREATEST(0, COALESCE(TIMESTAMP_DIFF(sd.surgeon_approval_date, sd.first_psp_review_date, DAY), 0)) <= {threshold})""")
    return " AND ".join(parts)


@app.route("/config", methods=["GET"])
def config():
    try:
        # Get live product counts from BigQuery (all time)
        # Pull products directly from live BigQuery — no static list, no duplicates
        df = client.query(f"""
            SELECT case_category_name, COUNT(*) as case_count
            FROM {tbl('vw_fact_case')}
            WHERE deleted = false AND case_category_name IS NOT NULL
            GROUP BY case_category_name ORDER BY case_count DESC
        """).to_dataframe()
        live_products = df['case_category_name'].tolist()  # already sorted by count

        # Build product groups — only include products that actually exist in BigQuery
        live_set = set(live_products)
        groups = {}
        for g, prods in PRODUCT_GROUPS.items():
            matched = [p for p in prods if p in live_set]
            if matched:
                groups[g] = matched

        return jsonify({
            "products": live_products,
            "product_groups": groups,
            "cleared": [p for p in live_products if p in CLEARED_PRODUCTS],
            "custom": [p for p in live_products if p not in CLEARED_PRODUCTS],
            "live_products": live_products,
        })
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/case-types", methods=["GET"])
def case_types():
    """Return distinct case_type_name values for the case type filter dropdown."""
    try:
        args = request.args
        product = args.get('product', '').strip()
        product_group = args.get('product_group', '').strip()

        conditions = ["f.deleted = false", "f.case_type_name IS NOT NULL"]
        if product:
            prods = [p.strip() for p in product.split(',')]
            joined = "','".join(prods)
            conditions.append(f"f.case_category_name IN ('{joined}')")
        elif product_group:
            all_prods = []
            for g in [g.strip() for g in product_group.split(',')]:
                all_prods.extend(PRODUCT_GROUPS.get(g, []))
            if all_prods:
                joined = "','".join(all_prods)
                conditions.append(f"f.case_category_name IN ('{joined}')")

        where_sql = " AND ".join(conditions)
        query = f"""
            SELECT f.case_type_name, COUNT(*) as case_count
            FROM {tbl('vw_fact_case')} f
            WHERE {where_sql}
            GROUP BY f.case_type_name
            ORDER BY case_count DESC
        """
        rows = client.query(query).to_dataframe().astype(str).to_dict(orient="records")
        return jsonify({"case_types": rows})
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/users-by-step", methods=["GET"])
def users_by_step():
    """Return distinct step names and/or userIds for populating filter dropdowns."""
    try:
        args = request.args
        step = args.get('step', '').strip()
        mode = args.get('mode', '').strip()  # 'users_only' to force user list
        where_parts = ["w.deleted = false", "w.workModuleSignatureType = 'ACCEPT'"]
        # Apply product filter via case join
        product = args.get('product', '').strip()
        product_group = args.get('product_group', '').strip()
        case_join = ""
        if product or product_group:
            case_join = f"JOIN {tbl('vw_fact_case')} f ON w.caseId = f.id"
            if product:
                prods = [p.strip() for p in product.split(',')]
                joined = "','".join(prods)
                where_parts.append(f"f.case_category_name IN (\'{joined}\')")
            if product_group:
                all_prods = []
                for g in [g.strip() for g in product_group.split(',')]:
                    all_prods.extend(PRODUCT_GROUPS.get(g, []))
                if all_prods:
                    joined = "','".join(all_prods)
                    where_parts.append(f"f.case_category_name IN (\'{joined}\')")

        if step:
            steps = [s.strip() for s in step.split(',')]
            if len(steps) == 1:
                where_parts.append(f"wm.name = '{steps[0]}'")
            else:
                joined = "','".join(steps)
                where_parts.append(f"wm.name IN ('{joined}')")

        if step or mode == 'users_only':
            # Return users (optionally filtered by step and/or product)
            where_sql = " AND ".join(where_parts)
            query = f"""
                SELECT w.signature as userId, COUNT(DISTINCT w.caseId) as case_count
                FROM {tbl('WorkModuleSignoff')} w
                LEFT JOIN {tbl('WorkModuleInstance')} wi ON w.workModuleInstanceId = wi.id
                LEFT JOIN {tbl('WorkModule')} wm ON wi.workModuleId = wm.id
                {case_join}
                WHERE {where_sql} AND w.signature IS NOT NULL AND w.signature != ''
                GROUP BY w.signature ORDER BY case_count DESC
            """
            rows = client.query(query).to_dataframe().astype(str).to_dict(orient="records")
            return jsonify({"mode": "users", "step": step, "users": rows})
        else:
            # Return step names
            where_sql = " AND ".join(where_parts)
            query = f"""
                SELECT wm.name as step_name, COUNT(DISTINCT w.caseId) as case_count
                FROM {tbl('WorkModuleSignoff')} w
                LEFT JOIN {tbl('WorkModuleInstance')} wi ON w.workModuleInstanceId = wi.id
                LEFT JOIN {tbl('WorkModule')} wm ON wi.workModuleId = wm.id
                {case_join}
                WHERE {where_sql} AND wm.name IS NOT NULL
                GROUP BY wm.name ORDER BY case_count DESC
            """
            rows = client.query(query).to_dataframe().astype(str).to_dict(orient="records")
            return jsonify({"mode": "steps", "steps": rows})
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/cases", methods=["GET"])
def analytics_cases():
    try:
        args = request.args
        where = build_where_clause(args)
        if args.get('milestone_field') and (args.get('date_from') or args.get('date_to')):
            mf = args.get('milestone_field')
            df_filter = date_range_filter(mf, args.get('date_from',''), args.get('date_to',''))
            query = f"WITH {signoff_cte()}, {on_hold_cte()} SELECT {case_select_fields()} {case_joins()} WHERE {where} AND {df_filter} ORDER BY f.createdAt DESC LIMIT 500"
        else:
            if args.get('date_from'):
                where += f" AND f.createdAt >= '{validate_date(args.get('date_from'))}'"
            if args.get('date_to'):
                where += f" AND f.createdAt <= '{validate_date(args.get('date_to'))} 23:59:59'"
            query = f"WITH {signoff_cte()}, {on_hold_cte()} SELECT {case_select_fields()} {case_joins()} WHERE {where} ORDER BY f.createdAt DESC LIMIT 500"
        df = client.query(query).to_dataframe()
        return jsonify({"cases": format_cases(df), "count": len(df)})
    except Exception as e:
        return handle_error(request.endpoint, e)


@app.route("/analytics/query", methods=["GET"])
def analytics_query():
    try:
        args = request.args
        metric = args.get('metric', 'count')
        agg = args.get('agg', 'avg')
        group_by = args.get('group_by')
        where = build_where_clause(args)
        date_from = args.get('date_from', '')
        date_to = args.get('date_to', '')
        use_created_at = args.get('use_created_at') == 'true'
        milestone_map = {'total_lt':'s.ship_wrk_comp_date','seg_lt':'sd.seg_review_date','surgeon_lt':'sd.surgeon_approval_date','digital_lt':'sd.first_psp_review_date','count':'sd.first_psp_review_date'}
        date_filter_sql = ""
        if use_created_at:
            if date_from: date_filter_sql += f" AND f.createdAt >= '{validate_date(date_from)}'"
            if date_to: date_filter_sql += f" AND f.createdAt <= '{validate_date(date_to)} 23:59:59'"
        else:
            date_field = milestone_map.get(metric)
            if date_field and (date_from or date_to):
                date_filter_sql = f"AND {date_range_filter(date_field, date_from, date_to)}"
        agg_map = {'avg':'ROUND(AVG({}), 1)','sum':'SUM({})','min':'MIN({})','max':'MAX({})','count':'COUNT({})'}
        if metric == 'onhold':
            agg_expr = "COUNTIF(f.onHold = true)"; count_expr = "COUNTIF(f.onHold = true)"
        elif metric == 'count':
            agg_expr = "COUNT(DISTINCT f.id)"; count_expr = "COUNT(DISTINCT f.id)"
        else:
            agg_expr = agg_map.get(agg,'ROUND(AVG({}), 1)').format(METRIC_MAP.get(metric,('1','',''))[0])
            count_expr = "COUNT(DISTINCT f.id)"
        group_map = {'product':'f.case_category_name','month':"FORMAT_DATETIME('%b %Y', CAST(f.createdAt AS DATETIME))",'surgeon':"CONCAT(f.phy_nameFirst, ' ', f.phy_nameLast)",'phase':'f.phase','state':'f.fac_state','facility':'f.fac_name'}
        ctes = f"WITH {signoff_cte()}, {on_hold_cte()}"
        base = f"{case_joins()} WHERE {where} {date_filter_sql}"
        if group_by and group_by in group_map:
            group_field = group_map[group_by]
            query = f"{ctes} SELECT {group_field} as label, {agg_expr} as value, {count_expr} as case_count {base} GROUP BY {group_field} ORDER BY case_count DESC LIMIT 50"
            df = client.query(query).to_dataframe().astype(str).replace('nan','').replace('None','')
            return jsonify({"rows": df[['label','value']].to_dict(orient='records'), "group_by": group_by, "metric": metric, "agg": agg})
        else:
            query = f"{ctes} SELECT {agg_expr} as result {base}"
            result = client.query(query).to_dataframe().iloc[0]['result']
            try: result = round(float(result), 1)
            except: result = str(result)
            return jsonify({"value": result, "metric": metric})
    except Exception as e:
        return handle_error(request.endpoint, e)

@app.route("/analytics/query/compare", methods=["GET"])
def analytics_query_compare():
    try:
        args = request.args
        metrics = [m.strip() for m in args.get('metrics', 'seg_lt,surgeon_lt').split(',') if m.strip() in METRIC_MAP]
        group_by = args.get('group_by', 'product')
        where = build_where_clause(args)
        date_from = args.get('date_from', '')
        date_to = args.get('date_to', '')
        if not metrics:
            return jsonify({"error": "No valid metrics"}), 400
        group_map = {'product':'f.case_category_name','month':"FORMAT_DATETIME('%b %Y', CAST(f.createdAt AS DATETIME))",'surgeon':"CONCAT(f.phy_nameFirst, ' ', f.phy_nameLast)",'phase':'f.phase','state':'f.fac_state'}
        group_field = group_map.get(group_by, 'f.case_category_name')
        primary_date_field = METRIC_MAP[metrics[0]][2]
        df_filter = date_range_filter(primary_date_field, date_from, date_to)
        metric_selects = ',\n'.join([f"ROUND(AVG({METRIC_MAP[m][0]}), 1) as {m}" for m in metrics])
        query = f"""
        WITH {signoff_cte()}, {on_hold_cte()}
        SELECT {group_field} as label, COUNT(DISTINCT f.id) as case_count, {metric_selects}
        {case_joins()}
        WHERE {where} AND {df_filter}
        GROUP BY {group_field} ORDER BY case_count DESC LIMIT 50
        """
        df = client.query(query).to_dataframe().fillna(0)
        labels = [shorten_product(l) for l in df['label'].astype(str).tolist()]
        colors = ['#0284c7','#16a34a','#7c3aed','#d97706','#0891b2','#dc2626']
        datasets = [{'label': METRIC_MAP[m][1], 'metric': m, 'data': [round(float(v), 1) for v in df[m].tolist()], 'color': colors[i % len(colors)]} for i, m in enumerate(metrics)]
        return jsonify({'labels': labels, 'datasets': datasets, 'group_by': group_by, 'metrics': metrics})
    except Exception as e:
        return handle_error(request.endpoint, e)

NLQ_METRICS = {
    'total_lt':   'Total lead time (days) — scan upload to shipping work complete',
    'seg_lt':     'Segmentation lead time (days) — scan upload to segmentation review',
    'surgeon_lt': 'Surgeon approval lead time (days) — first PSP review to surgeon approval',
    'digital_lt': 'Digital production lead time (days)',
    'count':      'Number of distinct cases (case count)',
    'created':    'Number of cases created (by createdAt date, not milestone date)',
    'onhold':     'Number of cases currently on hold',
    'seg_completed':     'Number of cases that have COMPLETED (passed) the Segmentation milestone — i.e. have a reviewer-accepted segmentation review date. Use this, not the generic step filter, for "cases that completed/passed segmentation".',
    'psp_completed':     'Number of cases that have COMPLETED (passed) the Proposed Surgical Plan / PSP milestone — i.e. have a reviewer-accepted PSP review date. Use this, not the generic step filter, for "cases that completed/passed the PSP / proposed surgical plan step".',
    'surgeon_completed': 'Number of cases that have COMPLETED (passed) surgeon approval on the PSP.',
    'peer_review_completed': 'Number of cases that have COMPLETED (passed) peer review.',
}

NLQ_AGGS = ['avg', 'sum', 'min', 'max', 'count']

NLQ_GROUP_BY = {
    'product':   'Product / case category (e.g. Reverse Total Shoulder Arthroplasty)',
    'case_type': 'Case type within a product (e.g. Custom rTSA - Custom Glenoid + Standard Stem, Glenosphere Only, etc.)',
    'month':     'Calendar month',
    'surgeon':   'Surgeon name',
    'phase':     'Case phase',
    'state':     'Facility state',
    'facility':  'Facility name',
}

NLQ_PRODUCTS = sorted({p for grp in PRODUCT_GROUPS.values() for p in grp})

NLQ_PRODUCT_GROUPS = sorted(PRODUCT_GROUPS.keys())

NLQ_LATERALITY = ['Left', 'Right', 'Bilateral']

NLQ_REGULATORY = ['Cleared', 'Custom']

def _nlq_system_prompt():
    """Builds the grounding prompt from the SAME constants the rest of the app
    uses for querying — this is the single source of truth Claude is allowed
    to draw from. Nothing here is invented; it is pulled directly from
    METRIC_MAP / PRODUCT_GROUPS / group_map so the two can never drift apart."""
    metrics_list = "\n".join(f"  - \"{k}\": {v}" for k, v in NLQ_METRICS.items())
    groups_list = "\n".join(f"  - \"{k}\": {v}" for k, v in NLQ_GROUP_BY.items())
    products_list = "\n".join(f"  - {p}" for p in NLQ_PRODUCTS)
    product_groups_list = "\n".join(f"  - {g}" for g in NLQ_PRODUCT_GROUPS)
    derived_metrics_list = "\n".join(f"  - \"{k}\": {v}" for k, v in NLQ_DERIVED_METRICS.items())
    wip_stages_list = "\n".join(f"  - \"{k}\": {v}" for k, v in NLQ_WIP_STAGES.items())
    return f"""You are the query-routing engine for DataGenie, an internal analytics tool for Restor3D's surgical case pipeline.

You have FOUR tools available:
1. `run_case_query` — for counts, averages, sums, and rankings of RAW case data (things directly in the case table — lead times, case counts, etc.).
2. `list_cases` — for returning individual case rows (case number, product, phase, surgeon, facility, on-hold status/days, dates, lead times) when the user wants to see specific cases rather than a number.
3. `derived_metric_query` — for CALCULATED metrics that use a custom formula elsewhere in the app: utilization, process efficiency, First Pass Yield (FPY), queue time, or On-Time Delivery (OTD). ALWAYS use this tool (never run_case_query) for any question mentioning utilization, efficiency, FPY/first pass yield, queue/wait time, or on-time delivery/OTD — these are NOT simple aggregates and run_case_query has no way to compute them correctly.
4. `list_wip_cases` — for listing cases currently sitting in one or more named WIP (work-in-progress) pipeline stages: segmentation, psp_design, surgeon_approval, peer_review, manufacturing. ALWAYS use this tool (never list_cases or run_case_query) for any question mentioning "WIP", "work in progress", or cases "currently in" one of these stages — these use real multi-condition bucket logic that a simple phase filter cannot reproduce. A range like "from segmentation through peer review" means stages=segmentation,psp_design,surgeon_approval,peer_review (all stages between and including the two named).

CRITICAL — "COMPLETED" / "PASSED" A STEP is NOT the same as the generic `step` filter:
run_case_query's `step` field (and list_cases'/list_wip_cases' `step` field) matches ANY accepted signoff on that step name — worker, reviewer, or approver — which is NOT the same as "how many cases have completed/passed [step]". For a completion/pass-through count specifically, use the dedicated metric instead: metric="seg_completed" (completed Segmentation), "psp_completed" (completed the Proposed Surgical Plan / PSP step), "surgeon_completed" (surgeon approved the PSP), or "peer_review_completed" (completed Peer Review) — with run_case_query, agg="count", NO step field set. Using the generic step filter for a "completed/passed" question will silently produce a different, WRONG number that won't match the app's real dashboards — never do this.

You may call these tools — in any combination, possibly MULTIPLE TIMES — using ONLY the approved vocabulary below. You NEVER invent a metric, product, grouping, or field that is not explicitly listed. If part of a question truly cannot be represented with this vocabulary even after breaking it into steps, say so plainly in your final answer — do not guess at new fields or silently substitute something else.

MULTI-STEP QUESTIONS:
Many questions require more than one query. For example, "average digital lead time for the top 3 case types by volume" requires:
  Step 1: run_case_query(metric="count", group_by="product", ...) to rank case types by volume and see which 3 are largest.
  Step 2: run_case_query(metric="digital_lt", agg="avg", group_by="product", product="<top 3 exact product names from step 1>", ...) to get the lead time for just those 3.
Another example, "list the cases on hold for RTSA" requires:
  Step 1: list_cases(on_hold_only=true, product="Reverse Total Shoulder Arthroplasty", ...)
Another example, "top 10 designers in segmentation by utilization" requires:
  Step 1: derived_metric_query(metric="utilization", step="Segmentation", top_n=10, sort_desc=true, ...)
Another example, "WIP cases from segmentation through peer review for the top 2 case types by volume in Total Ankle Replacement" requires:
  Step 1: run_case_query(metric="count", group_by="product", product_group="Total Ankle Replacement" or the matching product names, ...) to find the top 2 case types by volume within that product line.
  Step 2: list_wip_cases(stages="segmentation,psp_design,surgeon_approval,peer_review", product="<top 2 exact product names from step 1>", ...)
You can call tools up to {{max_steps}} times total. Use the result of each call to decide the next call's filters (e.g. which exact product names to pass into the `product` field). Once you have everything needed, respond with plain text (no more tool calls) — a SHORT interpretive summary in 1-3 sentences, using ONLY numbers/cases you actually got back from tool results. Never estimate, round, or infer from general knowledge.

CRITICAL — YOUR TEXT ANSWER IS NOT THE TABLE:
- When `list_cases`, `derived_metric_query`, or `list_wip_cases` returns rows, the UI already renders those as a table on its own. Your text response must be a brief 1-3 sentence summary only (e.g. counts, notable patterns) — NEVER reproduce the case list as a markdown table, NEVER list cases/users one-by-one in your text.
- NEVER mention a specific reason, note, or explanation for why a case is on hold (or any other case-level detail) unless that exact text appears in the `oh_note`, `last_case_note`, `last_internal_case_note`, or `last_design_case_note` fields of a tool result. If those fields are empty for the cases in question, say the notes aren't available — do not invent a plausible-sounding reason.
- If asked for a field that does not appear anywhere in the tool result JSON (e.g. something not in case_select_fields), say plainly that field isn't available in this data rather than guessing.

CRITICAL — DO NOT STATE CASE COUNTS IN YOUR TEXT ANSWER:
- Whenever the UI will render a table (from list_cases, list_wip_cases, derived_metric_query) or a chart (from run_case_query with a group_by) or a single-value display (run_case_query without group_by), that count/number is ALREADY visible to the person right below your text — the table's row count, the chart's bars, or the big number display.
- Your text must NOT state the count of cases in numerical form. Write around it — say "the cases shown below" or "these cases" or "the breakdown below shows the distribution", NEVER "there are N cases" / "N cases on hold" / "N WIP cases" / "N cases across…" / any pattern that includes a number followed by (something) "case(s)". This is because if you state a number that doesn't exactly match the table, it destroys trust — and the number is already visible, so restating it adds no value.
- DO include qualitative context in your text (which stages, filters applied, notable patterns, what the columns mean) — just never the raw count number itself.
- If the question is specifically about "cases on hold", still set `on_hold_only=true` on the list_cases call so the table is correctly scoped — just describe the result without a numeric count.
- Only exception: when a single-value result (run_case_query, no group_by) returns a lead-time in days (like "avg total lead time = 47 days"), THAT value is the actual answer and should appear in your text — because there's no big-number UI showing it in a way that stands alone (the chart wrapper is hidden for single values). But for anything counted as cases, never state the count.

AVAILABLE METRICS (metric, for run_case_query):
{metrics_list}

AVAILABLE AGGREGATIONS (agg, for run_case_query): {', '.join(NLQ_AGGS)}
(For metric="count", "created", or "onhold", agg is ignored — always a count.)

AVAILABLE GROUP_BY (optional, for run_case_query — omit for a single overall number):
{groups_list}

KNOWN PRODUCTS (case_category_name — use exact spelling if the user names one, or if referencing rows returned by a previous step):
{products_list}

KNOWN PRODUCT GROUPS (broader buckets):
{product_groups_list}

LATERALITY VALUES: {', '.join(NLQ_LATERALITY)}
REGULATORY VALUES: {', '.join(NLQ_REGULATORY)} (Cleared = {', '.join(CLEARED_PRODUCTS)}; Custom = everything else)

DERIVED METRICS (metric, for derived_metric_query — R3ID/BigQuery only):
{derived_metrics_list}
- granularity (daily/weekly/monthly) applies to utilization, process_efficiency, fpy only — omit for queue_time and otd.
- step and user filters apply to utilization and fpy only.
- surgeon filter applies to otd only.
- Each derived_metric_query result is a list of rows (e.g. one per user, or one per step) with a value field appropriate to that metric (utilization %, efficiency %, fpy %, avg_queue_hours, or otd_pct). Use top_n + sort_desc to answer "top N" / "bottom N" questions directly — do not manually sort text yourself.

WIP PIPELINE STAGES (stages, for list_wip_cases — comma-separated, in pipeline order):
{wip_stages_list}
- These are ORDERED: segmentation -> psp_design -> surgeon_approval -> peer_review -> manufacturing. A "range" phrase like "from X through Y" means all stages from X to Y inclusive in this order.
- Each returned case is tagged with which stage it's currently in (wip_stage field).

DATE HANDLING:
- If the user mentions a relative period ("last 30 days", "last quarter", "this year", "last 8 weeks"), compute date_from/date_to as ISO dates (YYYY-MM-DD) using {{today}} as today's date. Reuse the SAME date range across all steps of a multi-step question unless the question implies otherwise.
- If the user gives no date period, leave date_from/date_to empty (defaults to last 6 months on milestone date).
- Use `use_created_at=true` only if the user is explicitly asking about when cases were CREATED/opened, not about lead time or completion milestones.

RULES:
- Only use metric/group_by/product/product_group/laterality/regulatory values from the lists above — exact strings.
- When narrowing to specific items found in a prior step's results (e.g. "top 3 by volume"), pass their exact names — as returned in that step's `label` field — into the `product` field of the next call, comma-separated.
- FUZZY NAME MATCHING: the person may misspell, abbreviate, or loosely name a product, surgeon, facility, case type, step, or user. Match confidently against the known lists above (e.g. "RTSA" -> Reverse Total Shoulder Arthroplasty, "Toatl Ankle" -> Total Ankle Replacement, a surgeon's name with a typo -> the closest real surname) and proceed with the query using that match — do not stop to ask about typos or minor misspellings of things clearly on this list.
- If a name is CLOSE to something known but genuinely ambiguous (matches two or more candidates similarly well, e.g. it could plausibly be either of two different products), or matches NOTHING close enough to be confident, do NOT guess and do NOT silently drop it. Instead, skip calling the tool for that step, and ask for clarification using this EXACT structured format (no other prose mixed in, one option per line, this exact prefix):
CLARIFY: <one short sentence stating what's ambiguous>
OPTION: <candidate 1 exact name>
OPTION: <candidate 2 exact name>
OPTION: <candidate 3 exact name, if applicable>
  Do not add any other text before, between, or after these lines when asking for clarification this way.
- Never silently substitute a different product/surgeon/step/case type than what the person named without saying so in your answer.
- Never fabricate a metric or field outside this vocabulary.
- Keep tool calls minimal — don't call the tool more times than the question requires.
"""

NLQ_TOOL = {
    "name": "run_case_query",
    "description": "Execute ONE case analytics query step using only the approved metric/grouping/filter vocabulary. Call this multiple times in sequence to answer compound questions — use results from earlier calls to decide filters for later ones. Use this for counts, averages, sums, and rankings — NOT for listing individual cases.",
    "input_schema": {
        "type": "object",
        "properties": {
            "metric": {"type": "string", "enum": list(NLQ_METRICS.keys())},
            "agg": {"type": "string", "enum": NLQ_AGGS},
            "group_by": {"type": "string", "enum": list(NLQ_GROUP_BY.keys())},
            "product": {"type": "string", "description": "Comma-separated exact product names from the KNOWN PRODUCTS list, or empty."},
            "product_group": {"type": "string", "description": "Comma-separated exact product group names from KNOWN PRODUCT GROUPS list, or empty."},
            "laterality": {"type": "string", "enum": NLQ_LATERALITY + [""]},
            "regulatory": {"type": "string", "enum": NLQ_REGULATORY + [""]},
            "surgeon": {"type": "string", "description": "Surgeon last-name substring filter, or empty."},
            "facility": {"type": "string", "description": "Facility name substring filter, or empty."},
            "state": {"type": "string", "description": "US state abbreviation filter, or empty."},
            "case_type": {"type": "string", "description": "Comma-separated exact case type names (e.g. 'Standard', 'Revision'), or empty."},
            "step": {"type": "string", "description": "Comma-separated exact work-module step names, or empty."},
            "step_user": {"type": "string", "description": "Comma-separated exact userId/signature values, or empty."},
            "date_from": {"type": "string", "description": "ISO date YYYY-MM-DD or empty."},
            "date_to": {"type": "string", "description": "ISO date YYYY-MM-DD or empty."},
            "use_created_at": {"type": "boolean"},
            "step_label": {"type": "string", "description": "Short label describing what this specific step is checking, e.g. 'Rank case types by volume'."},
        },
        "required": ["metric", "agg", "step_label"],
    },
}

NLQ_LIST_CASES_TOOL = {
    "name": "list_cases",
    "description": "Return up to 500 INDIVIDUAL case rows matching the given filters, including: case number, product, case type, phase, surgeon, facility, laterality, on-hold status, days on hold, the exact date/time put on hold (put_on_hold_at), the on-hold comment (oh_note), and the most recent case/internal/design notes with author and date. Use this when the user asks to 'list', 'show me the cases', 'which cases', or wants case-level detail (including hold reasons/notes/dates) rather than a count/average. Do NOT use for aggregates — use run_case_query for those.",
    "input_schema": {
        "type": "object",
        "properties": {
            "on_hold_only": {"type": "boolean", "description": "If true, only return cases currently on hold (f.onHold = true)."},
            "product": {"type": "string", "description": "Comma-separated exact product names from the KNOWN PRODUCTS list, or empty."},
            "product_group": {"type": "string", "description": "Comma-separated exact product group names from KNOWN PRODUCT GROUPS list, or empty."},
            "laterality": {"type": "string", "enum": NLQ_LATERALITY + [""]},
            "regulatory": {"type": "string", "enum": NLQ_REGULATORY + [""]},
            "surgeon": {"type": "string", "description": "Surgeon last-name substring filter, or empty."},
            "facility": {"type": "string", "description": "Facility name substring filter, or empty."},
            "state": {"type": "string", "description": "US state abbreviation filter, or empty."},
            "case_type": {"type": "string", "description": "Comma-separated exact case type names, or empty."},
            "step": {"type": "string", "description": "Comma-separated exact work-module step names, or empty."},
            "step_user": {"type": "string", "description": "Comma-separated exact userId/signature values, or empty."},
            "date_from": {"type": "string", "description": "ISO date YYYY-MM-DD or empty."},
            "date_to": {"type": "string", "description": "ISO date YYYY-MM-DD or empty."},
            "use_created_at": {"type": "boolean"},
            "step_label": {"type": "string", "description": "Short label describing what this list represents, e.g. 'Cases currently on hold'."},
        },
        "required": ["step_label"],
    },
}

NLQ_DERIVED_METRICS = {
    'utilization':         'Designer/worker utilization % = earned minutes (standard time credit for signoffs) / (active_days * 480 min capacity). From the same calculation as the Utilization dashboard.',
    'process_efficiency':  'Process efficiency % = standard_time / actual_time, per worker per step. From the same calculation as the Process Efficiency chart.',
    'fpy':                 "First Pass Yield % = percent of a worker's cases where the reviewer's FIRST signoff was ACCEPT (no rework needed). From the same calculation as the FPY chart.",
    'queue_time':          'Queue time = idle hours a case waits before a worker picks it up, per step (team-level, not per-user). From the same calculation as the Queue Time report.',
    'otd':                 'On-Time Delivery % = percent of cases where Digital Production lead time was within the target (14 days). From the same calculation as the OTD report.',
}

NLQ_DERIVED_GRANULARITY = ['daily', 'weekly', 'monthly']

NLQ_DERIVED_METRIC_TOOL = {
    "name": "derived_metric_query",
    "description": (
        "Get a DERIVED / CALCULATED metric that is NOT a simple BigQuery count or average — utilization, process efficiency, "
        "First Pass Yield (FPY), queue time, or On-Time Delivery (OTD). These use custom formulas already computed elsewhere "
        "in the app (the same numbers shown on the Utilization / Process Efficiency / FPY / Queue Time / OTD dashboard pages) "
        "— NOT something to approximate with run_case_query. Use this whenever the question mentions utilization, efficiency, "
        "FPY, first pass yield, queue time, wait time, or on-time delivery. R3ID/BigQuery data only (not Camstar)."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "metric": {"type": "string", "enum": list(NLQ_DERIVED_METRICS.keys())},
            "granularity": {"type": "string", "enum": NLQ_DERIVED_GRANULARITY, "description": "Not used for queue_time or otd."},
            "product": {"type": "string", "description": "Comma-separated exact product names from the KNOWN PRODUCTS list, or empty."},
            "product_group": {"type": "string", "description": "Comma-separated exact product group names from KNOWN PRODUCT GROUPS list, or empty."},
            "step": {"type": "string", "description": "Comma-separated exact work-module step names (e.g. 'Segmentation', 'Design'), or empty. Not used for otd."},
            "user": {"type": "string", "description": "Comma-separated exact userId/signature values to filter to specific designers, or empty. Not used for otd."},
            "surgeon": {"type": "string", "description": "Surgeon substring filter — only used for otd."},
            "date_from": {"type": "string", "description": "ISO date YYYY-MM-DD or empty."},
            "date_to": {"type": "string", "description": "ISO date YYYY-MM-DD or empty."},
            "top_n": {"type": "integer", "description": "If the question asks for a top/bottom N ranking of users, set this (e.g. 10). Otherwise omit."},
            "sort_desc": {"type": "boolean", "description": "True for 'top' (highest first), false for 'bottom'/'lowest'. Default true."},
            "step_label": {"type": "string", "description": "Short label describing what this step is checking, e.g. 'Top 10 designers by utilization'."},
        },
        "required": ["metric", "step_label"],
    },
}

NLQ_WIP_STAGES = {
    'segmentation':      'Scans uploaded but segmentation review not yet done; not on hold/cancelled; not shipped/in surgery.',
    'psp_design':        'Segmentation reviewed but not yet past PSP (no PSP/peer review/ship complete); not awaiting surgeon; includes cases where the surgeon rejected the PSP and sent it back for rework.',
    'surgeon_approval':  'Past design, awaiting surgeon sign-off on the proposed surgical plan (and not currently a surgeon-rejected rework case).',
    'peer_review':       'Past PSP/surgeon approval, awaiting peer review; excludes marketing cases and manufacturing-phase cases.',
    'manufacturing':     'In the MANUFACTURING phase, not yet shipped; excludes marketing cases.',
}

NLQ_WIP_TOOL = {
    "name": "list_wip_cases",
    "description": (
        "List INDIVIDUAL work-in-progress (WIP) case rows for one or more named pipeline stages: "
        "segmentation, psp_design, surgeon_approval, peer_review, manufacturing. Each stage uses the app's real "
        "WIP bucket definitions (the same logic behind the WIP dashboard) — NOT a simple phase filter, and NOT "
        "something list_cases or run_case_query can approximate. Use this whenever the question mentions WIP, "
        "work in progress, or asks for cases 'currently in' one of these named stages (including a RANGE of "
        "stages, e.g. 'from segmentation through peer review' means stages=segmentation,psp_design,surgeon_approval,peer_review)."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "stages": {"type": "string", "description": "Comma-separated exact stage names from: segmentation, psp_design, surgeon_approval, peer_review, manufacturing."},
            "product": {"type": "string", "description": "Comma-separated exact product names from the KNOWN PRODUCTS list, or empty."},
            "product_group": {"type": "string", "description": "Comma-separated exact product group names from KNOWN PRODUCT GROUPS list, or empty."},
            "laterality": {"type": "string", "enum": NLQ_LATERALITY + [""]},
            "regulatory": {"type": "string", "enum": NLQ_REGULATORY + [""]},
            "surgeon": {"type": "string", "description": "Surgeon last-name substring filter, or empty."},
            "facility": {"type": "string", "description": "Facility name substring filter, or empty."},
            "state": {"type": "string", "description": "US state abbreviation filter, or empty."},
            "case_type": {"type": "string", "description": "Comma-separated exact case type names, or empty."},
            "step": {"type": "string", "description": "Comma-separated exact work-module step names, or empty."},
            "step_user": {"type": "string", "description": "Comma-separated exact userId/signature values, or empty."},
            "step_label": {"type": "string", "description": "Short label describing what this list represents, e.g. 'WIP cases: segmentation through peer review, TAR'."},
        },
        "required": ["stages", "step_label"],
    },
}

def _nlq_validate(payload: dict) -> dict:
    """Re-validates Claude's structured output against the real allowlists.
    Anything outside the allowlist is dropped (not passed through), so a
    hallucinated field can never reach build_where_clause / BigQuery."""
    clean = {}

    metric = payload.get('metric')
    clean['metric'] = metric if metric in NLQ_METRICS else 'count'

    agg = payload.get('agg')
    clean['agg'] = agg if agg in NLQ_AGGS else 'avg'

    group_by = payload.get('group_by')
    if group_by in NLQ_GROUP_BY:
        clean['group_by'] = group_by

    def _filter_csv(value, allowed):
        if not value:
            return ''
        items = [v.strip() for v in str(value).split(',') if v.strip()]
        kept = [v for v in items if v in allowed]
        return ','.join(kept)

    clean['product'] = _filter_csv(payload.get('product', ''), set(NLQ_PRODUCTS))
    clean['product_group'] = _filter_csv(payload.get('product_group', ''), set(NLQ_PRODUCT_GROUPS))

    laterality = payload.get('laterality', '')
    if laterality in NLQ_LATERALITY:
        clean['laterality'] = laterality

    regulatory = payload.get('regulatory', '')
    if regulatory in NLQ_REGULATORY:
        clean['regulatory'] = regulatory

    # Free-text substring filters — sanitized the same way build_where_clause does downstream.
    clean['surgeon'] = sanitize(str(payload.get('surgeon', '') or ''))[:80]
    clean['facility'] = sanitize(str(payload.get('facility', '') or ''))[:80]
    clean['state'] = sanitize(str(payload.get('state', '') or ''))[:10]
    clean['case_type'] = sanitize(str(payload.get('case_type', '') or ''))[:200]
    clean['step'] = sanitize(str(payload.get('step', '') or ''))[:200]
    clean['step_user'] = sanitize(str(payload.get('step_user', '') or ''))[:200]

    clean['date_from'] = validate_date(payload.get('date_from', '') or '')
    clean['date_to'] = validate_date(payload.get('date_to', '') or '')
    clean['use_created_at'] = bool(payload.get('use_created_at', False))

    clean['step_label'] = str(payload.get('step_label') or clean['metric'])[:120]
    return clean

def _nlq_execute(parsed: dict) -> dict:
    """Runs ONE validated query step through the SAME functions /analytics/query
    uses (build_where_clause / case_joins / signoff_cte / on_hold_cte /
    METRIC_MAP), and returns a JSON-safe result dict. This is the only place
    BigQuery is touched from the NLQ flow — every call into it has already
    passed through _nlq_validate, so nothing outside the allowlist can reach
    here regardless of how many steps Claude chains."""
    args = {
        'product': parsed['product'],
        'product_group': parsed['product_group'],
        'laterality': parsed.get('laterality', ''),
        'regulatory': parsed.get('regulatory', ''),
        'surgeon': parsed['surgeon'],
        'facility': parsed['facility'],
        'state': parsed['state'],
        'case_type': parsed.get('case_type', ''),
        'step': parsed.get('step', ''),
        'step_user': parsed.get('step_user', ''),
    }
    class _ArgShim(dict):
        def get(self, k, default=''):
            return dict.get(self, k, default) or default
    shim = _ArgShim(args)

    where = build_where_clause(shim)
    metric = parsed['metric']
    agg = parsed['agg']
    group_by = parsed.get('group_by')
    date_from = parsed['date_from']
    date_to = parsed['date_to']
    use_created_at = parsed['use_created_at']

    milestone_map = {'total_lt':'s.ship_wrk_comp_date','seg_lt':'sd.seg_review_date','surgeon_lt':'sd.surgeon_approval_date','digital_lt':'sd.first_psp_review_date','count':'sd.first_psp_review_date',
                      'seg_completed':'sd.seg_review_date','psp_completed':'sd.first_psp_review_date','surgeon_completed':'sd.surgeon_approval_date','peer_review_completed':'sd.peer_review_date'}
    date_filter_sql = ""
    if use_created_at:
        if date_from: date_filter_sql += f" AND f.createdAt >= '{validate_date(date_from)}'"
        if date_to: date_filter_sql += f" AND f.createdAt <= '{validate_date(date_to)} 23:59:59'"
    else:
        date_field = milestone_map.get(metric)
        if date_field and (date_from or date_to):
            date_filter_sql = f"AND {date_range_filter(date_field, date_from, date_to)}"

    # ── Milestone-completion metrics: same reviewer-accepted signoff_dates fields the real dashboards trust — ──
    # NOT the generic step/step_user filter in build_where_clause, which counts ANY accept (worker/reviewer/
    # approver) on a step name and does not match "cases that completed/passed" a specific milestone.
    completion_field_map = {'seg_completed':'sd.seg_review_date','psp_completed':'sd.first_psp_review_date','surgeon_completed':'sd.surgeon_approval_date','peer_review_completed':'sd.peer_review_date'}
    if metric in completion_field_map:
        date_filter_sql += f" AND {completion_field_map[metric]} IS NOT NULL"

    agg_map = {'avg':'ROUND(AVG({}), 1)','sum':'SUM({})','min':'MIN({})','max':'MAX({})','count':'COUNT({})'}
    if metric == 'onhold':
        agg_expr = "COUNTIF(f.onHold = true)"; count_expr = "COUNTIF(f.onHold = true)"
    elif metric in ('count', 'created') or metric in completion_field_map:
        agg_expr = "COUNT(DISTINCT f.id)"; count_expr = "COUNT(DISTINCT f.id)"
    else:
        agg_expr = agg_map.get(agg, 'ROUND(AVG({}), 1)').format(METRIC_MAP.get(metric, ('1','',''))[0])
        count_expr = "COUNT(DISTINCT f.id)"

    # ── Completion metrics AND the 'created' metric need the volume-style query path — the trusted ──
    # Volume tile bypasses vw_fact_case (which drops COMPLETED cases) and the Case_In_Take restriction
    # baked into build_where_clause/where, and instead uses raw Case + CaseCategory. 'created' is included
    # here too: it should count ALL cases created in the window, not just the 3 Case_In_Take types that
    # build_where_clause silently restricts to — that restriction made 'cases created' undercount relative
    # to 'psp_completed' and other volume-style metrics for the exact same case_type/date filters.
    is_completion_metric = metric in completion_field_map
    is_created_metric = metric == 'created'
    if is_completion_metric or is_created_metric:
        vol_where = volume_where_clause(shim)
        # Rebuild date filter with the correct field alias — same date range logic, unchanged.
        vol_date_filter = ""
        if is_created_metric or use_created_at:
            # 'created' always means case creation date, regardless of the use_created_at flag.
            if date_from: vol_date_filter += f" AND f.createdAt >= '{validate_date(date_from)}'"
            if date_to: vol_date_filter += f" AND f.createdAt <= '{validate_date(date_to)} 23:59:59'"
        else:
            vol_date_field = completion_field_map[metric]
            if date_from or date_to:
                vol_date_filter = f"AND {date_range_filter(vol_date_field, date_from, date_to)}"
        if is_completion_metric:
            vol_date_filter += f" AND {completion_field_map[metric]} IS NOT NULL"
        # Volume uses cc.name for product grouping and ct.name for case type — not f.case_category_name.
        group_map = {'product':'cc.name','case_type':'ct.name','month':"FORMAT_DATETIME('%b %Y', CAST(f.createdAt AS DATETIME))",'surgeon':"CONCAT(f.phy_nameFirst, ' ', f.phy_nameLast)",'phase':'f.phase','state':'f.fac_state','facility':'f.fac_name'}
        ctes = f"WITH {signoff_cte()}, {on_hold_cte()}"
        base = f"{volume_case_joins()} WHERE {vol_where} {vol_date_filter}"
    else:
        group_map = {'product':'f.case_category_name','case_type':'f.case_type_name','month':"FORMAT_DATETIME('%b %Y', CAST(f.createdAt AS DATETIME))",'surgeon':"CONCAT(f.phy_nameFirst, ' ', f.phy_nameLast)",'phase':'f.phase','state':'f.fac_state','facility':'f.fac_name'}
        ctes = f"WITH {signoff_cte()}, {on_hold_cte()}"
        base = f"{case_joins()} WHERE {where} {date_filter_sql}"

    if group_by and group_by in group_map:
        group_field = group_map[group_by]
        query = f"{ctes} SELECT {group_field} as label, {agg_expr} as value, {count_expr} as case_count {base} GROUP BY {group_field} ORDER BY case_count DESC LIMIT 50"
        df = client.query(query).to_dataframe().astype(str).replace('nan','').replace('None','')
        rows = df[['label','value']].to_dict(orient='records')
        return {
            "step_label": parsed['step_label'],
            "metric": metric,
            "agg": agg,
            "group_by": group_by,
            "rows": rows,
            "sql": query,
            "interpreted_as": {k: v for k, v in parsed.items() if v not in ('', None, False)},
        }
    else:
        query = f"{ctes} SELECT {agg_expr} as result {base}"
        result = client.query(query).to_dataframe().iloc[0]['result']
        try:
            result = round(float(result), 1)
        except Exception:
            result = str(result)
        return {
            "step_label": parsed['step_label'],
            "metric": metric,
            "agg": agg,
            "value": result,
            "sql": query,
            "interpreted_as": {k: v for k, v in parsed.items() if v not in ('', None, False)},
        }

def _nlq_validate_list_cases(payload: dict) -> dict:
    """Same allowlist-based re-validation as _nlq_validate, scoped to the
    filter fields relevant for listing individual cases."""
    clean = {}

    def _filter_csv(value, allowed):
        if not value:
            return ''
        items = [v.strip() for v in str(value).split(',') if v.strip()]
        kept = [v for v in items if v in allowed]
        return ','.join(kept)

    clean['product'] = _filter_csv(payload.get('product', ''), set(NLQ_PRODUCTS))
    clean['product_group'] = _filter_csv(payload.get('product_group', ''), set(NLQ_PRODUCT_GROUPS))

    laterality = payload.get('laterality', '')
    if laterality in NLQ_LATERALITY:
        clean['laterality'] = laterality

    regulatory = payload.get('regulatory', '')
    if regulatory in NLQ_REGULATORY:
        clean['regulatory'] = regulatory

    clean['surgeon'] = sanitize(str(payload.get('surgeon', '') or ''))[:80]
    clean['facility'] = sanitize(str(payload.get('facility', '') or ''))[:80]
    clean['state'] = sanitize(str(payload.get('state', '') or ''))[:10]
    clean['case_type'] = sanitize(str(payload.get('case_type', '') or ''))[:200]
    clean['step'] = sanitize(str(payload.get('step', '') or ''))[:200]
    clean['step_user'] = sanitize(str(payload.get('step_user', '') or ''))[:200]

    clean['date_from'] = validate_date(payload.get('date_from', '') or '')
    clean['date_to'] = validate_date(payload.get('date_to', '') or '')
    clean['use_created_at'] = bool(payload.get('use_created_at', False))
    clean['on_hold_only'] = bool(payload.get('on_hold_only', False))

    clean['step_label'] = str(payload.get('step_label') or 'Case list')[:120]
    return clean

def _nlq_enrich_case_type(cases: list) -> None:
    """Adds case_type_name to each case dict in-place, via a small standalone
    lookup query keyed on case id — kept entirely inside the NLQ module so
    the shared case_select_fields()/case_joins() functions (used by ~6 other
    non-NLQ endpoints) are never modified just to add one NLQ-specific column."""
    ids = [c.get('id') for c in cases if c.get('id')]
    if not ids:
        return
    id_list = "','".join(str(i).replace("'", "") for i in ids)
    try:
        df = client.query(f"SELECT id, case_type_name FROM {tbl('vw_fact_case')} WHERE id IN ('{id_list}')").to_dataframe()
        type_map = dict(zip(df['id'].astype(str), df['case_type_name'].astype(str)))
        for c in cases:
            c['case_type_name'] = type_map.get(str(c.get('id')), '')
    except Exception:
        for c in cases:
            c.setdefault('case_type_name', '')

def _nlq_execute_list_cases(parsed: dict) -> dict:
    """Lists up to 500 individual case rows using the SAME query path as the
    existing /analytics/cases endpoint (build_where_clause, case_select_fields,
    case_joins, signoff_cte, on_hold_cte, on_hold_user_cte, format_cases) — no
    new SQL surface, just the existing trusted case-listing code reused for
    NLQ. include_hold_user=True additionally joins the real put_on_hold_at
    timestamp (from vw_event_log_rank) so Claude has an actual hold date to
    reference instead of inferring one — and case_select_fields already
    includes f.onHoldComment (oh_note) and the last case notes, so Claude has
    real note text to draw from rather than needing to invent any."""
    args = {
        'product': parsed['product'],
        'product_group': parsed['product_group'],
        'laterality': parsed.get('laterality', ''),
        'regulatory': parsed.get('regulatory', ''),
        'surgeon': parsed['surgeon'],
        'facility': parsed['facility'],
        'state': parsed['state'],
        'case_type': parsed.get('case_type', ''),
        'step': parsed.get('step', ''),
        'step_user': parsed.get('step_user', ''),
    }
    class _ArgShim(dict):
        def get(self, k, default=''):
            return dict.get(self, k, default) or default
    shim = _ArgShim(args)

    where = build_where_clause(shim)
    if parsed.get('on_hold_only'):
        where += " AND f.onHold = true"

    date_from = parsed['date_from']
    date_to = parsed['date_to']
    if parsed.get('use_created_at'):
        if date_from: where += f" AND f.createdAt >= '{validate_date(date_from)}'"
        if date_to: where += f" AND f.createdAt <= '{validate_date(date_to)} 23:59:59'"

    query = f"WITH {signoff_cte()}, {on_hold_cte()}, {on_hold_user_cte()} SELECT {case_select_fields(include_hold_user=True)} {case_joins(include_hold_user=True)} WHERE {where} ORDER BY f.createdAt DESC LIMIT 500"
    df = client.query(query).to_dataframe()
    cases = format_cases(df)
    _nlq_enrich_case_type(cases)
    return {
        "step_label": parsed['step_label'],
        "cases": cases,
        "case_count": len(cases),
        "interpreted_as": {k: v for k, v in parsed.items() if v not in ('', None, False)},
    }

def _nlq_validate_derived_metric(payload: dict) -> dict:
    """Allowlist-validates a derived-metric request. metric MUST be one of the
    known formulas — nothing here can select an arbitrary endpoint or field."""
    clean = {}
    metric = payload.get('metric')
    clean['metric'] = metric if metric in NLQ_DERIVED_METRICS else 'utilization'

    granularity = payload.get('granularity')
    clean['granularity'] = granularity if granularity in NLQ_DERIVED_GRANULARITY else 'weekly'

    def _filter_csv(value, allowed):
        if not value:
            return ''
        items = [v.strip() for v in str(value).split(',') if v.strip()]
        kept = [v for v in items if v in allowed]
        return ','.join(kept)

    clean['product'] = _filter_csv(payload.get('product', ''), set(NLQ_PRODUCTS))
    clean['product_group'] = _filter_csv(payload.get('product_group', ''), set(NLQ_PRODUCT_GROUPS))
    clean['step'] = sanitize(str(payload.get('step', '') or ''))[:200]
    clean['user'] = sanitize(str(payload.get('user', '') or ''))[:200]
    clean['surgeon'] = sanitize(str(payload.get('surgeon', '') or ''))[:80]
    clean['date_from'] = validate_date(payload.get('date_from', '') or '')
    clean['date_to'] = validate_date(payload.get('date_to', '') or '')

    try:
        top_n = int(payload.get('top_n') or 0)
        clean['top_n'] = max(0, min(top_n, 100))
    except (TypeError, ValueError):
        clean['top_n'] = 0
    clean['sort_desc'] = bool(payload.get('sort_desc', True))
    clean['step_label'] = str(payload.get('step_label') or clean['metric'])[:120]
    return clean

def _nlq_execute_derived_metric(parsed: dict) -> dict:
    """Calls the SAME real endpoints the app's own dashboards use (R3ID's
    /analytics/utilization, /process-efficiency, /fpy, /wip; Report Generator's
    /report/queue-time, /report/otd) via real HTTP requests to those now-separate
    services — no formula is re-derived or approximated here. This guarantees
    NLQ numbers always match the dashboard numbers exactly, for both the
    underlying figures and any per-user rounding. Requires R3ID_SERVICE_URL and
    REPORT_GENERATOR_SERVICE_URL to be set to those services' deployed URLs."""
    metric = parsed['metric']
    qs = {}
    if parsed['product']: qs['product'] = parsed['product']
    if parsed['product_group']: qs['product_group'] = parsed['product_group']
    if parsed['date_from']: qs['date_from'] = parsed['date_from']
    if parsed['date_to']: qs['date_to'] = parsed['date_to']

    if metric == 'utilization':
        qs['granularity'] = parsed['granularity']
        if parsed['step']: qs['step'] = parsed['step']
        if parsed['user']: qs['step_user'] = parsed['user']
        payload = _fetch_from_service(R3ID_SERVICE_URL, '/analytics/utilization', qs, 'R3ID')
        rows = payload.get('users', [])
        value_key = 'utilization'
    elif metric == 'process_efficiency':
        qs['granularity'] = parsed['granularity']
        payload = _fetch_from_service(R3ID_SERVICE_URL, '/analytics/process-efficiency', qs, 'R3ID')
        rows = payload.get('users', [])
        value_key = 'efficiency'
    elif metric == 'fpy':
        qs['granularity'] = parsed['granularity']
        if parsed['step']: qs['step'] = parsed['step']
        if parsed['user']: qs['step_user'] = parsed['user']
        payload = _fetch_from_service(R3ID_SERVICE_URL, '/analytics/fpy', qs, 'R3ID')
        rows = payload.get('users', [])
        value_key = 'fpy'
    elif metric == 'queue_time':
        payload = _fetch_from_service(REPORT_GENERATOR_SERVICE_URL, '/analytics/report/queue-time', qs, 'Report Generator')
        rows = payload.get('steps', [])
        value_key = 'avg_queue_hours'
    elif metric == 'otd':
        if parsed['surgeon']: qs['surgeon'] = parsed['surgeon']
        payload = _fetch_from_service(REPORT_GENERATOR_SERVICE_URL, '/analytics/report/otd', qs, 'Report Generator')
        rows = payload.get('rows', payload.get('periods', payload.get('data', [])))
        value_key = 'otd_pct'
    else:
        rows, value_key, payload = [], None, {}

    if parsed.get('top_n') and value_key and rows and isinstance(rows, list) and rows and value_key in rows[0]:
        rows = sorted(rows, key=lambda r: (r.get(value_key) or 0), reverse=parsed['sort_desc'])[:parsed['top_n']]

    return {
        "step_label": parsed['step_label'],
        "metric": metric,
        "rows": rows,
        "team_avg": payload.get('team_avg'),
        "interpreted_as": {k: v for k, v in parsed.items() if v not in ('', None, False, 0)},
    }

def _nlq_validate_wip(payload: dict) -> dict:
    """Allowlist-validates a WIP-stage request. stages MUST be drawn from the
    real named buckets in analytics_wip() — nothing here can invent a stage
    or bypass its multi-condition bucket logic."""
    clean = {}
    stages_raw = payload.get('stages', '')
    stages = [s.strip() for s in str(stages_raw).split(',') if s.strip()]
    clean['stages'] = [s for s in stages if s in NLQ_WIP_STAGES] or ['segmentation']

    def _filter_csv(value, allowed):
        if not value:
            return ''
        items = [v.strip() for v in str(value).split(',') if v.strip()]
        kept = [v for v in items if v in allowed]
        return ','.join(kept)

    clean['product'] = _filter_csv(payload.get('product', ''), set(NLQ_PRODUCTS))
    clean['product_group'] = _filter_csv(payload.get('product_group', ''), set(NLQ_PRODUCT_GROUPS))

    laterality = payload.get('laterality', '')
    if laterality in NLQ_LATERALITY:
        clean['laterality'] = laterality
    regulatory = payload.get('regulatory', '')
    if regulatory in NLQ_REGULATORY:
        clean['regulatory'] = regulatory

    clean['surgeon'] = sanitize(str(payload.get('surgeon', '') or ''))[:80]
    clean['facility'] = sanitize(str(payload.get('facility', '') or ''))[:80]
    clean['state'] = sanitize(str(payload.get('state', '') or ''))[:10]
    clean['case_type'] = sanitize(str(payload.get('case_type', '') or ''))[:200]
    clean['step'] = sanitize(str(payload.get('step', '') or ''))[:200]
    clean['step_user'] = sanitize(str(payload.get('step_user', '') or ''))[:200]
    clean['step_label'] = str(payload.get('step_label') or 'WIP cases')[:120]
    return clean

def _nlq_execute_wip(parsed: dict) -> dict:
    """Lists WIP cases for one or more named pipeline stages by calling the
    real analytics_wip() function in-process (fetch_step mode) — the exact
    same multi-condition bucket logic used by the WIP dashboard (including
    the surgeon-rejected-rework routing). One call per requested stage;
    results are combined and tagged with which stage each case came from."""
    qs = {}
    if parsed['product']: qs['product'] = parsed['product']
    if parsed['product_group']: qs['product_group'] = parsed['product_group']
    if parsed.get('laterality'): qs['laterality'] = parsed['laterality']
    if parsed.get('regulatory'): qs['regulatory'] = parsed['regulatory']
    if parsed['surgeon']: qs['surgeon'] = parsed['surgeon']
    if parsed['facility']: qs['facility'] = parsed['facility']
    if parsed['state']: qs['state'] = parsed['state']
    if parsed.get('case_type'): qs['case_type'] = parsed['case_type']
    if parsed.get('step'): qs['step'] = parsed['step']
    if parsed.get('step_user'): qs['step_user'] = parsed['step_user']

    all_cases = []
    for stage in parsed['stages']:
        stage_qs = dict(qs)
        stage_qs['fetch_step'] = stage
        payload = _fetch_from_service(R3ID_SERVICE_URL, '/analytics/wip', stage_qs, 'R3ID')
        for c in payload.get('cases', []):
            c = dict(c)
            c['wip_stage'] = stage
            all_cases.append(c)

    all_cases = all_cases[:500]
    _nlq_enrich_case_type(all_cases)
    return {
        "step_label": parsed['step_label'],
        "cases": all_cases,
        "case_count": len(all_cases),
        "stages": parsed['stages'],
        "interpreted_as": {k: v for k, v in parsed.items() if v not in ('', None, False, [])},
    }

@app.route("/analytics/nlq", methods=["POST"])
def analytics_nlq():
    """
    Claude-powered natural-language query router — multi-step capable.
    Body: { "question": "...free text..." }

    Claude NEVER writes SQL and NEVER sees raw table/column names — it only
    picks from the fixed metric/group_by/product vocabulary defined above
    (NLQ_METRICS / NLQ_GROUP_BY / PRODUCT_GROUPS), which mirrors exactly what
    /analytics/query already validates and runs. For compound questions
    (e.g. "top 3 case types by volume, then their avg lead time"), Claude can
    call run_case_query up to NLQ_MAX_STEPS times, using the real result of
    each step to decide the next step's filters. Every single call — no
    matter how many steps — is re-validated (_nlq_validate) and executed only
    through the existing trusted query builders (_nlq_execute), so a
    hallucinated field can never reach BigQuery at any step. Claude's final
    text answer is generated only after seeing the actual numbers returned,
    and is instructed to use only those numbers.
    """
    if not _anthropic_client:
        return jsonify({"error": "NLQ is not configured. Set ANTHROPIC_API_KEY on the server."}), 503
    NLQ_MAX_STEPS = 4
    NLQ_MAX_HISTORY_TURNS = 6
    try:
        body = request.get_json(silent=True) or {}
        question = (body.get('question') or '').strip()
        if not question:
            return jsonify({"error": "Missing 'question'."}), 400

        # ── Conversation history — plain Q&A text only, NOT prior tool calls. ──
        # This lets follow-ups like "what about just RTSA?" resolve against the prior
        # question's topic, without replaying old tool-call payloads (which would bloat
        # every request and mostly isn't relevant to the new question anyway). Each
        # entry is trusted only as much as anything else in a user message — it still
        # flows through the exact same tool-call validation as a fresh question.
        raw_history = body.get('history') or []
        history_turns = []
        if isinstance(raw_history, list):
            for turn in raw_history[-NLQ_MAX_HISTORY_TURNS:]:
                if not isinstance(turn, dict):
                    continue
                h_q = str(turn.get('question', '') or '').strip()[:500]
                h_a = str(turn.get('answer_text', '') or '').strip()[:1000]
                if h_q and h_a:
                    history_turns.append({"role": "user", "content": h_q})
                    history_turns.append({"role": "assistant", "content": h_a})

        # ── Pinned filters from the dedicated NLQ filter panel — always applied, silently, to every tool call ──
        # Validated through the SAME allowlist logic as everything else (same _filter_csv pattern), so a bad
        # or tampered value here can no more reach BigQuery than a hallucinated one from Claude could.
        raw_filters = body.get('filters') or {}
        def _pin_csv(value, allowed):
            if not value: return ''
            items = [v.strip() for v in str(value).split(',') if v.strip()]
            return ','.join(v for v in items if v in allowed)
        pinned_filters = {
            'product': _pin_csv(raw_filters.get('product', ''), set(NLQ_PRODUCTS)),
            'product_group': _pin_csv(raw_filters.get('product_group', ''), set(NLQ_PRODUCT_GROUPS)),
            'case_type': sanitize(str(raw_filters.get('case_type', '') or ''))[:300],
            'step': sanitize(str(raw_filters.get('step', '') or ''))[:300],
            'user': sanitize(str(raw_filters.get('user', '') or ''))[:300],
            'date_from': validate_date(raw_filters.get('date_from', '') or ''),
            'date_to': validate_date(raw_filters.get('date_to', '') or ''),
        }
        pinned_filters = {k: v for k, v in pinned_filters.items() if v}

        today_str = datetime.date.today().isoformat()
        system_prompt = _nlq_system_prompt().replace("{today}", today_str).replace("{max_steps}", str(NLQ_MAX_STEPS))
        if history_turns:
            system_prompt += (
                "\n\nCONVERSATION CONTEXT: The person may be following up on their own earlier questions in this "
                "session (shown below as prior turns). Use that context to resolve references like 'what about "
                "just RTSA' or 'same thing but last month' — but still only use the approved tool vocabulary; "
                "never assume a metric/product/date from history that wasn't actually validated in this turn's "
                "own tool calls. CRITICALLY: the filter panel can change between turns. Ignore any filter values "
                "mentioned in your OWN prior responses in this conversation — those describe what was pinned AT "
                "THE TIME, which may no longer be true. Only ever trust the PINNED FILTERS line in THIS system "
                "prompt (below) for what's currently pinned right now, on this turn."
            )
        if pinned_filters:
            pin_desc = ", ".join(f"{k}={v}" for k, v in pinned_filters.items())
            system_prompt += (
                f"\n\nPINNED FILTERS RIGHT NOW (from the person's filter panel, as of THIS turn — ALREADY APPLIED "
                f"to every tool call automatically, you do not need to and should not set these yourself): {pin_desc}. "
                f"These take priority over anything conflicting in the question text or in earlier conversation turns; "
                f"if the question names a different product/date than these pinned filters, mention the conflict in "
                f"your final answer rather than silently picking one."
            )
        else:
            system_prompt += (
                "\n\nPINNED FILTERS RIGHT NOW: none. The filter panel is currently empty/unset on this turn, "
                "regardless of what may have been pinned in an earlier turn of this conversation. Do not assume "
                "any product, case type, step, or date range is still pinned unless the person's question text "
                "itself names one."
            )

        messages = history_turns + [{"role": "user", "content": question}]
        steps_used = []
        final_text = None

        for step_num in range(NLQ_MAX_STEPS + 1):
            force_stop = step_num >= NLQ_MAX_STEPS
            msg = _anthropic_client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=800,
                system=system_prompt if not force_stop else system_prompt + "\n\nYou have used all available query steps. Respond now with your final text answer only, using the results already gathered — do not call the tool again.",
                tools=[] if force_stop else [NLQ_TOOL, NLQ_LIST_CASES_TOOL, NLQ_DERIVED_METRIC_TOOL, NLQ_WIP_TOOL],
                messages=messages,
            )

            tool_blocks = [b for b in msg.content if b.type == "tool_use"]
            text_blocks = [b.text for b in msg.content if b.type == "text"]

            if not tool_blocks:
                final_text = "\n".join(text_blocks).strip() or "Could not produce a final answer from the gathered results."
                break

            messages.append({"role": "assistant", "content": msg.content})
            tool_results_content = []
            for tb in tool_blocks:
                try:
                    tb_input = dict(tb.input or {})
                    # Pinned filters always win — applied last, after Claude's own input, before validation.
                    # Field names differ slightly per tool schema (e.g. derived_metric_query uses 'user' where
                    # the others use 'step_user'), so map onto whatever each tool actually accepts.
                    if pinned_filters.get('product'): tb_input['product'] = pinned_filters['product']
                    if pinned_filters.get('product_group'): tb_input['product_group'] = pinned_filters['product_group']
                    if pinned_filters.get('date_from'): tb_input['date_from'] = pinned_filters['date_from']
                    if pinned_filters.get('date_to'): tb_input['date_to'] = pinned_filters['date_to']
                    if tb.name in ("run_case_query", "list_cases", "list_wip_cases"):
                        if pinned_filters.get('case_type'): tb_input['case_type'] = pinned_filters['case_type']
                        if pinned_filters.get('step'): tb_input['step'] = pinned_filters['step']
                        if pinned_filters.get('user'): tb_input['step_user'] = pinned_filters['user']
                    elif tb.name == "derived_metric_query":
                        if pinned_filters.get('step'): tb_input['step'] = pinned_filters['step']
                        if pinned_filters.get('user'): tb_input['user'] = pinned_filters['user']

                    if tb.name == "list_cases":
                        parsed = _nlq_validate_list_cases(tb_input)
                        result = _nlq_execute_list_cases(parsed)
                    elif tb.name == "derived_metric_query":
                        parsed = _nlq_validate_derived_metric(tb_input)
                        result = _nlq_execute_derived_metric(parsed)
                    elif tb.name == "list_wip_cases":
                        parsed = _nlq_validate_wip(tb_input)
                        result = _nlq_execute_wip(parsed)
                    else:
                        parsed = _nlq_validate(tb_input)
                        result = _nlq_execute(parsed)
                except Exception as qe:
                    result = {"step_label": (tb.input or {}).get('step_label', ''), "error": str(qe)}
                steps_used.append(result)
                tool_results_content.append({
                    "type": "tool_result",
                    "tool_use_id": tb.id,
                    "content": json.dumps(result, default=str)[:6000],
                })
            messages.append({"role": "user", "content": tool_results_content})

            if msg.stop_reason != "tool_use":
                # Claude stopped calling tools on its own after seeing this step's result-round;
                # loop again once more without tools to force the final text answer.
                continue

        if final_text is None:
            # Exhausted steps without a natural stop — ask once more for a final answer, no tools.
            wrapup = _anthropic_client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=500,
                system="Summarize the answer in 1-3 sentences using only the numbers already gathered in this conversation. Do not call any tool.",
                messages=messages,
            )
            final_text = "\n".join(b.text for b in wrapup.content if b.type == "text").strip()

        # ── Structural fix: the displayed table/chart is ALWAYS ground truth. Any case-count number in Claude's ──
        # text is rewritten to match it — not just flagged. Two layers:
        #  1) On-hold auto-correction: if the question is about on-hold cases and the table wasn't actually
        #     scoped to on-hold-only, re-filter the rows here (the onHold field is already in the data).
        #  2) Universal text correction: rewrite any "N [adjective(s)] case(s)" phrase — where an adjective
        #     can be a hyphenated or plain word like "on-hold", "WIP", "completed", "active" — to the actual
        #     ground-truth count. The prompt tells Claude NOT to state counts at all, but if it does anyway,
        #     the number gets corrected instead of shipping a lie.
        # Note: the "N cases" phrasing regex allows up to 3 intervening word-characters (hyphenated is OK).
        # This catches "7 on-hold cases", "7 WIP cases", "294 completed cases", "42 active cases", etc.
        last_step = steps_used[-1] if steps_used else None
        question_is_about_hold = bool(re.search(r'\bon.?hold\b|\bhold\b', question, re.IGNORECASE))
        if last_step and isinstance(last_step.get('cases'), list) and last_step['cases'] and question_is_about_hold:
            cases = last_step['cases']
            already_hold_only = all(str(c.get('onHold', '')).strip().lower() == 'true' for c in cases)
            if not already_hold_only:
                filtered = [c for c in cases if str(c.get('onHold', '')).strip().lower() == 'true']
                last_step['cases'] = filtered
                last_step['case_count'] = len(filtered)
                last_step.setdefault('interpreted_as', {})['on_hold_only'] = True

        # Matches: "N cases", "N adj cases", "N adj adj cases", "N adj-adj cases" — up to 3 intervening tokens.
        # Skips "top N ... cases" specifically since "top 2 case types" etc. is a filter descriptor, not a count.
        CASE_COUNT_RE = re.compile(r'(?<!top\s)\b\d+(?:\s+[\w-]+){0,3}\s+cases?\b', re.IGNORECASE)
        if last_step and 'case_count' in last_step:
            actual_count = last_step['case_count']
            noun = "case" if actual_count == 1 else "cases"
            final_text = CASE_COUNT_RE.sub(f"{actual_count} {noun}", final_text)
        elif last_step and 'value' in last_step:
            try:
                actual_value = last_step['value']
                noun = "case" if str(actual_value) == "1" else "cases"
                final_text = CASE_COUNT_RE.sub(f"{actual_value} {noun}", final_text)
            except Exception:
                pass

        return jsonify({
            "answer_text": final_text,
            "steps": steps_used,
            "step_count": len(steps_used),
        })
    except Exception as e:
        return handle_error(request.endpoint, e)

_LOG = "`restor3d-data-warehouse.production3_logging_public.Log`"

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=True, host="0.0.0.0", port=port)

