"""
Milwaukee County Intel — Motivated Seller Lead Scraper
======================================================
Sibling of the Ventura County / San Diego County scrapers, but Milwaukee's
public data is *far* better: everything comes from free, structured bulk
open-data feeds — no Playwright, no anti-bot portal, no paid parcel API.

Sources (all free, refreshed nightly by the City):
  • MPROP — Master Property File (City Assessor)            [the property spine]
        https://data.milwaukee.gov/dataset/mprop
        Assessment Roll + Property Tax Roll + owner + mailing address +
        assessed value + land use + year built + owner-occupancy, one row per
        parcel keyed by the 10-digit TAXKEY.  Doubles as the "City of Milwaukee
        Property Assessment Search" the assessor site exposes one-parcel-at-a-time.
  • Delinquent Real Estate Tax Accounts (City Treasurer)   [primary distress]
        https://data.milwaukee.gov/dataset/delinquent-real-estate-tax-accounts
        Tax Key #, owner, mailing, levy year, principal owed.
  • Vacant Buildings / Accela (Dept. of Neighborhood Services / City Open Data)
        https://data.milwaukee.gov/dataset/accelavacantbuilding          [code / abandonment]
  • Milwaukee County parcel GIS (Land Information Office)   [optional geometry]
        https://lio.milwaukeecountywi.gov/arcgis/rest/services

The datasets join cleanly on the 10-digit zero-padded TAXKEY, so we treat MPROP
as the spine and layer the distress signals on top, then score + rank every
property that carries at least one motivated-seller signal.

Datasets the user asked about that are NOT on the open-data portal (Register of
Deeds mortgages, Clerk-of-Courts foreclosure filings, Probate cases) live behind
the county's Landshark subscription and the state WCCA court portal, both of
which forbid automated scraping / sit behind anti-bot. Those are implemented as
clearly-documented, disabled-by-default modules (see foreclosures.py notes in the
README) so the pipeline is honest about what is wired vs. gated. Property Sales
data IS on the portal and is used as a free recent-conveyance signal.

Outputs (same contract as the Ventura scraper):
  data/records.json
  data/ghl_export.csv
  dashboard/records.json
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT          = Path(__file__).resolve().parent.parent
DATA_DIR      = ROOT / "data"
DASHBOARD_DIR = ROOT / "dashboard"
DATA_DIR.mkdir(parents=True, exist_ok=True)
DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)

RECORDS_JSON   = DATA_DIR      / "records.json"
DASHBOARD_JSON = DASHBOARD_DIR / "records.json"
GHL_CSV        = DATA_DIR      / "ghl_export.csv"

# ── Config: City of Milwaukee Open Data (CKAN) ────────────────────────────────
CKAN_BASE   = "https://data.milwaukee.gov"
CKAN_SQL    = f"{CKAN_BASE}/api/3/action/datastore_search_sql"
CKAN_SEARCH = f"{CKAN_BASE}/api/3/action/datastore_search"

# Resource IDs (confirmed live against data.milwaukee.gov).
RES_MPROP      = "0a2c7f31-cd15-4151-8222-09dd57d5f16d"   # Master Property File
RES_DELINQUENT = "8f1367e1-6f8f-44cc-8ed6-2eecd8267ec7"   # Delinquent Real Estate Tax Accounts
RES_VACANT     = "46dca88b-fec0-48f1-bda6-7296249ea61f"   # Vacant Buildings (Accela)
RES_LANDUSE    = "232354a9-dc45-46e6-aafd-0e3175302725"   # Land-use code → description lookup

MPROP_CSV_URL = (
    f"{CKAN_BASE}/dataset/562ab824-48a5-42cd-b714-87e205e489ba/"
    f"resource/{RES_MPROP}/download/mprop.csv"
)

# A property record links out to the City's public assessment page.
ASSESSMENT_URL = "https://assessments.milwaukee.gov/PropInfoResults.asp?taxkey="

# Behaviour toggles
MIN_DELINQUENT   = float(os.getenv("MILW_MIN_DELINQUENT", "0"))   # drop delinquent balances below this
INCLUDE_VACANT   = os.getenv("MILW_INCLUDE_VACANT", "1").strip() not in ("0", "false", "")
GIS_ENRICH       = os.getenv("MILW_GIS_ENRICH", "0").strip() not in ("0", "false", "")  # optional geometry
HTTP_TIMEOUT     = int(os.getenv("MILW_HTTP_TIMEOUT", "90"))
USER_AGENT       = "Milwaukee-County-Intel/1.0 (motivated-seller lead research)"

# Milwaukee County LIO parcel layer (optional — for centroid geometry only).
COUNTY_GIS_URL = os.getenv(
    "MILW_GIS_PARCEL_URL",
    "https://lio.milwaukeecountywi.gov/arcgis/rest/services/PropertyInfo/Parcels/MapServer/0/query",
)

# LAND_USE code → human description. Loaded once at runtime from the City's
# authoritative land-use lookup dataset (falls back to an empty map on error).
LAND_USE_LABELS: dict[str, str] = {}


def load_land_use_labels(session: requests.Session) -> None:
    try:
        rows = _datastore_all(session, RES_LANDUSE)
        for r in rows:
            code = str(r.get("CODE") or "").strip()
            desc = (r.get("DESCRIPTION") or "").strip().title()
            if code and desc and desc.upper() != "NO DESCRIPTION":
                LAND_USE_LABELS[code] = desc
        log.info("Loaded %d land-use labels", len(LAND_USE_LABELS))
    except Exception as e:
        log.warning("Could not load land-use labels (%s) — codes will show raw", e)


# ── Data model ────────────────────────────────────────────────────────────────
@dataclass
class Lead:
    taxkey:       str = ""
    owner:        str = ""
    owner2:       str = ""
    prop_address: str = ""
    prop_city:    str = "Milwaukee"
    prop_state:   str = "WI"
    prop_zip:     str = ""
    mail_address: str = ""
    mail_city:    str = ""
    mail_state:   str = ""
    mail_zip:     str = ""
    land_use:     str = ""
    land_use_label: str = ""
    year_built:   Optional[int]   = None
    units:        Optional[int]   = None
    assessed_land:        Optional[float] = None
    assessed_improvement: Optional[float] = None
    assessed_total:       Optional[float] = None
    convey_date:  str = ""
    convey_price: Optional[float] = None
    owner_occupied: Optional[bool] = None
    # distress signals
    cat:          str = ""          # primary category (drives the dashboard filter)
    cat_label:    str = ""          # primary human label
    signals:      list = field(default_factory=list)   # every category that hit
    delinquent_years:  list = field(default_factory=list)
    delinquent_total:  Optional[float] = None
    vacant_since:      str = ""
    # scoring / output
    flags:        list = field(default_factory=list)
    score:        int  = 0
    record_url:   str  = ""


# ── CKAN helpers ──────────────────────────────────────────────────────────────

def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def _datastore_all(session: requests.Session, resource_id: str) -> list[dict]:
    """Pull an entire CKAN datastore resource (datastore_search, limit high)."""
    out: list[dict] = []
    offset = 0
    while True:
        r = session.get(
            CKAN_SEARCH,
            params={"resource_id": resource_id, "limit": 32000, "offset": offset},
            timeout=HTTP_TIMEOUT,
        )
        r.raise_for_status()
        res = r.json()["result"]
        recs = res.get("records", [])
        out.extend(recs)
        total = res.get("total", 0)
        offset += len(recs)
        if not recs or offset >= total:
            break
    return out


# ── Key normalisation ─────────────────────────────────────────────────────────

def _norm_taxkey(v) -> str:
    """Every Milwaukee dataset keys on a 10-digit zero-padded tax key."""
    digits = "".join(ch for ch in str(v or "") if ch.isdigit())
    return digits.zfill(10) if digits else ""


def _num(v) -> Optional[float]:
    if v in (None, ""):
        return None
    try:
        return float(str(v).replace(",", "").replace("$", "").strip())
    except (TypeError, ValueError):
        return None


def _int(v) -> Optional[int]:
    n = _num(v)
    return int(n) if n is not None else None


# ════════════════════════════════════════════════════════════════════════════
# STEP 1 — Distress signals (define the lead universe)
# ════════════════════════════════════════════════════════════════════════════

def fetch_delinquent(session: requests.Session) -> dict[str, dict]:
    """Delinquent Real Estate Tax Accounts → {taxkey: aggregated info}.

    One taxkey can appear once per delinquent levy year; a property that is
    behind on several years is chronically distressed, so we aggregate the
    principal owed and collect the delinquent years."""
    log.info("Fetching delinquent real-estate tax accounts …")
    rows = _datastore_all(session, RES_DELINQUENT)
    log.info("  %d delinquent account rows", len(rows))

    agg: dict[str, dict] = {}
    for row in rows:
        tk = _norm_taxkey(row.get("Tax Key #"))
        if not tk:
            continue
        principal = _num(row.get("Total Tax Principal")) or 0.0
        year = str(row.get("Levy Year") or "").strip()
        d = agg.setdefault(tk, {
            "total": 0.0, "years": set(), "owner": "", "mail": {},
            "prop_address": "", "ald": "",
        })
        d["total"] += principal
        if year:
            d["years"].add(year)
        # keep the richest owner / mailing info seen for this key
        if not d["owner"] and row.get("Owner's Name"):
            d["owner"] = (row.get("Owner's Name") or "").strip()
        if not d["mail"] and row.get("Owner's Mailing Address"):
            d["mail"] = {
                "addr":  (row.get("Owner's Mailing Address") or "").strip(),
                "city":  (row.get("City") or "").strip(),
                "state": (row.get("State") or "").strip(),
                "zip":   (row.get("Zip") or "").strip(),
            }
        if not d["prop_address"] and row.get("Property Address"):
            d["prop_address"] = (row.get("Property Address") or "").strip()
        d["ald"] = str(row.get("Ald Dist") or "").strip()

    # Finalise: sort years, drop tiny balances if configured.
    out: dict[str, dict] = {}
    for tk, d in agg.items():
        if d["total"] < MIN_DELINQUENT:
            continue
        d["years"] = sorted(d["years"])
        out[tk] = d
    log.info("  %d unique delinquent parcels (min balance $%.0f)", len(out), MIN_DELINQUENT)
    return out


def fetch_vacant(session: requests.Session) -> dict[str, dict]:
    """Vacant Building registrations → {taxkey: info}. A registered vacant
    building is a strong walk-away / motivated-seller signal."""
    if not INCLUDE_VACANT:
        return {}
    log.info("Fetching vacant building registrations …")
    rows = _datastore_all(session, RES_VACANT)
    log.info("  %d vacant building rows", len(rows))

    out: dict[str, dict] = {}
    for row in rows:
        tk = _norm_taxkey(row.get("PARCELNBR"))
        if not tk:
            continue
        opened = str(row.get("DATEOPENED") or "").strip()[:10]
        prev = out.get(tk)
        if prev and prev.get("since", "") >= opened:
            continue   # keep the most recent registration
        out[tk] = {
            "since":   opened,
            "address": (row.get("ADDRFULLLINE") or "").strip(),
            "value_improved": _num(row.get("VALUEIMPROVED")),
            "land_use": str(row.get("LANDUSE") or "").strip(),
        }
    log.info("  %d unique vacant-building parcels", len(out))
    return out


# ════════════════════════════════════════════════════════════════════════════
# STEP 2 — MPROP enrichment (owner, mailing, assessed value, occupancy)
# ════════════════════════════════════════════════════════════════════════════

# MPROP columns we actually use — pulled by TAXKEY so we never download 90 MB.
_MPROP_COLS = [
    "TAXKEY", "HOUSE_NR_LO", "HOUSE_NR_SFX", "SDIR", "STREET", "STTYPE",
    "OWNER_NAME_1", "OWNER_NAME_2", "OWNER_MAIL_ADDR", "OWNER_CITY_STATE",
    "OWNER_ZIP", "GEO_ZIP_CODE", "C_A_LAND", "C_A_IMPRV", "C_A_TOTAL",
    "LAND_USE", "LAND_USE_GP", "OWN_OCPD", "YR_BUILT", "NR_UNITS",
    "CONVEY_DATE", "CONVEY_FEE",
]


def enrich_from_mprop(session: requests.Session, leads: dict[str, Lead]) -> None:
    """Fill owner / mailing / assessed value / occupancy from MPROP for each
    lead taxkey, via chunked CKAN SQL (only the ~20k distress parcels, not the
    whole 160k file)."""
    taxkeys = list(leads.keys())
    if not taxkeys:
        return
    log.info("Enriching %d parcels from MPROP …", len(taxkeys))
    cols = ", ".join(f'"{c}"' for c in _MPROP_COLS)

    matched = 0
    CHUNK = 250
    for i in range(0, len(taxkeys), CHUNK):
        chunk = taxkeys[i:i + CHUNK]
        in_list = ", ".join("'" + tk.replace("'", "") + "'" for tk in chunk)
        sql = f'SELECT {cols} FROM "{RES_MPROP}" WHERE "TAXKEY" IN ({in_list})'
        try:
            # POST avoids URL-length limits on the IN clause.
            r = session.post(CKAN_SQL, data={"sql": sql}, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            records = r.json()["result"]["records"]
        except Exception as e:
            log.warning("  MPROP chunk %d failed: %s", i // CHUNK, e)
            continue
        for m in records:
            tk = _norm_taxkey(m.get("TAXKEY"))
            lead = leads.get(tk)
            if not lead:
                continue
            _apply_mprop(lead, m)
            matched += 1
        time.sleep(0.1)

    log.info("  MPROP matched %d/%d parcels", matched, len(taxkeys))


def _apply_mprop(lead: Lead, m: dict) -> None:
    # Property address (assemble from parts if the distress feed didn't have it)
    if not lead.prop_address:
        parts = [m.get("HOUSE_NR_LO"), m.get("HOUSE_NR_SFX"), m.get("SDIR"),
                 m.get("STREET"), m.get("STTYPE")]
        lead.prop_address = " ".join(str(p) for p in parts if p and str(p) != "0").strip()
    lead.prop_zip = lead.prop_zip or (str(m.get("GEO_ZIP_CODE") or "").strip()[:5])

    # Owner + mailing (MPROP is authoritative; only fill blanks)
    lead.owner  = lead.owner  or (m.get("OWNER_NAME_1") or "").strip()
    lead.owner2 = lead.owner2 or (m.get("OWNER_NAME_2") or "").strip()
    if not lead.mail_address:
        lead.mail_address = (m.get("OWNER_MAIL_ADDR") or "").strip()
        city_state = (m.get("OWNER_CITY_STATE") or "").strip()
        # OWNER_CITY_STATE is like "MILWAUKEE WI" — split trailing 2-letter state
        if city_state:
            bits = city_state.rsplit(" ", 1)
            if len(bits) == 2 and len(bits[1]) == 2 and bits[1].isalpha():
                lead.mail_city, lead.mail_state = bits[0].rstrip(", ").strip(), bits[1]
            else:
                lead.mail_city = city_state.rstrip(", ").strip()
        lead.mail_zip = (str(m.get("OWNER_ZIP") or "").strip()[:5]).lstrip("0") or ""

    # Assessment (equity signal)
    lead.assessed_land        = _num(m.get("C_A_LAND"))
    lead.assessed_improvement = _num(m.get("C_A_IMPRV"))
    lead.assessed_total       = _num(m.get("C_A_TOTAL"))

    # Property characteristics
    lead.land_use   = str(m.get("LAND_USE") or "").strip()
    lead.year_built = _int(m.get("YR_BUILT")) or None
    lead.units      = _int(m.get("NR_UNITS")) or None
    lead.land_use_label = LAND_USE_LABELS.get(lead.land_use, "")

    # Owner-occupancy: 'O' = owner-occupied, anything else = absentee.
    occ = (m.get("OWN_OCPD") or "").strip().upper()
    lead.owner_occupied = True if occ == "O" else False

    # Last conveyance (deed) — free recent-sale signal.
    lead.convey_date  = str(m.get("CONVEY_DATE") or "").strip()[:10]
    lead.convey_price = _num(m.get("CONVEY_FEE"))


# ════════════════════════════════════════════════════════════════════════════
# STEP 3 — Assemble leads from the signal maps
# ════════════════════════════════════════════════════════════════════════════

def build_leads(delinquent: dict[str, dict], vacant: dict[str, dict]) -> dict[str, Lead]:
    """A property becomes a lead if it carries at least one distress signal
    (tax delinquency or a vacant-building registration). Absentee / equity are
    scoring modifiers, not lead qualifiers — otherwise every rental in the city
    would flood the list."""
    leads: dict[str, Lead] = {}

    for tk, d in delinquent.items():
        lead = leads.setdefault(tk, Lead(taxkey=tk))
        lead.signals.append("tax_delinquent")
        lead.delinquent_total = round(d["total"], 2)
        lead.delinquent_years = d["years"]
        lead.owner = lead.owner or d.get("owner", "")
        lead.prop_address = lead.prop_address or d.get("prop_address", "")
        mail = d.get("mail") or {}
        if mail:
            lead.mail_address = mail.get("addr", "")
            lead.mail_city    = mail.get("city", "")
            lead.mail_state   = mail.get("state", "")
            lead.mail_zip     = (mail.get("zip", "") or "")[:5]

    for tk, v in vacant.items():
        lead = leads.setdefault(tk, Lead(taxkey=tk))
        lead.signals.append("vacant")
        lead.vacant_since = v.get("since", "")
        lead.prop_address = lead.prop_address or v.get("address", "")

    for tk, lead in leads.items():
        lead.record_url = ASSESSMENT_URL + tk
    return leads


# ════════════════════════════════════════════════════════════════════════════
# STEP 4 — Scoring
# ════════════════════════════════════════════════════════════════════════════

# Primary category precedence (most-motivated first) for the dashboard filter.
_CAT_PRECEDENCE = [
    ("vacant",         "Vacant Building"),
    ("tax_delinquent", "Tax Delinquent"),
]


def _entity_owner(name: str) -> bool:
    import re
    return bool(re.search(r"\b(LLC|INC|CORP|CO|LP|LLP|LTD|TRUST|PROPERTIES|INVESTMENT|HOLDINGS|REALTY|GROUP|BANK)\b",
                          (name or "").upper()))


def score_lead(lead: Lead) -> None:
    # Weights are tuned so a bare, tiny, owner-occupied delinquency lands in the
    # low tier while the genuinely-motivated cases (chronic debt, big balances,
    # out-of-state / absentee owners, vacant + delinquent, high equity) climb
    # into the high tier — otherwise "tax delinquent" alone floods the top.
    score = 10
    flags: list[str] = []
    sig = set(lead.signals)

    # ── Tax delinquency ──────────────────────────────────────────────────────
    if "tax_delinquent" in sig:
        flags.append("Tax delinquent")
        score += 12
        bal = lead.delinquent_total or 0
        if   bal >= 20_000: flags.append("Large tax debt (≥$20k)"); score += 28
        elif bal >= 10_000: flags.append("Tax debt ≥$10k");         score += 22
        elif bal >= 5_000:  flags.append("Tax debt ≥$5k");          score += 16
        elif bal >= 2_000:  score += 9
        elif bal >= 500:    score += 4
        n_years = len(lead.delinquent_years)
        if   n_years >= 4: flags.append(f"Chronic ({n_years} yrs delinquent)"); score += 22
        elif n_years == 3: flags.append("3 years delinquent");                  score += 16
        elif n_years == 2: flags.append("2 years delinquent");                  score += 9

    # ── Vacant building ──────────────────────────────────────────────────────
    if "vacant" in sig:
        flags.append("Vacant building")
        score += 32

    # ── Combo: vacant + delinquent = walk-away ───────────────────────────────
    if "vacant" in sig and "tax_delinquent" in sig:
        flags.append("Vacant + delinquent")
        score += 8

    # ── Absentee owner (OWN_OCPD != 'O') ─────────────────────────────────────
    if lead.owner_occupied is False:
        flags.append("Absentee owner")
        score += 8

    # ── Out-of-state owner ───────────────────────────────────────────────────
    if lead.mail_state and lead.mail_state.upper() not in ("WI", ""):
        flags.append("Out-of-state owner")
        score += 12

    # ── Equity (assessed value) ──────────────────────────────────────────────
    if lead.assessed_total:
        if   lead.assessed_total >= 500_000: flags.append("High equity (≥$500k)"); score += 16
        elif lead.assessed_total >= 250_000: flags.append("Equity ≥$250k");        score += 9
        elif lead.assessed_total >= 120_000: score += 4

    # ── Entity owner (tired landlord / investor) ─────────────────────────────
    if _entity_owner(lead.owner):
        flags.append("LLC / corp owner")
        score += 4

    # ── Long-time owner (more accumulated equity) ────────────────────────────
    yr = _conveyance_year(lead.convey_date)
    if yr and (datetime.now(timezone.utc).year - yr) >= 15:
        flags.append("Owned 15+ years")
        score += 4

    lead.flags = flags
    lead.score = min(score, 100)

    # Primary category for the dashboard filter.
    for key, label in _CAT_PRECEDENCE:
        if key in sig:
            lead.cat, lead.cat_label = key, label
            break


def _conveyance_year(s: str) -> Optional[int]:
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(s.strip()[:10], fmt).year
        except ValueError:
            continue
    # bare 4-digit year fallback
    d = "".join(ch for ch in s if ch.isdigit())
    if len(d) >= 4 and d[:4].isdigit():
        y = int(d[:4])
        if 1900 <= y <= datetime.now().year:
            return y
    return None


# ════════════════════════════════════════════════════════════════════════════
# STEP 5 — "New this week" via diff against the last committed run
# ════════════════════════════════════════════════════════════════════════════

def flag_new_this_week(leads: list[Lead]) -> None:
    """The weekly cadence means freshness = "appeared since last run". Compare
    this run's taxkeys against the previously committed records.json and flag
    parcels that are newly distressed."""
    prev: set[str] = set()
    if RECORDS_JSON.exists():
        try:
            old = json.loads(RECORDS_JSON.read_text(encoding="utf-8"))
            prev = {r.get("taxkey", "") for r in old.get("records", [])}
        except (json.JSONDecodeError, OSError):
            prev = set()
    if not prev:
        return   # first run — don't flag everything as new
    n_new = 0
    for lead in leads:
        if lead.taxkey not in prev:
            lead.flags.append("New this week")
            lead.score = min(lead.score + 5, 100)
            n_new += 1
    log.info("Flagged %d new-this-week leads", n_new)


# ════════════════════════════════════════════════════════════════════════════
# OUTPUT
# ════════════════════════════════════════════════════════════════════════════

def save_records_json(leads: list[Lead]) -> None:
    with_addr = sum(1 for l in leads if l.prop_address)
    payload = {
        "fetched_at":   datetime.now(timezone.utc).isoformat(),
        "source":       "City of Milwaukee Open Data (MPROP, Treasurer, DNS) + Milwaukee County LIO",
        "county":       "Milwaukee County, WI",
        "total":        len(leads),
        "with_address": with_addr,
        "records":      [asdict(l) for l in leads],
    }
    j = json.dumps(payload, indent=2, default=str)
    RECORDS_JSON.write_text(j, encoding="utf-8")
    DASHBOARD_JSON.write_text(j, encoding="utf-8")
    log.info("Saved %d leads to records.json (%d with address)", len(leads), with_addr)


def save_ghl_csv(leads: list[Lead]) -> None:
    cols = [
        "First Name", "Last Name", "Owner Name",
        "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
        "Property Address", "Property City", "Property State", "Property Zip",
        "Tax Key", "Land Use", "Assessed Value", "Year Built", "Units",
        "Owner Occupied", "Lead Type", "Signals",
        "Delinquent Balance", "Delinquent Years", "Vacant Since",
        "Seller Score", "Motivated Seller Flags", "Source", "Record URL",
    ]

    def split_name(full: str):
        # Entities keep the whole name in Last Name so GHL doesn't mangle them.
        if _entity_owner(full):
            return "", full.strip()
        parts = full.strip().split()
        if not parts:
            return "", ""
        return (" ".join(parts[1:]), parts[0]) if len(parts) > 1 else (parts[0], "")

    with open(GHL_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for l in leads:
            first, last = split_name(l.owner)
            w.writerow({
                "First Name":             first,
                "Last Name":              last,
                "Owner Name":             l.owner,
                "Mailing Address":        l.mail_address,
                "Mailing City":           l.mail_city,
                "Mailing State":          l.mail_state,
                "Mailing Zip":            l.mail_zip,
                "Property Address":       l.prop_address,
                "Property City":          l.prop_city,
                "Property State":         l.prop_state,
                "Property Zip":           l.prop_zip,
                "Tax Key":                l.taxkey,
                "Land Use":               l.land_use_label or l.land_use,
                "Assessed Value":         f"{l.assessed_total:.0f}" if l.assessed_total else "",
                "Year Built":             l.year_built or "",
                "Units":                  l.units or "",
                "Owner Occupied":         "" if l.owner_occupied is None else ("Yes" if l.owner_occupied else "No"),
                "Lead Type":              l.cat_label,
                "Signals":                " | ".join(l.signals),
                "Delinquent Balance":     f"{l.delinquent_total:.2f}" if l.delinquent_total else "",
                "Delinquent Years":       ", ".join(l.delinquent_years),
                "Vacant Since":           l.vacant_since,
                "Seller Score":           l.score,
                "Motivated Seller Flags": " | ".join(l.flags),
                "Source":                 "City of Milwaukee Open Data",
                "Record URL":             l.record_url,
            })
    log.info("Saved GHL CSV → %s", GHL_CSV)


# ════════════════════════════════════════════════════════════════════════════
# OPTIONAL — Milwaukee County GIS geometry (centroid) by TAXKEY
# ════════════════════════════════════════════════════════════════════════════

def enrich_geometry(session: requests.Session, leads: dict[str, Lead]) -> None:
    """Optional: attach a lat/lon centroid from the Milwaukee County LIO parcel
    layer (Land Information Office). Off by default — MPROP already carries all
    the data we score on; geometry is only useful if Garett wants to map leads.
    Enable with MILW_GIS_ENRICH=1."""
    if not GIS_ENRICH:
        return
    log.info("GIS geometry enrichment enabled — querying Milwaukee County LIO …")
    # Left as a best-effort, non-blocking hook. The LIO layer keys parcels on
    # TAXKEY; a production build would batch `TAXKEY IN (...)` with
    # returnCentroid=true. Kept minimal so a GIS outage never breaks the run.
    log.info("  (geometry hook is a stub — see README)")


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def main() -> None:
    log.info("╔══════════════════════════════════════════════════╗")
    log.info("║  Milwaukee County Intel — Seller Lead Scraper    ║")
    log.info("╚══════════════════════════════════════════════════╝")

    session = _session()
    load_land_use_labels(session)

    # 1. Distress signals define the lead universe
    delinquent = fetch_delinquent(session)
    vacant     = fetch_vacant(session)

    # 2. Assemble leads
    leads_map = build_leads(delinquent, vacant)
    log.info("Lead universe: %d parcels with ≥1 distress signal", len(leads_map))

    # 3. Enrich from MPROP (owner, mailing, assessed value, occupancy)
    enrich_from_mprop(session, leads_map)
    enrich_geometry(session, leads_map)

    # 4. Score
    leads = list(leads_map.values())
    for lead in leads:
        score_lead(lead)

    # 5. Weekly freshness (before sort so the +5 is reflected in ranking)
    flag_new_this_week(leads)

    leads.sort(key=lambda l: l.score, reverse=True)

    # 6. Save
    save_records_json(leads)
    save_ghl_csv(leads)

    # Summary
    high = [l for l in leads if l.score >= 70]
    med  = [l for l in leads if 40 <= l.score < 70]
    low  = [l for l in leads if l.score < 40]
    n_vacant = sum(1 for l in leads if "vacant" in l.signals)
    n_absentee = sum(1 for l in leads if l.owner_occupied is False)
    log.info("─" * 52)
    log.info("  Total leads   : %d", len(leads))
    log.info("  High ≥70      : %d", len(high))
    log.info("  Med 40-69     : %d", len(med))
    log.info("  Low <40       : %d", len(low))
    log.info("  Vacant        : %d", n_vacant)
    log.info("  Absentee      : %d", n_absentee)
    if leads:
        top = leads[0]
        log.info("  Top lead      : %s [score=%d] %s", top.owner or "?", top.score, top.prop_address)
    log.info("─" * 52)

    summary_path = os.getenv("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a") as f:
            f.write("## Milwaukee County Intel — Lead Scraper Results\n\n")
            f.write("| Metric | Value |\n|--------|-------|\n")
            f.write(f"| Total leads | {len(leads)} |\n")
            f.write(f"| High score (≥70) | {len(high)} |\n")
            f.write(f"| Medium score (40-69) | {len(med)} |\n")
            f.write(f"| Low score (<40) | {len(low)} |\n")
            f.write(f"| Vacant buildings | {n_vacant} |\n")
            f.write(f"| Absentee owners | {n_absentee} |\n")
            f.write(f"| Generated at | {datetime.now(timezone.utc).isoformat()} |\n")


if __name__ == "__main__":
    main()
