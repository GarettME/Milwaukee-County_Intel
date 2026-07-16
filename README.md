# Milwaukee County Intel

Weekly motivated-seller lead scraper for **Milwaukee County, WI** — the sibling
of the Ventura County and San Diego County intel tools. Unlike those (which
fight Tyler/AcclaimWeb recorder portals with Playwright), Milwaukee publishes
everything as clean, free **bulk open data**, so this one is a fast, dependency-
light pull — no browser, no proxy, no paid API key.

## What it does

1. Pulls the **distress signals** that define the motivated-seller universe:
   - **Delinquent Real Estate Tax Accounts** (City Treasurer) — owner, mailing,
     levy year(s), principal owed.
   - **Vacant Building registrations** (Dept. of Neighborhood Services / Accela).
2. Uses **MPROP — the Master Property File** (City Assessor) as the property
   spine, joined on the 10-digit `TAXKEY`, to add owner name, mailing address,
   **assessed value** (equity), land use, year built, and **owner-occupancy**.
   MPROP is the assessment roll *and* the property-tax roll in one file — the
   same data the City's "Property Assessment Search" exposes one parcel at a time.
3. **Scores & ranks** every parcel that carries at least one distress signal
   (tax delinquency and/or vacant registration), boosting for: big / chronic
   tax debt, vacant + delinquent combos, absentee & out-of-state owners, high
   equity, entity ("tired landlord") owners, and long-time ownership.
4. Flags **"new this week"** by diffing against the previously committed run.
5. Exports:
   - `data/records.json` — ranked leads (also copied to `dashboard/`)
   - `data/ghl_export.csv` — GoHighLevel-ready import
   - `dashboard/index.html` — filterable web dashboard (GitHub Pages)

## Data sources (all free, refreshed nightly by the City)

| Dataset | Department | How it's used |
|---|---|---|
| MPROP Master Property File | City Assessor | Property spine: owner, mailing, **assessed value**, land use, occupancy |
| Assessment Roll / Property Tax Roll | Assessor / Treasurer | Included in MPROP |
| Delinquent Real Estate Tax Accounts | City Treasurer | **Primary distress signal** + $ severity |
| Vacant Buildings (Accela) | Neighborhood Services / City Open Data | Walk-away / abandonment signal |
| GIS Parcel layer | Milwaukee County Land Information Office | Optional geometry (`MILW_GIS_ENRICH=1`) |

### Datasets the user asked about that are **gated** (not on the open portal)

These live behind subscription / anti-bot walls and are **intentionally not
scraped** (matching how the Ventura tool documents ReportAll). If Garett obtains
access, they can be wired as additional signal modules:

| Dataset | Where it lives | Access reality |
|---|---|---|
| Recorded Deeds, Mortgage Records | Register of Deeds — **Landshark** | Paid subscription portal; ToS forbids scraping. `Property Sales Data` on the open portal is a free partial proxy for recent conveyances. |
| Foreclosures | Clerk of Courts — **WCCA** (case type FC) | State court portal, behind anti-bot; ToS-restricted. |
| Probate Cases | Probate Court — **WCCA** (case type IN/PR) | Same as above. |

Note: MPROP *does* carry `TAX_DELQ`, `BI_VIOL`, and `RAZE_STATUS` columns, but
they are **masked to placeholder sentinels** in the public export, so we rely on
the dedicated Treasurer/DNS feeds instead.

## Scoring

Base 10, then (capped at 100):

- Tax delinquent present **+12**; balance ≥$20k **+28**, ≥$10k **+22**, ≥$5k **+16**, ≥$2k **+9**, ≥$500 **+4**
- Chronic delinquency: ≥4 yrs **+22**, 3 yrs **+16**, 2 yrs **+9**
- Vacant building **+32**; vacant **and** delinquent **+8**
- Absentee owner **+8**; out-of-state owner **+12**
- Equity (assessed): ≥$500k **+16**, ≥$250k **+9**, ≥$120k **+4**
- LLC / corp owner **+4**; owned 15+ years **+4**; new this week **+5**

Typical run: ~12–13k leads → roughly 1.5k High (≥70) / 7k Medium / 4k Low.

## Running

```bash
pip install -r scraper/requirements.txt
python scraper/fetch.py
```

Environment toggles:

| Var | Default | Effect |
|---|---|---|
| `MILW_MIN_DELINQUENT` | `0` | Drop delinquent balances below this dollar amount |
| `MILW_INCLUDE_VACANT` | `1` | Include vacant-building registrations |
| `MILW_GIS_ENRICH` | `0` | Attach Milwaukee County LIO parcel geometry (stub hook) |

## Automation

`.github/workflows/scraper.yml` runs **every Monday at 08:00 UTC**, commits the
refreshed data, and deploys the dashboard to GitHub Pages
(Settings → Pages → Source: GitHub Actions).
