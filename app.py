import io, os, re, json, time, requests
import numpy as np
import pandas as pd
from datetime import datetime
import streamlit as st

# =========================
# Config
# =========================
USDA_BASE = "https://rdgdwe.sc.egov.usda.gov/arcgis/rest/services/Eligibility/Eligibility/MapServer"
USDA_LAYER_RBS = 2                 # layer 2 = RBS ineligible polygons
TIMEOUT = 25
SLEEP_ON_LIMIT = 2.0

# IMPORTANT: Full-address mode will ONLY use "Mailing Address".
# HQ-style columns are NOT used as a full address.
# (PitchBook HQ Address Line 1 etc. are accepted as PARTS when present.)
ALIASES = {
    # company/name
    "company": "company",
    "company name": "company", "company_name": "company", "business": "company",
    "business name": "company", "firm": "company", "organization": "company",
    "organisation": "company", "org": "company", "client": "company", "customer": "company",
    "companies": "company",
    "name": "company",

    # ids / links / misc
    "company id": "id",
    "website": "domain",
    "view company online": "source_link",
    "grata link": "source_link",

    # classic address parts
    "address": "street", "address1": "street", "addr": "street", "street address": "street",
    "address line 1": "street",

    # pitchbook parts (treated as parts, not full)
    "hq address line 1": "street",
    "hq city": "city",
    "hq state/province": "state",
    "hq state": "state",
    "hq post code": "zip",
    "hq postcode": "zip",
    "hq postal code": "zip",

    # zip/state variants
    "zipcode": "zip", "zip_code": "zip", "postal": "zip", "postal_code": "zip", "province": "state",

    # FULL MODE: ONLY mailing address is allowed as full-address
    "mailing address": "address_full",

    # NOTE: intentionally NOT mapping any of these to full:
    # "headquarters", "hq address", "hq location", "location", "full address"
}

st.set_page_config(page_title="USDA RBS Eligibility Checker", layout="wide")
st.title("USDA RBS Eligibility Checker")
st.caption("Upload an Excel (.xlsx). We geocode with Google, check USDA RBS polygons, and return results.")

# Prefer secrets (Streamlit Cloud) but allow env var fallback
API_KEY = st.secrets.get("GOOGLE_MAPS_API_KEY", os.getenv("GOOGLE_MAPS_API_KEY", ""))


# =========================
# Helpers
# =========================
def _canon(name: str) -> str:
    k = str(name).strip().lower()
    return ALIASES.get(k, k)

def _norm_series(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
         .replace({"<NA>": "", "nan": "", "None": None})
         .fillna("")
         .str.replace(r"\s+", " ", regex=True)
         .str.strip()
    )

def _first_nonempty_rowwise(df_sub: pd.DataFrame) -> pd.Series:
    """Take first non-empty value per row across given columns; returns a Series."""
    if df_sub.shape[1] == 0:
        return pd.Series([""] * len(df_sub), index=df_sub.index)
    norm = df_sub.apply(_norm_series)
    return norm.apply(lambda row: next((v for v in row if v != ""), ""), axis=1)

def _comp(components, type_name, short=True):
    """Extract an address component by Google type (e.g., 'locality', 'postal_code')."""
    for c in components or []:
        if type_name in c.get("types", []):
            return c.get("short_name" if short else "long_name", "")
    return ""

@st.cache_data(show_spinner=False)
def _load_excel(bytes_) -> pd.DataFrame:
    """Load and normalize .xlsx; coalesce canonical duplicates to avoid DataFrame .str errors."""
    df = pd.read_excel(io.BytesIO(bytes_), dtype=str, engine="openpyxl")
    df.columns = [_canon(c) for c in df.columns]

    # Coalesce duplicates so each canonical name is a Series
    for key in ["company","id","street","city","state","zip","address_full"]:
        dup = [c for c in df.columns if c == key]
        if len(dup) > 1:
            s = _first_nonempty_rowwise(df.loc[:, dup])
            df.drop(columns=dup, inplace=True)
            df[key] = s

    # Ensure optional
    if "id" not in df.columns:
        df["id"] = np.arange(1, len(df) + 1).astype(str)
    if "company" not in df.columns:
        df["company"] = ""

    # Normalize key columns if present
    for c in ["street","city","state","zip","address_full"]:
        if c in df.columns:
            df[c] = _norm_series(df[c])

    # Derive/normalize ZIP (helpful even in full mode)
    if "zip" in df.columns:
        df["zip"] = df["zip"].str.extract(r"(\d{5})", expand=False)

    return df.reset_index(drop=True)

def google_geocode(address: str):
    """
    Geocode with Google; return dict with lon/lat/formatted/components, or None.
    Also used to backfill parts when starting from a full address.
    """
    if not API_KEY:
        return None
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {"address": address, "key": API_KEY, "region": "us"}
    for attempt in range(1, 6):
        try:
            resp = requests.get(url, params=params, timeout=TIMEOUT)
            data = resp.json()
            status = data.get("status", "")
            if status == "OK":
                res = data["results"][0]
                loc = res["geometry"]["location"]
                formatted = res.get("formatted_address", "")
                comps = res.get("address_components", [])
                return {
                    "lon": float(loc["lng"]),
                    "lat": float(loc["lat"]),
                    "formatted": formatted,
                    "components": comps,
                }
            elif status in ("OVER_QUERY_LIMIT", "RESOURCE_EXHAUSTED"):
                time.sleep(SLEEP_ON_LIMIT * attempt); continue
            elif status in ("ZERO_RESULTS", "INVALID_REQUEST", "REQUEST_DENIED", "UNKNOWN_ERROR"):
                if status == "UNKNOWN_ERROR" and attempt < 3:
                    time.sleep(0.8 * attempt); continue
                return None
            else:
                return None
        except Exception:
            if attempt == 5: return None
            time.sleep(0.5 * attempt)
    return None

def usda_in_ineligible(lon: float, lat: float, layer_id: int = USDA_LAYER_RBS) -> bool:
    url = f"{USDA_BASE}/{layer_id}/query"
    geometry = json.dumps({"x": float(lon), "y": float(lat), "spatialReference": {"wkid": 4326}})
    params = {
        "f": "json",
        "geometry": geometry,
        "geometryType": "esriGeometryPoint",
        "inSR": 4326,
        "spatialRel": "esriSpatialRelIntersects",
        "returnGeometry": "false",
        "outFields": "OBJECTID",
        "where": "1=1",
    }
    r = requests.get(url, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    data = r.json()
    return len(data.get("features", [])) > 0


# =========================
# UI
# =========================
upload = st.file_uploader(
    "Upload Excel (.xlsx)",
    type=["xlsx"],
    help="Provide street/city/state/zip OR a single 'Mailing Address' column (full-address mode)."
)
run = st.button("Process file", type="primary")

if run:
    if upload is None:
        st.warning("Please upload an .xlsx file first."); st.stop()
    if not API_KEY:
        st.error("GOOGLE_MAPS_API_KEY is not set. Add it in Settings → Secrets and rerun."); st.stop()

    with st.spinner("Processing…"):
        try:
            df = _load_excel(upload.read())
        except Exception as e:
            st.error(str(e)); st.stop()

        # Ensure consistent output columns exist
        for col in ["street", "city", "state", "zip"]:
            if col not in df.columns:
                df[col] = ""

        # ---------- Build full_address using ONLY Mailing Address if present ----------
        has_full = ("address_full" in df.columns) and df["address_full"].str.strip().ne("").any()
        if has_full:
            df["full_address"] = df["address_full"].str.replace(r"[\r\n]+", ", ", regex=True).str.strip()
            mode = "full"   # mailing-address-only full mode
        elif all(c in df.columns for c in ["street","city","state","zip"]):
            # Fallback to parts mode
            zip_s = df["zip"].astype(str).str.extract(r"(\d{5})", expand=False).fillna("")
            df["full_address"] = (
                df["street"].astype(str).str.strip() + ", " +
                df["city"].astype(str).str.strip() + ", " +
                df["state"].astype(str).str.strip() + " " +
                zip_s
            ).str.strip()
            mode = "parts"
        else:
            st.error("Could not find a 'Mailing Address' column or the 4 parts (street/city/state/zip)."); st.stop()

        # Show both input and Google-formatted address in outputs
        df["input_address"] = df["full_address"]
        df["formatted_address"] = ""  # filled after geocoding

        # ---------- Validation depends on mode ----------
        if mode == "full":
            bad_mask = df["full_address"].astype(str).str.strip().eq("")
        else:
            req = ["street","city","state","zip"]
            missing_mask = df[req].isna()
            blank_mask = df[req].apply(lambda s: s.fillna("").astype(str).str.strip().eq(""))
            bad_mask = missing_mask.any(axis=1) | blank_mask.any(axis=1)

        df_bad = df.loc[bad_mask].copy()
        df_good = df.loc[~bad_mask].copy()

        if len(df_bad) > 0:
            st.warning(
                f"{len(df_bad)} row(s) are missing required address info for this mode ({mode}). "
                "They’ll be skipped. Download to fix and re-upload."
            )
            st.download_button(
                "Download problematic rows (CSV)",
                data=df_bad.to_csv(index=False).encode(),
                file_name="rows_to_fix.csv",
                mime="text/csv",
            )

        # Work with good rows
        df = df_good.reset_index(drop=True)

        # Prepare output fields
        df["geocode_success"] = False
        df["lon"] = np.nan
        df["lat"] = np.nan
        df["eligible_rbs"] = None

        # Debug panel
        with st.expander("Detected columns (debug)"):
            st.write({"mode": mode})
            st.write(sorted(df.columns))
            st.write(df.head(3))

        # Geocode (cached)
        cache = {}
        for i, r in df.iterrows():
            a = r["full_address"]
            ll = cache.get(a)
            if ll is None:
                ll = google_geocode(a)
                cache[a] = ll
            if ll:
                df.at[i, "lon"] = ll["lon"]
                df.at[i, "lat"] = ll["lat"]
                df.at[i, "geocode_success"] = True
                df.at[i, "formatted_address"] = ll["formatted"]

                # Backfill parts if they were blank (nice for full mode)
                if not str(df.at[i, "city"]).strip():
                    df.at[i, "city"]  = _comp(ll["components"], "locality") or _comp(ll["components"], "sublocality")
                if not str(df.at[i, "state"]).strip():
                    df.at[i, "state"] = _comp(ll["components"], "administrative_area_level_1", short=True)
                if not str(df.at[i, "zip"]).strip():
                    df.at[i, "zip"]   = _comp(ll["components"], "postal_code", short=True)
                if not str(df.at[i, "street"]).strip():
                    num  = _comp(ll["components"], "street_number", short=False)
                    rt   = _comp(ll["components"], "route", short=False)
                    street = f"{num} {rt}".strip()
                    if street:
                        df.at[i, "street"] = street

        # USDA check
        for i, r in df.iterrows():
            if not r["geocode_success"]:
                continue
            try:
                in_inel = usda_in_ineligible(r["lon"], r["lat"])
                df.at[i, "eligible_rbs"] = (not in_inel)
            except Exception:
                df.at[i, "eligible_rbs"] = None

        # Output columns (now include input/formatted address)
        out_cols = [
            "id","company",
            "input_address","formatted_address",
            "street","city","state","zip",
            "lon","lat","geocode_success","eligible_rbs"
        ]
        for c in out_cols:
            if c not in df.columns:
                df[c] = "" if c not in ["lon","lat","geocode_success","eligible_rbs"] else np.nan

        # Metrics
        st.subheader("Results")
        c1, c2, c3, c4, c5 = st.columns(5)
        with c1: st.metric("Rows processed", len(df))
        with c2: st.metric("Geocode OK", int(df["geocode_success"].sum()))
        with c3: st.metric("Eligible", int((df["eligible_rbs"] == True).sum()))
        with c4: st.metric("Ineligible", int((df["eligible_rbs"] == False).sum()))
        with c5: st.metric("No result", int(len(df) - (df["eligible_rbs"] == True).sum() - (df["eligible_rbs"] == False).sum()))

        st.dataframe(df[out_cols], width='stretch')

        # Downloads
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_bytes = df[out_cols].to_csv(index=False).encode()
        xlsx_buf = io.BytesIO(); df[out_cols].to_excel(xlsx_buf, index=False)

        st.download_button("Download CSV", data=csv_bytes,
                           file_name=f"usda_rbs_google_{stamp}.csv", mime="text/csv")
        st.download_button("Download XLSX", data=xlsx_buf.getvalue(),
                           file_name=f"usda_rbs_google_{stamp}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

with st.expander("About & caveats"):
    st.write("""
    - FULL mode now uses ONLY the “Mailing Address” column. HQ-style fields are ignored.
    - If “Mailing Address” is missing, the app falls back to the 4-part address (street/city/state/zip) if available.
    - We show both the original input address and Google’s formatted address; parts are backfilled when possible.
    - `eligible_rbs = True` means the point is **not** inside an RBS ineligible polygon.
    """)
