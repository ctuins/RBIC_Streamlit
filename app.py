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

# Allow a single full-address column instead of parts
FULL_ADDRESS_CANDIDATES = [
    # Grata
    "mailing address"
]

# Base aliases
ALIASES = {
    # company/name
    "company": "company",
    "company name": "company", "company_name": "company", "business": "company",
    "business name": "company", "firm": "company", "organization": "company",
    "organisation": "company", "org": "company", "client": "company", "customer": "company",

    # address parts
    "address": "street", "address1": "street", "addr": "street", "street address": "street",
    "address line 1": "street",

    # zip/state variants
    "zipcode": "zip", "zip_code": "zip", "postal": "zip", "postal_code": "zip", "province": "state",
}

# Extend aliases for Grata + PitchBook
ALIASES.update({
    # ids / names / links
    "company id": "id",
    "companies": "company",
    "name": "company",
    "website": "domain",
    "view company online": "source_link",
    "grata link": "source_link",

    # PitchBook parts
    "hq address line 1": "street",
    "hq city": "city",
    "hq state/province": "state",
    "hq state": "state",
    "hq post code": "zip",
    "hq postcode": "zip",
    "hq postal code": "zip",

    # full-address fields -> unified name
    "mailing address": "address_full",
    "headquarters": "address_full",
    "hq address": "address_full",
    "location": "address_full",
    "full address": "address_full",
    "hq location": "address_full",

    # optional metadata
    "hq country/territory/region": "country",
    "primary industry code": "industry_code",
})

st.set_page_config(page_title="USDA RBS Eligibility Checker", layout="wide")
st.title("USDA RBS Eligibility Checker")
st.caption("Upload an Excel (.xlsx). We geocode with Google, check USDA RBS polygons, and return results.")

API_KEY = st.secrets.get("GOOGLE_MAPS_API_KEY", os.getenv("GOOGLE_MAPS_API_KEY", ""))

# =========================
# Helpers
# =========================
def _canon(name: str) -> str:
    k = str(name).strip().lower()
    return ALIASES.get(k, k)

def _coalesce_str(series: pd.Series) -> pd.Series:
    return series.astype(str).replace({"<NA>": "", "nan": "", "None": None}).fillna("").str.strip()

def _first_nonempty_rowwise(df_sub: pd.DataFrame) -> pd.Series:
    """Take first non-empty value per row across given columns; returns a Series."""
    if df_sub.shape[1] == 0:
        return pd.Series([""] * len(df_sub), index=df_sub.index)
    norm = df_sub.apply(lambda s: s.astype(str).str.strip())
    return norm.apply(lambda row: next((v for v in row if v not in ("", "nan", "None")), ""), axis=1)

@st.cache_data(show_spinner=False)
def _load_excel(bytes_) -> pd.DataFrame:
    df = pd.read_excel(io.BytesIO(bytes_), dtype=str, engine="openpyxl")
    df.columns = [_canon(c) for c in df.columns]

    # Coalesce duplicates that canonicalized to the same name (so we always have Series, not DataFrames)
    for key in ["address_full", "street", "city", "state", "zip", "company", "id"]:
        dup_cols = [c for c in df.columns if c == key]
        if len(dup_cols) > 1:
            s = _first_nonempty_rowwise(df.loc[:, dup_cols])
            df.drop(columns=dup_cols, inplace=True)
            df[key] = s

    cols = set(df.columns)

    # Prefer non-empty address_full if present; else promote a candidate
    has_full_col = ("address_full" in cols) and _coalesce_str(df["address_full"]).ne("").any()
    if not has_full_col:
        for cand in FULL_ADDRESS_CANDIDATES:
            if cand in cols and _coalesce_str(df[cand]).ne("").any():
                df["address_full"] = df[cand]
                has_full_col = True
                break

    # If neither full nor parts exist -> helpful error
    required_parts = {"street", "city", "state", "zip"}
    has_parts = required_parts.issubset(set(df.columns))
    if not (has_full_col or has_parts):
        present = ", ".join(df.columns)
        raise ValueError(
            "Could not find address columns. Provide either: "
            "(1) columns street, city, state, zip; or "
            "(2) a single full-address column (e.g., 'Headquarters', 'Mailing Address', or 'HQ Location'). "
            f"Present columns: [{present}]"
        )

    # Ensure optional
    if "id" not in df.columns:
        df["id"] = np.arange(1, len(df) + 1).astype(str)
    if "company" not in df.columns:
        df["company"] = ""

    # Derive/normalize ZIP (not strictly required in full-address mode)
    if "zip" in df.columns:
        df["zip"] = _coalesce_str(df["zip"]).str.extract(r"(\d{5})", expand=False)
    elif has_full_col:
        df["zip"] = _coalesce_str(df["address_full"]).str.extract(r"(\d{5})", expand=False)
    else:
        df["zip"] = np.nan

    return df.reset_index(drop=True)

def google_geocode(address: str):
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
                loc = data["results"][0]["geometry"]["location"]
                return float(loc["lng"]), float(loc["lat"])
            elif status in ("OVER_QUERY_LIMIT", "RESOURCE_EXHAUSTED"):
                time.sleep(SLEEP_ON_LIMIT * attempt); continue
            elif status in ("ZERO_RESULTS", "INVALID_REQUEST", "REQUEST_DENIED", "UNKNOWN_ERROR"):
                if status == "UNKNOWN_ERROR" and attempt < 3:
                    time.sleep(0.8 * attempt); continue
                return None
            else:
                return None
        except Exception:
            if attempt == 5:
                return None
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
    help="Provide street/city/state/zip OR a single full-address column (Headquarters/Mailing Address/HQ Location)."
)
run = st.button("Process file", type="primary")

if run:
    if upload is None:
        st.warning("Please upload an .xlsx file first."); st.stop()

    if not API_KEY:
        st.error("GOOGLE_MAPS_API_KEY is not set. Add it in the app’s Settings → Secrets and rerun."); st.stop()

    with st.spinner("Processing…"):
        try:
            df = _load_excel(upload.read())
        except Exception as e:
            st.error(str(e)); st.stop()

        # Ensure expected columns exist even in full-address mode (for consistent outputs)
        for col in ["street", "city", "state", "zip"]:
            if col not in df.columns:
                df[col] = ""

        # ---------- Build full_address FIRST ----------
        has_address_full = ("address_full" in df.columns) and _coalesce_str(df["address_full"]).ne("").any()
        if has_address_full:
            df["full_address"] = _coalesce_str(df["address_full"])
            mode = "full"
        elif all(c in df.columns for c in ["street", "city", "state", "zip"]):
            street_s = _coalesce_str(df["street"])
            city_s   = _coalesce_str(df["city"])
            state_s  = _coalesce_str(df["state"])
            zip_s    = _coalesce_str(df["zip"]).str.extract(r"(\d{5})", expand=False).fillna("")
            df["full_address"] = (street_s + ", " + city_s + ", " + state_s + " " + zip_s).str.strip()
            mode = "parts"
        else:
            st.error("Cannot construct full_address; missing both full-address and parts."); st.stop()

        # ---------- Validation depends on mode ----------
        if mode == "full":
            # Only require 'full_address' to be non-empty
            bad_mask = df["full_address"].astype(str).str.strip().eq("")
        else:  # parts mode
            required_per_row = ["street", "city", "state", "zip"]
            missing_mask = df[required_per_row].isna()
            blank_mask = df[required_per_row].apply(lambda s: s.fillna("").astype(str).str.strip().eq(""))
            bad_mask = missing_mask.any(axis=1) | blank_mask.any(axis=1)

        df_bad = df.loc[bad_mask].copy()
        df_good = df.loc[~bad_mask].copy()

        if len(df_bad) > 0:
            st.warning(
                f"{len(df_bad)} row(s) are missing required address info for this mode ({mode}). "
                "They’ll be skipped here. Download to fix and re-upload."
            )
            bad_csv = df_bad.to_csv(index=False).encode()
            st.download_button("Download problematic rows (CSV)", data=bad_csv,
                               file_name="rows_to_fix.csv", mime="text/csv")

        # Work with good rows
        df = df_good.reset_index(drop=True)

        # Prep derived columns
        df["geocode_success"] = False
        df["lon"] = np.nan
        df["lat"] = np.nan
        df["eligible_rbs"] = None

        # Debug view
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
                df.at[i, "lon"], df.at[i, "lat"] = ll
                df.at[i, "geocode_success"] = True

        # USDA check
        for i, r in df.iterrows():
            if not r["geocode_success"]:
                continue
            try:
                in_inel = usda_in_ineligible(r["lon"], r["lat"])
                df.at[i, "eligible_rbs"] = (not in_inel)
            except Exception:
                df.at[i, "eligible_rbs"] = None

        # Output columns
        out_cols = ["id","company","street","city","state","zip","lon","lat","geocode_success","eligible_rbs"]
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

        st.download_button(
            "Download CSV",
            data=csv_bytes,
            file_name=f"usda_rbs_google_{stamp}.csv",
            mime="text/csv",
        )
        st.download_button(
            "Download XLSX",
            data=xlsx_buf.getvalue(),
            file_name=f"usda_rbs_google_{stamp}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

with st.expander("About & caveats"):
    st.write("""
    - Geocoding uses Google Maps Geocoding API. Set `GOOGLE_MAPS_API_KEY` in the app’s **Settings → Secrets**.
    - `eligible_rbs = True` means the point is **not** inside an RBS ineligible polygon.
    - Accepts classic 4-part addresses, Grata (Headquarters/Mailing Address), and PitchBook (HQ Location or HQ Address Line 1/City/State/Post Code).
    - Validation adapts to the detected mode: full-address OR parts.
    """)
