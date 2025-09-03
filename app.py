import io, os, re, json, time, requests
import numpy as np
import pandas as pd
from datetime import datetime
import streamlit as st

# =========================
# Config
# =========================
USDA_BASE = "https://rdgdwe.sc.egov.usda.gov/arcgis/rest/services/Eligibility/Eligibility/MapServer"
USDA_LAYER_RBS = 2                 # RBS ineligible polygons (layer 2 = ineligible)
TIMEOUT = 25
SLEEP_ON_LIMIT = 2.0               # backoff if Google returns OVER_QUERY_LIMIT

# If a single full-address column exists, we can use it instead of parts.
FULL_ADDRESS_CANDIDATES = [
    # Grata
    "mailing address", "headquarters", "hq address", "location", "full address",
    # PitchBook
    "hq location",
]

# Base aliases (original)
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

# Extend aliases for Grata + PitchBook + convenience
ALIASES.update({
    # IDs / names / links
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

    # Full-address fields (normalize to one name)
    "mailing address": "address_full",
    "headquarters": "address_full",
    "hq address": "address_full",
    "location": "address_full",
    "full address": "address_full",
    "hq location": "address_full",

    # Optional metadata (kept if present; not required)
    "hq country/territory/region": "country",
    "primary industry code": "industry_code",
})

st.set_page_config(page_title="USDA RBS Eligibility Checker", layout="wide")
st.title("USDA RBS Eligibility Checker")
st.caption("Upload an Excel (.xlsx). We geocode with Google, check USDA RBS polygons, and return results.")

# Prefer secrets (Streamlit Cloud) but allow env var fallback
API_KEY = st.secrets.get("GOOGLE_MAPS_API_KEY", os.getenv("GOOGLE_MAPS_API_KEY", ""))


# =========================
# Helpers
# =========================
def _canon(col_name: str) -> str:
    """Normalize a header using ALIASES (case/space-insensitive)."""
    k = str(col_name).strip().lower()
    return ALIASES.get(k, k)

def _coalesce_str(series: pd.Series) -> pd.Series:
    """Return a string, trimmed, with NaNs -> ''."""
    return series.fillna("").astype(str).str.strip()

@st.cache_data(show_spinner=False)
def _load_excel(bytes_) -> pd.DataFrame:
    """Load and normalize an .xlsx into a DataFrame with flexible headers."""
    df = pd.read_excel(io.BytesIO(bytes_), dtype=str, engine="openpyxl")
    df.columns = [_canon(c) for c in df.columns]
    cols = set(df.columns)

    # Prefer an existing non-empty 'address_full'
    has_full_col = ("address_full" in cols) and df["address_full"].astype(str).str.strip().ne("").any()

    # Classic 4-part format present?
    required_parts = {"street", "city", "state", "zip"}
    has_parts = required_parts.issubset(cols)

    # If neither, try to promote a candidate to 'address_full'
    if not (has_full_col or has_parts):
        for cand in FULL_ADDRESS_CANDIDATES:
            if cand in cols and df[cand].astype(str).str.strip().ne("").any():
                df["address_full"] = df[cand]
                has_full_col = True
                break

    if not (has_full_col or has_parts):
        present = ", ".join(df.columns)
        raise ValueError(
            "Could not find address columns. Provide either: "
            "(1) columns street, city, state, zip; or "
            "(2) a single full-address column (e.g., 'Headquarters', 'Mailing Address', or 'HQ Location'). "
            f"Present columns: [{present}]"
        )

    # Ensure optional columns
    if "id" not in cols:
        df["id"] = np.arange(1, len(df) + 1).astype(str)
    if "company" not in cols:
        df["company"] = ""

    # Normalize/derive ZIP
    if "zip" in df.columns:
        df["zip"] = _coalesce_str(df["zip"]).str.extract(r"(\d{5})", expand=False)
    elif has_full_col:
        df["zip"] = _coalesce_str(df["address_full"]).str.extract(r"(\d{5})", expand=False)
    else:
        df["zip"] = np.nan

    return df.reset_index(drop=True)

def google_geocode(address: str):
    """Geocode with Google; return (lon, lat) or None."""
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
    """Return True if inside an ineligible polygon."""
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
        st.warning("Please upload an .xlsx file first.")
        st.stop()

    # Preflight: key required
    if not API_KEY:
        st.error("GOOGLE_MAPS_API_KEY is not set. Add it in the app’s Settings → Secrets and rerun.")
        st.stop()

    with st.spinner("Processing…"):
        # Load + normalize headers
        try:
            df = _load_excel(upload.read())
        except Exception as e:
            st.error(str(e))
            st.stop()

        # Ensure columns exist even if source only had a full address
        for col in ["street", "city", "state", "zip"]:
            if col not in df.columns:
                df[col] = ""

        # -------- Row-level validation & bad-rows export (NO .str on DataFrame!) --------
        required_per_row = ["street", "city", "state", "zip"]
        # Missing mask
        missing_mask = df[required_per_row].isna()
        # Blank-string mask (strip per column)
        blank_mask = df[required_per_row].apply(lambda s: s.fillna("").astype(str).str.strip().eq(""))
        bad_mask = missing_mask.any(axis=1) | blank_mask.any(axis=1)

        df_bad = df.loc[bad_mask].copy()
        df_good = df.loc[~bad_mask].copy()

        if len(df_bad) > 0:
            st.warning(
                f"{len(df_bad)} row(s) are missing required fields or a valid 5-digit ZIP. "
                "They’ll be skipped here. Download to fix and re-upload."
            )
            bad_csv = df_bad.to_csv(index=False).encode()
            st.download_button("Download problematic rows (CSV)", data=bad_csv, file_name="rows_to_fix.csv", mime="text/csv")

        # Work with good rows for geocoding
        df = df_good.reset_index(drop=True)

        # Prep derived columns
        df["geocode_success"] = False
        df["lon"] = np.nan
        df["lat"] = np.nan
        df["eligible_rbs"] = None

        # Build full_address from either a provided full-address field or the parts
        if "address_full" in df.columns and df["address_full"].astype(str).str.strip().ne("").any():
            df["full_address"] = _coalesce_str(df["address_full"])
        else:
            street_s = _coalesce_str(df["street"])
            city_s   = _coalesce_str(df["city"])
            state_s  = _coalesce_str(df["state"])
            zip_s    = _coalesce_str(df["zip"]).str.extract(r"(\d{5})", expand=False).fillna("")
            df["full_address"] = (street_s + ", " + city_s + ", " + state_s + " " + zip_s).str.strip()

        # Optional: show detected columns
        with st.expander("Detected columns (debug)"):
            st.write(sorted(df.columns))
            st.write(df.head(3))

        # Geocode (cached by full address)
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

        # Output columns (ensure present)
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

        # NOTE: Streamlit deprecation fix — use width='stretch' instead of use_container_width
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
    - Rows missing required fields get offered as a separate CSV to fix.
    """)
