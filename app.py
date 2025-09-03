import io, os, re, json, time, requests
import numpy as np
import pandas as pd
from datetime import datetime
import streamlit as st

# ====== CONFIG ======
USDA_BASE = "https://rdgdwe.sc.egov.usda.gov/arcgis/rest/services/Eligibility/Eligibility/MapServer"
USDA_LAYER_RBS = 2                 # RBS ineligible polygons
TIMEOUT = 25
SLEEP_ON_LIMIT = 2.0               # backoff if Google returns OVER_QUERY_LIMIT

REQUIRED = {"street","city","state","zip"}
OPTIONAL = {"id","company"}
ALIASES = {
    # direct mapping
    "company":"company",
    "address":"street","address1":"street","addr":"street","street address":"street",
    "zipcode":"zip","zip_code":"zip","postal":"zip","postal_code":"zip","province":"state",
    # common company variants
    "company name":"company","company_name":"company","business":"company","business name":"company",
    "firm":"company","organization":"company","organisation":"company","org":"company",
    "client":"company","customer":"company"
}

st.set_page_config(page_title="USDA RBS Eligibility Checker", layout="wide")
st.title("USDA RBS Eligibility Checker")
st.caption("Upload an Excel (.xlsx). We geocode with Google, check USDA RBS polygons, and return results.")

# Read API key from Streamlit Secrets (preferred) or environment
API_KEY = st.secrets.get("GOOGLE_MAPS_API_KEY", os.getenv("GOOGLE_MAPS_API_KEY", ""))

# ====== HELPERS ======
def _canon(c):
    return ALIASES.get(str(c).strip().lower(), str(c).strip().lower())

@st.cache_data(show_spinner=False)
def _load_excel(bytes_):
    df = pd.read_excel(io.BytesIO(bytes_), dtype=str, engine="openpyxl")
    df.columns = [_canon(c) for c in df.columns]

    # Ensure required
    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}. Present: {list(df.columns)}")

    # Ensure optional
    if "id" not in df.columns:
        df["id"] = np.arange(1, len(df)+1).astype(str)
    if "company" not in df.columns:
        df["company"] = ""

    # Normalize/keep all rows
    df["zip"] = df["zip"].astype(str).str.extract(r"(\d{5})", expand=False)
    return df.reset_index(drop=True)

def google_geocode(address: str):
    """
    Returns (lon, lat) or None. Handles common API responses and rate limiting backoff.
    """
    if not API_KEY:
        return None
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {"address": address, "key": API_KEY, "region": "us"}
    for attempt in range(1, 6):
        try:
            resp = requests.get(url, params=params, timeout=TIMEOUT)
            data = resp.json()
            status = data.get("status","")
            if status == "OK":
                loc = data["results"][0]["geometry"]["location"]
                return float(loc["lng"]), float(loc["lat"])
            elif status in ("OVER_QUERY_LIMIT", "RESOURCE_EXHAUSTED"):
                time.sleep(SLEEP_ON_LIMIT * attempt)
                continue
            elif status in ("ZERO_RESULTS","INVALID_REQUEST","REQUEST_DENIED","UNKNOWN_ERROR"):
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
    return len(data.get("features", [])) > 0  # True => inside ineligible polygon

# ====== UI ======
upload = st.file_uploader(
    "Upload Excel (.xlsx)",
    type=["xlsx"],
    help="Required columns: street, city, state, zip. Optional: id, company."
)
run = st.button("Process file", type="primary")

if run:
    if upload is None:
        st.warning("Please upload an .xlsx file first.")
        st.stop()

    with st.spinner("Processing…"):
        try:
            df = _load_excel(upload.read())
        except Exception as e:
            st.error(f"Error reading Excel: {e}")
            st.stop()

        # Prep cols
        df["geocode_success"] = False
        df["lon"] = np.nan
        df["lat"] = np.nan
        df["eligible_rbs"] = None
        df["full_address"] = df.apply(lambda r: f"{r['street']}, {r['city']}, {r['state']} {r['zip']}", axis=1)

        # Geocode with simple cache
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

        out_cols = ["id","company","street","city","state","zip","lon","lat","geocode_success","eligible_rbs"]

        # Metrics
        st.subheader("Results")
        c1, c2, c3, c4, c5 = st.columns(5)
        with c1: st.metric("Rows", len(df))
        with c2: st.metric("Geocode OK", int(df["geocode_success"].sum()))
        with c3: st.metric("Eligible", int((df["eligible_rbs"] == True).sum()))
        with c4: st.metric("Ineligible", int((df["eligible_rbs"] == False).sum()))
        with c5: st.metric("No result", int(len(df) - (df["eligible_rbs"] == True).sum() - (df["eligible_rbs"] == False).sum()))

        st.dataframe(df[out_cols], use_container_width=True)

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
    - Geocoding uses Google Maps Geocoding API. Set `GOOGLE_MAPS_API_KEY` in Streamlit **Secrets**.
    - `eligible_rbs = True` means the point is **not** inside an RBS ineligible polygon.
    - Rate limits may apply; caching reduces repeat lookups for identical addresses.
    - For a key-free demo, you can swap geocoding for ZIP centroids (pgeocode) but note edge-case risk near boundaries.
    """)
