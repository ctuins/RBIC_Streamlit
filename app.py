import io, os, re, json, time, requests
import numpy as np
import pandas as pd
from datetime import datetime
import streamlit as st

# ====== CONFIG ======
USDA_BASE = "https://rdgdwe.sc.egov.usda.gov/arcgis/rest/services/Eligibility/Eligibility/MapServer"
USDA_LAYER_RBS = 2
TIMEOUT = 25
SLEEP_ON_LIMIT = 2.0

# Accept a single full-address column if present
FULL_ADDRESS_CANDIDATES = [
    # Grata
    "mailing address", "headquarters", "hq address", "location", "full address",
    # PitchBook
    "hq location",
]

# Base aliases (your original set)
ALIASES = {
    "company":"company",
    "address":"street","address1":"street","addr":"street","street address":"street",
    "zipcode":"zip","zip_code":"zip","postal":"zip","postal_code":"zip","province":"state",
    "company name":"company","company_name":"company","business":"company","business name":"company",
    "firm":"company","organization":"company","organisation":"company","org":"company",
    "client":"company","customer":"company"
}

# Extend aliases for Grata + PitchBook
ALIASES.update({
    # IDs / names / links
    "company id": "id",
    "companies": "company",
    "name": "company",
    "website": "domain",
    "view company online": "source_link",
    "grata link": "source_link",

    # Classic address variants
    "address line 1": "street",

    # PitchBook parts
    "hq address line 1": "street",
    "hq city": "city",
    "hq state/province": "state",
    "hq state": "state",
    "hq post code": "zip",
    "hq postcode": "zip",
    "hq postal code": "zip",

    # Full-address fields -> normalized to 'address_full'
    "mailing address": "address_full",
    "headquarters": "address_full",
    "hq address": "address_full",
    "location": "address_full",
    "full address": "address_full",
    "hq location": "address_full",

    # Optional metadata if you want later
    "hq country/territory/region": "country",
    "primary industry code": "industry_code",
})

st.set_page_config(page_title="USDA RBS Eligibility Checker", layout="wide")
st.title("USDA RBS Eligibility Checker")
st.caption("Upload an Excel (.xlsx). We geocode with Google, check USDA RBS polygons, and return results.")

API_KEY = st.secrets.get("GOOGLE_MAPS_API_KEY", os.getenv("GOOGLE_MAPS_API_KEY", ""))

# ====== HELPERS ======
def _canon(c: str) -> str:
    return ALIASES.get(str(c).strip().lower(), str(c).strip().lower())

@st.cache_data(show_spinner=False)
def _load_excel(bytes_):
    df = pd.read_excel(io.BytesIO(bytes_), dtype=str, engine="openpyxl")
    df.columns = [_canon(c) for c in df.columns]
    cols = set(df.columns)

    # Prefer a full address column if available and non-empty
    has_full_col = ("address_full" in cols) and df["address_full"].astype(str).str.strip().ne("").any()

    # Classic parts present?
    required_parts = {"street", "city", "state", "zip"}
    has_parts = required_parts.issubset(cols)

    # Promote any candidate to address_full if needed
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

    # Optional columns
    if "id" not in cols:
        df["id"] = np.arange(1, len(df)+1).astype(str)
    if "company" not in cols:
        df["company"] = ""

    # Normalize ZIP or extract from full address
    if "zip" in df.columns:
        df["zip"] = df["zip"].astype(str).str.extract(r"(\d{5})", expand=False)
    elif has_full_col:
        df["zip"] = df["address_full"].astype(str).str.extract(r"(\d{5})", expand=False)
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
            status = data.get("status","")
            if status == "OK":
                loc = data["results"][0]["geometry"]["location"]
                return float(loc["lng"]), float(loc["lat"])
            elif status in ("OVER_QUERY_LIMIT", "RESOURCE_EXHAUSTED"):
                time.sleep(SLEEP_ON_LIMIT * attempt); continue
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
    return len(data.get("features", [])) > 0

# ====== UI ======
upload = st.file_uploader(
    "Upload Excel (.xlsx)",
    type=["xlsx"],
    help="Provide street/city/state/zip OR a single full-address column (Headquarters/Mailing Address/HQ Location)."
)
run = st.button("Process file", type="primary")

if run:
    if upload is None:
        st.warning("Please upload an .xlsx file first."); st.stop()

    # Preflight: friendly error if key is missing
    if not API_KEY:
        st.error("GOOGLE_MAPS_API_KEY is not set. Add it in app Settings → Secrets and rerun."); st.stop()

    with st.spinner("Processing…"):
        try:
            df = _load_excel(upload.read())
        except Exception as e:
            st.error(str(e)); st.stop()

        # Prep columns
        df["geocode_success"] = False
        df["lon"] = np.nan
        df["lat"] = np.nan
        df["eligible_rbs"] = None

        # Build full_address robustly
        if "address_full" in df.columns and df["address_full"].astype(str).str.strip().ne("").any():
            df["full_address"] = df["address_full"].fillna("").astype(str)
        elif all(c in df.columns for c in ["street","city","state","zip"]):
            df["zip"] = df["zip"].astype(str).str.extract(r"(\d{5})", expand=False)
            df["full_address"] = df.apply(
                lambda r: f"{(r['street'] or '')}, {(r['city'] or '')}, {(r['state'] or '')} {(r['zip'] or '')}".strip(),
                axis=1
            )
        else:
            st.error("Cannot construct full_address; missing both full-address and parts."); st.stop()

        # Optional: show detected columns
        with st.expander("Detected columns (debug)"):
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

        out_cols = ["id","company","street","city","state","zip","lon","lat","geocode_success","eligible_rbs"]

        st.subheader("Results")
        c1, c2, c3, c4, c5 = st.columns(5)
        with c1: st.metric("Rows", len(df))
        with c2: st.metric("Geocode OK", int(df["geocode_success"].sum()))
        with c3: st.metric("Eligible", int((df["eligible_rbs"] == True).sum()))
        with c4: st.metric("Ineligible", int((df["eligible_rbs"] == False).sum()))
        with c5: st.metric("No result", int(len(df) - (df["eligible_rbs"] == True).sum() - (df["eligible_rbs"] == False).sum()))

        # NOTE: replace deprecated use_container_width with width='stretch'
        st.dataframe(df[out_cols], width="stretch")

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
    - Geocoding uses Google Maps Geocoding API. Set `GOOGLE_MAPS_API_KEY` in the app’s **Secrets**.
    - `eligible_rbs = True` means the point is **not** inside an RBS ineligible polygon.
    - Works with classic columns, Grata (Headquarters/Mailing Address), and PitchBook (HQ Location or HQ Address Line 1 + City + State + Post Code).
    """)
