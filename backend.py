import requests
import datetime
import json
import uuid
import re
import io
from typing import Any, Dict, Optional, Union

import pandas as pd
import pdfplumber

# =========================
# CONFIG (HARDCODED)
# =========================
LANGFLOW_BASE_URL = "https://hackathon-langflow-aah4bgc4hrashthc.canadacentral-01.azurewebsites.net"
FILES_ENDPOINT = f"{LANGFLOW_BASE_URL}/api/v2/files"

LANGFLOW_API_KEY = "sk-WGPeqWt71U0ICYZd1L2iz-76K-BrMAeF88X5gKpHKGw"
headers = {"x-api-key": LANGFLOW_API_KEY}

# Τα IDs που έχεις (μπορεί να είναι σωστά ή όχι). Αν παίρνεις HTML, τρέξε list_flows() να πάρεις σωστά.
WORKFLOW_ID_INJECTION = "f29e42c4-f045-4eea-bf3c-0073d3dba7fd"  # αν έχεις άλλο, βάλε το σωστό
WORKFLOW_ID_EVENTS = "954bac75-5c07-46ed-a004-1eee4d97e609"

# Αυτά πρέπει να ταιριάζουν με το node id στο Langflow graph (tweaks keys)
TWEAK_FILE_NODE_INJECTION = "File-Kgijo"
TWEAK_FILE_NODE_EVENTS = "File-TeORO"

DEFAULT_TIMEOUT = 60  # seconds

ALIASES = {
    "Age":  ["Age", "Ηλικία"],
    "BMI":  ["BMI", "Δείκτης μάζας σώματος"],
    "Chol": ["Cholesterol (Chol)", "Cholesterol", "Chol"],
    "HDL":  ["HDL Cholesterol", "HDL"],
    "LDL":  ["LDL Cholesterol", "LDL"],
    "TG":   ["Triglycerides (TG)", "Triglycerides", "TG"],
    "Cr":   ["Creatinine", "Cr"],
    "BUN":  ["Urea", "Blood Urea Nitrogen", "BUN"],
}

# =========================
# PDF -> KV / DF
# =========================

def extract_kv_from_pdf(pdf_file: Union[bytes, str]) -> dict:
    """
    Extract key/value lab tests from PDF text using aliases + regex.
    Returns: { "LDL": {"value": 2.81, "unit": "mmol/L", "raw": "..."} , ... }
    """
    text = ""

    if isinstance(pdf_file, bytes):
        file_obj: Any = io.BytesIO(pdf_file)
    else:
        file_obj = pdf_file  # path/filename

    with pdfplumber.open(file_obj) as pdf:
        for page in pdf.pages:
            text += "\n" + (page.extract_text() or "")

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    found: Dict[str, Dict[str, Any]] = {}
    for line in lines:
        # matches: "<test name> <value> <unit> ..."
        m = re.search(r"^(.*?)(\d+(?:[.,]\d+)?)\s+([A-Za-z/]+)\b", line)
        if not m:
            continue

        test_name = m.group(1).strip()
        value = float(m.group(2).replace(",", "."))
        unit = m.group(3).strip()

        for key, names in ALIASES.items():
            if any(test_name.lower().startswith(n.lower()) for n in names):
                found[key] = {"value": value, "unit": unit, "raw": line}
                break

    return found


def pdf_to_df(pdf_file: Union[bytes, str]) -> pd.DataFrame:
    """
    Create a one-row DataFrame with columns = ALIASES keys and values = extracted numeric values (or None).
    """
    found_data = extract_kv_from_pdf(pdf_file)
    row = {k: (found_data.get(k, {}).get("value", None)) for k in ALIASES.keys()}
    return pd.DataFrame([row])

# =========================
# Uploads
# =========================

def upload_pdf(pdf_bytes: bytes, filename: str) -> dict:
    """
    Upload a PDF to Langflow Files API.
    Returns the JSON response (dict) which should contain a "path".
    """
    files = {"file": (filename, pdf_bytes, "application/pdf")}

    resp = requests.post(FILES_ENDPOINT, headers=headers, files=files, timeout=DEFAULT_TIMEOUT)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Upload PDF failed ({resp.status_code}): {resp.text[:500]}")

    try:
        out = resp.json()
    except Exception:
        raise RuntimeError(
            f"Upload PDF returned non-JSON response. content-type={resp.headers.get('content-type')} "
            f"head={resp.text[:500]}"
        )

    # optional debug: save extracted CSV locally
    try:
        df = pdf_to_df(pdf_bytes)
        csv_name = filename.replace(".pdf", ".csv")
        df.to_csv(csv_name, index=False)
        print(f"[debug] CSV saved: {csv_name}")
    except Exception as e:
        print(f"[debug] Failed to convert PDF to CSV: {e}")

    return out


def upload_json(json_data: Union[bytes, dict], filename: str) -> dict:
    """
    Upload a JSON file to Langflow Files API.
    Returns the JSON response (dict) which should contain a "path".
    """
    if isinstance(json_data, dict):
        json_bytes = json.dumps(json_data, ensure_ascii=False).encode("utf-8")
    else:
        json_bytes = json_data

    files = {"file": (filename, json_bytes, "application/json")}

    resp = requests.post(FILES_ENDPOINT, headers=headers, files=files, timeout=DEFAULT_TIMEOUT)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Upload JSON failed ({resp.status_code}): {resp.text[:500]}")

    try:
        return resp.json()
    except Exception:
        raise RuntimeError(
            f"Upload JSON returned non-JSON response. content-type={resp.headers.get('content-type')} "
            f"head={resp.text[:500]}"
        )

# =========================
# Langflow Run + Robust JSON extraction
# =========================

def _get_file_path(upload_response: Any) -> str:
    """
    Pull the 'path' from Langflow files upload response.
    Supports dict or list[dict].
    """
    if isinstance(upload_response, dict):
        fp = upload_response.get("path")
        if fp:
            return fp

    if isinstance(upload_response, list) and upload_response:
        first = upload_response[0]
        if isinstance(first, dict) and first.get("path"):
            return first["path"]

    raise ValueError(f"Could not find file path in upload_response: {str(upload_response)[:500]}")


def _safe_json_from_text(text: str) -> Optional[Union[dict, list]]:
    """
    Try to parse JSON from:
    - raw string
    - fenced ```json ... ```
    - first {...} or [...] block inside text
    """
    if not text or not str(text).strip():
        return None

    s = str(text).strip()
    s = re.sub(r"```(?:json)?", "", s, flags=re.IGNORECASE).replace("```", "").strip()

    try:
        return json.loads(s)
    except Exception:
        pass

    m = re.search(r"(\{.*\}|\[.*\])", s, flags=re.DOTALL)
    if not m:
        return None

    candidate = m.group(1).strip()
    try:
        return json.loads(candidate)
    except Exception:
        return None


def extract_json_from_langflow(response: dict) -> dict:
    """
    Locate output text inside Langflow run response and extract JSON from it.
    Returns {} if not found/parseable.
    """
    text_block = None

    candidate_paths = [
        lambda r: r["outputs"][0]["outputs"][0]["results"]["text"]["data"]["text"],
        lambda r: r["outputs"][0]["outputs"][0]["outputs"]["text"]["message"],
        lambda r: r["outputs"][0]["outputs"][0]["artifacts"]["text"]["raw"],
    ]

    for getter in candidate_paths:
        try:
            text_block = getter(response)
            if text_block:
                break
        except Exception:
            continue

    if text_block is None:
        return {}

    if isinstance(text_block, dict):
        text_block = text_block.get("text", "")

    parsed = _safe_json_from_text(str(text_block))
    if isinstance(parsed, dict):
        return parsed
    if isinstance(parsed, list):
        return {"items": parsed}

    return {}

# =========================
# Helper: list flows to confirm correct ids
# =========================

def list_flows() -> Any:
    """
    Fetch flows list from Langflow.
    Useful when your run endpoint returns HTML and you suspect wrong flow_id/base route.
    """
    base = LANGFLOW_BASE_URL.rstrip("/")
    api_roots = [base, base + "/api"] if not base.endswith("/api") else [base, base[:-4]]

    candidates = []
    for api_root in api_roots:
        if api_root.endswith("/api"):
            candidates.append(f"{api_root}/v1/flows")
        else:
            candidates.append(f"{api_root}/api/v1/flows")

    last_err = None
    for url in candidates:
        try:
            r = requests.get(
                url,
                headers={"x-api-key": LANGFLOW_API_KEY, "accept": "application/json"},
                timeout=DEFAULT_TIMEOUT,
                allow_redirects=False,
            )
            ct = (r.headers.get("content-type") or "").lower()
            if "application/json" in ct:
                return r.json()
            last_err = f"{url} -> {r.status_code} {ct} head={r.text[:200]}"
        except Exception as e:
            last_err = f"{url} failed: {e}"

    raise RuntimeError(f"Could not list flows. Last error: {last_err}")

# =========================
# Run flow (tries multiple endpoint layouts)
# =========================

def start_run_workflow(filename: str, workflow_type: str, upload_response: Optional[dict] = None) -> dict:
    if not upload_response:
        raise ValueError("upload_response is required to get the file path")

    file_path = _get_file_path(upload_response)

    if workflow_type == "injection":
        workflow_id = WORKFLOW_ID_INJECTION
        file_node = TWEAK_FILE_NODE_INJECTION
    elif workflow_type == "events":
        workflow_id = WORKFLOW_ID_EVENTS
        file_node = TWEAK_FILE_NODE_EVENTS
    else:
        raise ValueError(f"Invalid workflow_type: {workflow_type}")

    payload = {
        "input_value": "no input",
        "input_type": "text",
        "output_type": "text",
        "session_id": str(uuid.uuid4()),
        "tweaks": {file_node: {"path": [file_path]}},
    }

    base = LANGFLOW_BASE_URL.rstrip("/")
    api_root_candidates = [base, base + "/api"] if not base.endswith("/api") else [base, base[:-4]]

    endpoint_candidates = []
    for api_root in api_root_candidates:
        if api_root.endswith("/api"):
            endpoint_candidates.append(f"{api_root}/v1/run/{workflow_id}?stream=false")
            endpoint_candidates.append(f"{api_root}/v1/run/{workflow_id}/?stream=false")
        else:
            endpoint_candidates.append(f"{api_root}/api/v1/run/{workflow_id}?stream=false")
            endpoint_candidates.append(f"{api_root}/api/v1/run/{workflow_id}/?stream=false")

    last_info = None

    for url in endpoint_candidates:
        resp = requests.post(
            url,
            headers={
                "x-api-key": LANGFLOW_API_KEY,
                "Content-Type": "application/json",
                "accept": "application/json",
            },
            json=payload,
            timeout=DEFAULT_TIMEOUT,
            allow_redirects=False,
        )

        ct = (resp.headers.get("content-type") or "").lower()
        head = (resp.text or "")[:700]
        last_info = f"url={url} status={resp.status_code} ct={ct} head={head}"

        # Αν είναι JSON, ok
        if "application/json" in ct:
            try:
                body = resp.json()
            except Exception as e:
                raise RuntimeError(f"Run returned application/json but json() failed: {e}. head={head}")
            return extract_json_from_langflow(body)

        # HTML = UI index / redirect fallback -> συνέχισε να δοκιμάζει άλλο endpoint
        if "text/html" in ct:
            continue

        # κάτι άλλο -> συνέχισε
        continue

    raise RuntimeError(
        "Langflow returned NON-JSON response for all run endpoint candidates.\n"
        f"Last: {last_info}\n\n"
        "Αυτό συνήθως σημαίνει:\n"
        "1) λάθος flow id (WORKFLOW_ID_*) ή\n"
        "2) το API δεν είναι exposed στο συγκεκριμένο domain/path.\n\n"
        "Δοκίμασε να τρέξεις:\n"
        "  from backend import list_flows\n"
        "  flows = list_flows(); print(flows)\n"
        "και πάρε το σωστό flow id."
    )

# =========================
# Optional local stub: events generator
# =========================

def get_events(user_id: int, date: datetime.date) -> dict:
    events = []
    week_later = date + datetime.timedelta(days=7)

    for i in range(5):
        events.append({"name": f"Event {i}", "date": date.strftime("%Y-%m-%d")})
    for i in range(5, 10):
        events.append({"name": f"Event {i}", "date": week_later.strftime("%Y-%m-%d")})

    return {"recommended_events": events}
