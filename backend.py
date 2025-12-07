import requests
import datetime
import json
import uuid
import re
import io
import pandas as pd
import pdfplumber

LANGFLOW_BASE_URL = "https://hackathon-langflow-aah4bgc4hrashthc.canadacentral-01.azurewebsites.net"
FILES_ENDPOINT = f"{LANGFLOW_BASE_URL}/api/v2/files"
LANGFLOW_API_KEY = "sk-KAfsyatnVQIV3okvl2-ACFXxvfXW2EpqdXmWJAJn2PA"
headers = {
        "x-api-key": LANGFLOW_API_KEY,
    }
WORKFLOW_ID_INJECTION = "f29e42c4-f045-4eea-bf3c-0073d3bba7fd"
WORKFLOW_ID_EVENTS = "5fe87fa9-87f5-43e1-9822-b0d06951bdef"

ALIASES = {
    "chol": ["Cholesterol (Chol)", "Cholesterol", "Chol"],
    "hdl":  ["HDL Cholesterol", "HDL"],
    "ldl":  ["LDL Cholesterol", "LDL"],
    "triglycerides": ["Triglycerides (TG)", "Triglycerides", "TG"],
}

def extract_kv_from_pdf(pdf_file: bytes | str) -> dict:
    text = ""
    
    # If it's bytes, wrap in BytesIO
    if isinstance(pdf_file, bytes):
        file_obj = io.BytesIO(pdf_file)
    else:
        file_obj = pdf_file

    with pdfplumber.open(file_obj) as pdf:
        for page in pdf.pages:
            text += "\n" + (page.extract_text() or "")

    # βρίσκει γραμμές τύπου: "<test name> <value> <unit> ..."
    # π.χ. "LDL Cholesterol 2.81 mmol/L < 3.36"
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    found = {}
    for line in lines:
        m = re.search(r"^(.*?)(\d+(?:[.,]\d+)?)\s+([A-Za-z/]+)\b", line)
        if not m:
            continue
        test_name = m.group(1).strip()
        value = float(m.group(2).replace(",", "."))
        unit = m.group(3).strip()

        # match σε alias
        for key, names in ALIASES.items():
            if any(test_name.lower().startswith(n.lower()) for n in names):
                found[key] = {"value": value, "unit": unit, "raw": line}
                break

    return found

def pdf_to_df(pdf_file: bytes | str) -> pd.DataFrame:
    # Use the regex extraction logic instead of table extraction
    found_data = extract_kv_from_pdf(pdf_file)
    
    # Create transposed DataFrame: one row, columns are the keys (chol, hdl, etc.)
    row = {k: (found_data.get(k, {}).get("value", None)) for k in ALIASES.keys()}
    df = pd.DataFrame([row])
    
    return df


def upload_pdf(pdf_bytes: bytes, filename: str) -> requests.Response:
    """
    Uploads a PDF file to Langflow API.
    
    Parameters:
    - pdf_bytes: bytes of the PDF file
    - filename: filename of the PDF
    """

    headers = {
        "x-api-key": LANGFLOW_API_KEY,
    }
    
    files = {
        "file": (filename, pdf_bytes, "application/pdf")
    }
    
    resp = requests.post(FILES_ENDPOINT, headers=headers, files=files)
    
    if resp.status_code not in [200, 201]:
        print(f"Error {resp.status_code}: {resp.text}")
        resp.raise_for_status()
    
    # Προαιρετικά: Εξαγωγή σε CSV για debugging ή τοπική χρήση
    try:
        df = pdf_to_df(pdf_bytes)
        df.to_csv(filename.replace(".pdf", ".csv"), index=False)
        print("CSV saved successfully.")
    except Exception as e:
        print(f"Failed to convert PDF to CSV: {e}")

    # Return the JSON response for Langflow workflow
    return resp.json()
    


def upload_json(json_data: bytes | dict, filename: str) -> requests.Response:
    """
    Uploads a JSON file to Langflow API.
    
    Parameters:
    - json_data: bytes of the JSON file or a dict/object to be serialized to JSON
    - filename: filename of the JSON file
    """
    headers = {
        "x-api-key": LANGFLOW_API_KEY,
    }
    
    # If json_data is a dict, convert it to JSON bytes
    if isinstance(json_data, dict):
        json_bytes = json.dumps(json_data).encode('utf-8')
    else:
        json_bytes = json_data
    
    files = {
        "file": (filename, json_bytes, "application/json")
    }
    
    resp = requests.post(FILES_ENDPOINT, headers=headers, files=files)
    
    if resp.status_code not in [200, 201]:
        print(f"Error {resp.status_code}: {resp.text}")
        resp.raise_for_status()
    
    return resp.json()


def start_run_workflow(filename: str, workflow_type: str, upload_response: dict = None) -> dict:
    """
    Starts the run workflow in Langflow.
    
    Parameters:
    - filename: filename of the uploaded file
    - workflow_type: type of the workflow to run ('injection' or 'events')
    - upload_response: optional response from upload_pdf (must contain file path)
    """
    if not upload_response:
        raise ValueError("upload_response is required to get the file path")
    
    # Βρες το file path από το upload response
    # Δοκίμασε διάφορους πιθανούς keys που μπορεί να έχει το response
    file_path = (
        upload_response.get("path")
    )
    
    if not file_path:
        # Αν δεν βρέθηκε, δοκίμασε να δεις αν είναι list/array
        if isinstance(upload_response, list) and len(upload_response) > 0:
            file_path = upload_response[0].get("path") if isinstance(upload_response[0], dict) else None
        
        if not file_path:
            raise ValueError(f"Could not find file path in upload_response: {upload_response}")

    if workflow_type == 'injection':
        workflow_id = WORKFLOW_ID_INJECTION
        payload = {
            "output_type": "text",
            "input_type": "text",
            "input_value": "no input",
            "session_id": str(uuid.uuid4()),
            "tweaks": {
            "File-Kgijo": {
                "path": [
                    file_path
                ]
            }
            }
        }
    elif workflow_type == 'events':
        workflow_id = WORKFLOW_ID_EVENTS
        payload = {
            "output_type": "text",
            "input_type": "text",
            "input_value": "no input",
            "session_id": str(uuid.uuid4()),
            "tweaks": {
            "File-TeORO": {
                "path": [
                    file_path
                ]
            }
            }
        }
    else:
        raise ValueError(f"Invalid workflow id: {workflow_id}")

    request_headers = {
        "x-api-key": LANGFLOW_API_KEY,
        "Content-Type": "application/json"
    }
    
    resp = requests.post(
        f"{LANGFLOW_BASE_URL}/api/v1/run/{workflow_id}", 
        headers=request_headers, 
        json=payload
    )

    resp.raise_for_status()
    return extract_json_from_langflow(resp.json())


def get_events(user_id: int, date: datetime.date) -> dict:
    events = []
    # Calculate date one week later
    week_later = date + datetime.timedelta(days=7)
    
    # Create events for the given date
    for i in range(5):
        events.append({
            "name": f"Event {i}",
            "date": date.strftime("%Y-%m-%d")
        })
    
    # Create events for one week later
    for i in range(5, 10):
        events.append({
            "name": f"Event {i}",
            "date": week_later.strftime("%Y-%m-%d")
        })
    
    return events

def extract_json_from_langflow(response: dict) -> dict:
    try:
        # Try multiple paths to find the text output
        text_block = None
        
        # Path 1: Standard
        try:
            text_block = response["outputs"][0]["outputs"][0]["results"]["text"]["data"]["text"]
        except (KeyError, IndexError, TypeError):
            pass
            
        # Path 2: Message output (seen in logs)
        if not text_block:
            try:
                text_block = response["outputs"][0]["outputs"][0]["outputs"]["text"]["message"]
            except (KeyError, IndexError, TypeError):
                pass

        # Path 3: Artifacts
        if not text_block:
            try:
                text_block = response["outputs"][0]["outputs"][0]["artifacts"]["text"]["raw"]
            except (KeyError, IndexError, TypeError):
                pass
        
        if not text_block:
            print("Could not find text block in response")
            return {}

        # If it's a dict (sometimes happens), get 'text' field
        if isinstance(text_block, dict):
             text_block = text_block.get('text', '')
             
        json_str = re.sub(r"```json|```", "", str(text_block)).strip()
        data = json.loads(json_str)
        print(data)
        return data

    except Exception as e:
        print("Parsing error:", e)
        return {}

