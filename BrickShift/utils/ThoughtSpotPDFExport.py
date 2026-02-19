import os
import time
import uuid
import requests
from datetime import datetime
from typing import Optional, Dict, List
from urllib.parse import urlparse, parse_qs

# Configuration constants
CATALOG = "dbx_migration_poc"
SCHEMA = "migrationvalidation"
#EXPORT_PATH = "/Volumes/dbx_migration_poc/migrationvalidation/thoughtspot_exports"
EXPORT_PATH = "/tmp/thoughtspot_exports"

def generate_session_id():
    return f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

def generate_export_id():
    return str(uuid.uuid4())

def parse_thoughtspot_url(url: str) -> Dict[str, Optional[str]]:
    result = {
        'liveboard_id': None,
        'base_url': None
    }
    try:
        parsed = urlparse(url)
        result['base_url'] = f"{parsed.scheme}://{parsed.netloc}"
        path = parsed.fragment if parsed.fragment else parsed.path
        parts = path.split('/')
        
        for i, part in enumerate(parts):
            if part == 'pinboard' and i + 1 < len(parts):
                liveboard_id = parts[i + 1].split('?')[0]
                result['liveboard_id'] = liveboard_id
                break
        
        if not result['liveboard_id'] and parsed.query:
            params = parse_qs(parsed.query)
            if 'id' in params:
                result['liveboard_id'] = params['id'][0]
                
    except Exception as e:
        print(f"Error parsing URL: {e}")
    
    return result

def authenticate_thoughtspot(base_url: str, username: str, password: str) -> Optional[str]:
    try:
        login_url = f"{base_url}/callosum/v1/session/login"
        payload = {
            "username": username,
            "password": password
        }
        response = requests.post(login_url, data=payload)
        
        if response.status_code != 200:
            print(f"Authentication failed: {response.status_code} - {response.text}")
            return None
        
        jsession = response.cookies.get("JSESSIONID")
        if not jsession:
            print("No JSESSIONID in response")
            return None
        
        print("ThoughtSpot authentication successful")
        return jsession
    except Exception as e:
        print(f"Authentication error: {e}")
        return None

def validate_session(base_url: str, jsession: str) -> bool:
    try:    
        headers = {"Cookie": f"JSESSIONID={jsession}"}
        response = requests.get(f"{base_url}/api/rest/2.0/auth/session/user", headers=headers)
        return response.status_code == 200
    except Exception:
        return False

def get_liveboard_details(base_url: str, jsession: str, liveboard_id: str) -> Optional[Dict]:
    try:
        headers = {
            "Content-Type": "application/json",
            "Cookie": f"JSESSIONID={jsession}"
        }
        response = requests.post(
            f"{base_url}/api/rest/2.0/metadata/details",
            json={"metadata": [{"identifier": liveboard_id, "type": "LIVEBOARD"}]},
            headers=headers
        )
        response.raise_for_status()
        details = response.json().get('metadata', [])
        return details[0] if details else None
    except Exception as e:
        print(f"Error getting liveboard details: {e}")
        return None

def export_thoughtspot_liveboard(
    base_url: str,
    liveboard_id: str,
    jsession: str,
    session_id: str,
    liveboard_name: Optional[str] = None,
    export_format: str = "PDF",
    max_retries: int = 3
) -> Dict:
    
    start_time = time.time()
    export_id = generate_export_id()
    timestamp = datetime.now()

    if not liveboard_name:
        details = get_liveboard_details(base_url, jsession, liveboard_id)
        liveboard_name = details.get('name', liveboard_id) if details else liveboard_id
    
    headers = {
        "Content-Type": "application/json",
        "Cookie": f"JSESSIONID={jsession}"
    }
    
    for attempt in range(max_retries):
        try:
            if not validate_session(base_url, jsession):
                raise Exception("Session expired or invalid")

            export_url = f"{base_url}/api/rest/2.0/report/liveboard"
            payload = {
                "metadata_identifier": liveboard_id,
                "file_format": export_format
            }
            
            print(f"Initiating export (attempt {attempt + 1}/{max_retries})...")
            response = requests.post(export_url, json=payload, headers=headers)
            response.raise_for_status()
            
            file_bytes = response.content
            file_size_mb = len(file_bytes) / (1024 * 1024)
            
            ts = timestamp.strftime("%Y%m%d_%H%M%S")
            filename = f"{session_id}_thoughtspot_{liveboard_id}_{ts}.{export_format.lower()}"
            filepath = os.path.join(EXPORT_PATH, filename)
            
            # Ensure output directory exists (Works in volumes)
            os.makedirs(EXPORT_PATH, exist_ok=True)
            
            with open(filepath, "wb") as f:
                f.write(file_bytes)
            
            duration_ms = int((time.time() - start_time) * 1000)
            
            result = {
                "export_id": export_id,
                "session_id": session_id,
                "source": "thoughtspot",
                "dashboard_id": liveboard_id,
                "dashboard_name": liveboard_name,
                "workspace_id": None,
                "report_id": liveboard_id,
                "export_format": export_format,
                "file_path": filepath,
                "filename": filename,
                "file_size_bytes": len(file_bytes),
                "file_size_mb": round(file_size_mb, 2),
                "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"), # Convert to string for JSON serialization
                "export_status": "success",
                "export_duration_ms": duration_ms,
                "error": None
            }
            return result
            
        except Exception as e:
            error_msg = str(e)
            print(f"Attempt {attempt + 1} failed: {error_msg}")
            
            if attempt == max_retries - 1:
                return {
                    "export_id": export_id,
                    "session_id": session_id,
                    "source": "thoughtspot",
                    "dashboard_id": liveboard_id,
                    "dashboard_name": liveboard_name,
                    "workspace_id": None,
                    "report_id": liveboard_id,
                    "export_format": export_format,
                    "file_path": None,
                    "filename": None,
                    "file_size_bytes": None,
                    "file_size_mb": None,
                    "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    "export_status": "failed",
                    "export_duration_ms": int((time.time() - start_time) * 1000),
                    "error": error_msg
                }
            time.sleep(2 ** attempt)

def export_from_url(url: str, username: str, password: str, session_id: Optional[str] = None) -> Dict:
    if not session_id:
        session_id = generate_session_id()

    parsed = parse_thoughtspot_url(url)
    if not parsed['liveboard_id']:
        return {'status': 'failed', 'error': 'Invalid ThoughtSpot URL'}
    
    base_url = parsed['base_url']
    if not base_url:
        return {'status': 'failed', 'error': 'No Base URL found'}

    jsession = authenticate_thoughtspot(base_url, username, password)
    if not jsession:
        return {'status': 'failed', 'error': 'Authentication failed'}
    
    return export_thoughtspot_liveboard(base_url, parsed['liveboard_id'], jsession, session_id)