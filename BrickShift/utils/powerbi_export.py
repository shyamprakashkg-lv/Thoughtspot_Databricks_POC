import os
import time
import uuid
import requests
import msal
from datetime import datetime
from typing import Optional, Dict, List
try:
    from pyspark.sql import SparkSession, Row
    from pyspark.sql.types import *
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    print("Spark not detected - Skipping Metadata Logging to Delta.")

CATALOG = "dbx_migration_poc"
SCHEMA = "migrationvalidation"
METADATA_TABLE = f"{CATALOG}.{SCHEMA}.powerbi_exports_metadata"

if os.path.exists("/Volumes/"):
    EXPORT_PATH = "/Volumes/dbx_migration_poc/migrationvalidation/powerbi_exports"
else:
    EXPORT_PATH = os.path.join(os.getcwd(), "exports")

os.makedirs(EXPORT_PATH, exist_ok=True)

def get_metadata_schema():
    if not SPARK_AVAILABLE: return None
    return StructType([
        StructField("export_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("source", StringType(), True),
        StructField("dashboard_id", StringType(), True),
        StructField("dashboard_name", StringType(), True),
        StructField("workspace_id", StringType(), True),
        StructField("report_id", StringType(), True),
        StructField("export_format", StringType(), True),
        StructField("file_path", StringType(), True),
        StructField("filename", StringType(), True),
        StructField("file_size_bytes", LongType(), True),
        StructField("file_size_mb", DoubleType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("export_status", StringType(), True),
        StructField("export_duration_ms", IntegerType(), True),
        StructField("error", StringType(), True)
    ])

def save_to_delta(result_dict: Dict, table_name: str = METADATA_TABLE):
    if not SPARK_AVAILABLE:
        print(f"[WARN] Spark unavailable. Skipping Delta log for: {result_dict.get('filename')}")
        return
    try:
        spark = SparkSession.builder.getOrCreate()
        schema = get_metadata_schema()
        df = spark.createDataFrame([Row(**result_dict)], schema=schema)
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_name)
        print(f"Metadata logged to {table_name}")
    except Exception as e:
        print(f"[ERROR] Failed to save to Delta: {e}")

def generate_session_id():
    return f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

def generate_export_id():
    return str(uuid.uuid4())

def parse_powerbi_url(url: str) -> Dict[str, Optional[str]]:
    parts = url.rstrip('/').split('/')
    result = {'workspace_id': None, 'report_id': None, 'dashboard_id': None}
    try:
        for i, part in enumerate(parts):
            if part == 'groups' and i + 1 < len(parts):
                result['workspace_id'] = parts[i + 1]
            if part == 'reports' and i + 1 < len(parts):
                result['report_id'] = parts[i + 1].split('?')[0]
            if part == 'dashboards' and i + 1 < len(parts):
                result['dashboard_id'] = parts[i + 1].split('?')[0]
    except Exception as e:
        print(f"Error parsing URL: {e}")
    return result

def authenticate_powerbi(tenant_id: str, client_id: str, client_secret: str) -> Optional[str]:
    try:
        authority = f"https://login.microsoftonline.com/{tenant_id}"
        app = msal.ConfidentialClientApplication(
            client_id=client_id, authority=authority, client_credential=client_secret
        )
        result = app.acquire_token_for_client(scopes=["https://analysis.windows.net/powerbi/api/.default"])
        if "access_token" not in result:
            print(f"Authentication failed: {result.get('error_description')}")
            return None
        return result['access_token']
    except Exception as e:
        print(f"Authentication error: {e}")
        return None

def get_report_details(token: str, workspace_id: str, report_id: str) -> Optional[Dict]:
    try:
        headers = {'Authorization': f'Bearer {token}'}
        response = requests.get(
            f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports/{report_id}",
            headers=headers
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error getting report details: {e}")
        return None

def export_powerbi_dashboard(workspace_id, report_id, token, session_id, dashboard_name=None, export_format="PDF", max_retries=3, poll_interval=5):
    start_time = time.time()
    export_id = generate_export_id()
    timestamp = datetime.now()
    
    if not dashboard_name:
        report_details = get_report_details(token, workspace_id, report_id)
        dashboard_name = report_details.get('name', report_id) if report_details else report_id
    
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    for attempt in range(max_retries):
        try:
            pages_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports/{report_id}/pages"
            pages_resp = requests.get(pages_url, headers=headers)
            pages_resp.raise_for_status()
            all_pages = pages_resp.json().get('value', [])
            visible_pages = [{"pageName": p['name']} for p in all_pages if p.get('visibility') == 0 or p.get('visibility') is None]

            export_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports/{report_id}/ExportTo"
            payload = {
                "format": export_format,
                "powerBIReportConfiguration": {
                    "settings": {"includeHiddenPages": False},
                    "reportConfiguration": {"pages": visible_pages}
                }
            }
            resp = requests.post(export_url, json=payload, headers=headers)
            resp.raise_for_status()
            export_job_id = resp.json()["id"]
            
            status_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports/{report_id}/exports/{export_job_id}"
            file_url = None
            while True:
                status_resp = requests.get(status_url, headers=headers)
                status_data = status_resp.json()
                status = status_data["status"]
                print(f"Export Status: {status}")
                if status == "Succeeded":
                    file_url = status_data["resourceLocation"]
                    break
                elif status == "Failed":
                    raise Exception(f"Export failed: {status_data.get('error', {}).get('message')}")
                time.sleep(poll_interval)
            
            file_resp = requests.get(file_url, headers=headers)
            file_bytes = file_resp.content
            
            ts = timestamp.strftime("%Y%m%d_%H%M%S")
            filename = f"{session_id}_full_report_{ts}.{export_format.lower()}"
            filepath = os.path.join(EXPORT_PATH, filename)
            
            with open(filepath, "wb") as f:
                f.write(file_bytes)
                
            result = {
                "export_id": export_id, "session_id": session_id, "source": "powerbi",
                "dashboard_id": report_id, "dashboard_name": dashboard_name,
                "workspace_id": workspace_id, "report_id": report_id,
                "export_format": export_format, "file_path": filepath,
                "filename": filename, "file_size_bytes": len(file_bytes),
                "file_size_mb": round(len(file_bytes) / (1024 * 1024), 2),
                "timestamp": timestamp, "export_status": "success",
                "export_duration_ms": int((time.time() - start_time) * 1000), "error": None
            }
            save_to_delta(result)
            return result
            
        except Exception as e:
            print(f"Attempt {attempt+1} failed: {e}")
            if attempt == max_retries - 1:
                err_res = {"export_status": "failed", "error": str(e), "timestamp": timestamp}
                return err_res
            time.sleep(2 ** attempt)

def export_from_url(url, tenant_id, client_id, client_secret, session_id=None):
    if not session_id: session_id = generate_session_id()
    parsed = parse_powerbi_url(url)
    if not parsed['workspace_id']: return {'export_status': 'failed', 'error': 'Invalid URL format'}
    
    token = authenticate_powerbi(tenant_id, client_id, client_secret)
    if not token: return {'export_status': 'failed', 'error': 'Auth failed'}
    
    return export_powerbi_dashboard(parsed['workspace_id'], parsed['report_id'], token, session_id)