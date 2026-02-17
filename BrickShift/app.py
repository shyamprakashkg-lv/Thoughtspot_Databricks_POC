import streamlit as st
import requests
import time
import json
from datetime import datetime
import os
import pandas as pd
from fpdf import FPDF
import uuid
import re
import sys

# Import the utils from the prototype
sys.path.append(os.path.join(os.path.dirname(__file__), '..')) # Adjust based on folder structure
try:
    from utils import powerbi_export, vision_engine
except ImportError:
    pass # Handle gracefully if files missing

# Page configuration
st.set_page_config(
    page_title="BrickShift - Dashboard Converter",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    /* Professional gradient background - Deep Enterprise Blue */
    .main {
        background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
        padding: 0;
        margin: 0;
    }
    
    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background: #0f172a;
        padding: 1rem;
        border-right: 1px solid #334155;
    }
    
    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] h1,
    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] h2,
    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] h3 {
        color: #f8fafc;
    }
    
    /* Typography */
    h1 {
        color: #f8fafc !important;
        margin: 0 !important;
        padding: 0.5rem 0 !important;
        font-family: 'Inter', sans-serif;
    }
    
    h2, h3 {
        color: #f8fafc !important;
        font-family: 'Inter', sans-serif;
    }
    
    p, li {
        color: #cbd5e1 !important;
    }
    
    /* Panel Selector Buttons */
    .panel-selector-container {
        display: flex;
        gap: 1rem;
        justify-content: center;
        margin: 2rem auto;
        max-width: 900px;
    }
    
    /* Primary button styling */
    .stButton > button[kind="primary"] {
        background: #2563eb;
        color: white;
        border: none;
        padding: 0.75rem 2rem;
        font-size: 1rem;
        font-weight: 600;
        border-radius: 6px;
        width: 100%;
        min-width: 200px;
        transition: all 0.3s ease;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
    }
    
    .stButton > button[kind="primary"]:hover {
        background: #1d4ed8;
        box-shadow: 0 6px 16px rgba(0, 0, 0, 0.4);
        transform: translateY(-2px);
    }
    
    .stButton > button[kind="primary"]:active {
        transform: translateY(0px);
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.3);
    }
    
    /* Secondary button styling */
    .stButton > button[kind="secondary"] {
        background: #1e293b;
        color: #cbd5e1;
        border: 1px solid #475569;
        padding: 0.75rem 1.5rem;
        font-size: 0.95rem;
        font-weight: 500;
        border-radius: 6px;
        transition: all 0.3s ease;
        height: 60px;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.2);
    }
    
    .stButton > button[kind="secondary"]:hover {
        background: #334155;
        border-color: #64748b;
        color: #f8fafc;
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
    }
    
    .stButton > button[kind="secondary"]:active {
        transform: translateY(0px);
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.2);
    }
    
    .stButton > button[kind="secondary"]:disabled {
        background: #1e293b;
        color: #64748b;
        border-color: #334155;
        opacity: 0.5;
        cursor: not-allowed;
        box-shadow: none;
    }
    
    .stButton > button[kind="secondary"]:disabled:hover {
        transform: none;
        box-shadow: none;
    }
    
    /* Active panel button state - Subtle glow with background color */
    div[data-testid="column"]:has(button[kind="primary"]) button[kind="primary"] {
        background: #1e293b !important;
        color: #f8fafc !important;
        border: 1px solid #64748b !important;
        box-shadow: 
            0 0 20px rgba(148, 163, 184, 0.4),
            0 0 40px rgba(148, 163, 184, 0.2),
            0 4px 12px rgba(0, 0, 0, 0.3) !important;
        animation: subtle-pulse 3s ease-in-out infinite;
    }
    
    @keyframes subtle-pulse {
        0%, 100% {
            box-shadow: 
                0 0 20px rgba(148, 163, 184, 0.4),
                0 0 40px rgba(148, 163, 184, 0.2),
                0 4px 12px rgba(0, 0, 0, 0.3);
        }
        50% {
            box-shadow: 
                0 0 30px rgba(148, 163, 184, 0.5),
                0 0 60px rgba(148, 163, 184, 0.3),
                0 6px 16px rgba(0, 0, 0, 0.4);
        }
    }

    /* Top right test connection button */
    .top-right-button {
        position: fixed;
        top: 1rem;
        right: 1rem;
        z-index: 1000;
    }

    
    /* Metric cards */
    .metric-card {
        background: #1e293b;
        border: 1px solid #334155;
        border-radius: 8px;
        padding: 1.25rem;
        text-align: center;
        margin: 0.5rem 0;
    }
    
    .metric-label {
        font-size: 0.75rem;
        color: #94a3b8;
        margin-bottom: 0.5rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        font-weight: 600;
    }
    
    .metric-value {
        font-size: 1.5rem;
        font-weight: 700;
        color: #f8fafc;
        font-family: 'Roboto Mono', monospace;
    }
    
    .status-success { color: #10b981; }
    .status-error { color: #ef4444; }
    .status-running { color: #3b82f6; }
    
    /* Message boxes */
    .message-box {
        padding: 1rem 1.25rem;
        border-radius: 6px;
        margin: 1rem 0;
        border-left: 4px solid;
        background: #1e293b;
        color: #f8fafc;
        font-size: 0.95rem;
    }
    
    .message-success { border-color: #10b981; }
    .message-error { border-color: #ef4444; }
    .message-info { border-color: #3b82f6; }
    
    /* Dashboard output card */
    .dashboard-card {
        background: #1e293b;
        border: 1px solid #334155;
        border-radius: 8px;
        padding: 1.5rem;
        color: #f8fafc;
        margin: 1rem 0;
    }
    
    .dashboard-title {
        font-size: 1.25rem;
        font-weight: 600;
        margin-bottom: 1.5rem;
        text-align: center;
        color: #f8fafc;
    }
    
    .dashboard-item {
        background: #0f172a;
        border: 1px solid #334155;
        border-radius: 6px;
        padding: 1rem;
        text-align: center;
        margin: 0.5rem;
    }
    
    .dashboard-label {
        font-size: 0.7rem;
        color: #94a3b8;
        margin-bottom: 0.5rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        font-weight: 600;
    }
    
    .dashboard-value {
        font-size: 0.95rem;
        font-weight: 500;
        word-break: break-all;
        color: #f8fafc;
    }
    
    .dashboard-link {
        display: inline-block;
        background: #334155;
        color: #f8fafc;
        text-decoration: none;
        padding: 0.5rem 1.5rem;
        border-radius: 4px;
        font-size: 0.9rem;
        font-weight: 500;
        transition: all 0.2s;
    }
    
    .dashboard-link:hover {
        background: #475569;
    }
    
    /* Tabs styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 1.5rem;
        background: transparent;
        border-bottom: 1px solid #334155;
        padding: 0.5rem 0 0 0;
    }
    
    .stTabs [data-baseweb="tab"] {
        padding: 0.75rem 0;
        font-weight: 500;
        color: #94a3b8;
        font-size: 1rem;
    }
    
    .stTabs [aria-selected="true"] {
        color: #3b82f6;
        border-bottom: 2px solid #3b82f6;
    }
    
    /* Expander */
    .stExpander {
        background: #1e293b;
        border: 1px solid #334155;
        border-radius: 6px;
    }
    
    .streamlit-expanderHeader {
        background: transparent;
        color: #f8fafc !important;
        font-size: 0.95rem;
    }
    
    /* Code/File content */
    .file-content {
        background: #0f172a;
        border: 1px solid #334155;
        border-radius: 6px;
        padding: 1rem;
        color: #e2e8f0;
        font-family: 'Roboto Mono', monospace;
        font-size: 0.85rem;
    }
    
    /* Global Cleanups */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    .block-container { 
        padding-top: 0rem !important;
        padding-bottom: 0rem !important;
    }
    
    /* Remove top margin from first element */
    .block-container > div:first-child {
        margin-top: 0 !important;
        padding-top: 0 !important;
    }
    
    /* Hide sidebar toggle button */
    [data-testid="collapsedControl"] {
        display: none !important;
    }
    
    button[kind="header"] {
        display: none !important;
    }
    
    section[data-testid="stSidebar"] > div > button {
        display: none !important;
    }
    
    [data-testid="stSidebarCollapsedControl"] {
        display: none !important;
    }
    
    /* Text Input */
    .stTextInput > div > div > input {
        background: #1e293b;
        color: #f8fafc;
        border: 1px solid #475569;
        border-radius: 6px;
    }
    
    .stTextInput > div > div > input:focus {
        border-color: #3b82f6;
        box-shadow: 0 0 0 1px #3b82f6;
    }
    
    /* Coming Soon Screen */
    .coming-soon-screen {
        text-align: center;
        padding: 4rem 2rem;
        max-width: 600px;
        margin: 2rem auto;
        background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
        border: 2px solid #334155;
        border-radius: 16px;
    }
    
    .coming-soon-title {
        font-size: 2rem;
        font-weight: 700;
        color: #f8fafc;
        margin-bottom: 1rem;
    }
    
    .coming-soon-text {
        font-size: 1.1rem;
        color: #94a3b8;
        line-height: 1.6;
    }
    
    .coming-soon-badge {
        display: inline-block;
        background: linear-gradient(135deg, #f59e0b, #d97706);
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 6px;
        font-size: 0.85rem;
        font-weight: 600;
        margin-top: 1rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }

    /* ------------------------------------------------ */
    /* FIXED: Sidebar Footer Container                  */
    /* ------------------------------------------------ */
    .genie-widget-container {
        position: fixed;
        bottom: 0;
        left: 0;
        width: 21rem;
        padding: 1rem;
        z-index: 99999;
        background-color: #0f172a;
        border-top: 1px solid #334155;
        display: flex;       /* Use Flexbox */
        flex-direction: column; /* Stack buttons vertically */
        gap: 0.5rem;         /* Gap between buttons */
    }

    /* Force buttons to full width */
    .genie-widget-container .stButton {
        width: 100% !important;
        margin: 0 !important;
    }

    .genie-widget-container .stButton > button {
        width: 100% !important;
        border-radius: 6px;
        display: flex;
        justify-content: center;
        align-items: center;
        
        /* Your Genie styling */
        background: linear-gradient(to right, #1e293b, #0f172a);
        border: 1px solid #60a5fa; 
        color: #e2e8f0;
        border-radius: 4px; /* Matches standard Streamlit button radius */
        
        display: flex;
        align-items: center;
        justify-content: center;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
        transition: all 0.2s ease;
    }

    /* Hover effect */
    .genie-widget-container .stButton > button:hover {
        border-color: #93c5fd;
        background: linear-gradient(to right, #334155, #1e293b);
        transform: translateY(-1px);
    }
    
    /* ------------------------------------------------ */
    /* NEW: External Icon for Genie Button              */
    /* ------------------------------------------------ */
    
    /* 1. Target the button inside the genie widget container */
    .genie-widget-container .stButton > button {
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
        gap: 12px !important; /* Increased gap for better spacing */
    }

    /* 2. Inject the icon using a pseudo-element */
    .genie-widget-container .stButton > button::before {
        content: "";
        display: inline-block;
        width: 1.2rem;
        height: 1.2rem;
        
        /* FORCE WHITE COLOR explicitly instead of currentColor */
        background-color: #f8fafc !important; 
        
        /* WEB & STANDARD MASKS */
        -webkit-mask: url('https://cdn.jsdelivr.net/npm/heroicons@2.0.18/24/outline/sparkles.svg') no-repeat center;
        mask: url('https://cdn.jsdelivr.net/npm/heroicons@2.0.18/24/outline/sparkles.svg') no-repeat center;
        
        -webkit-mask-size: contain;
        mask-size: contain;
        
        /* Ensure it renders on top */
        z-index: 1;
    }
</style>
""", unsafe_allow_html=True)

def load_config():
    config_path = "config.json"
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            st.error(f"Configuration error: {str(e)}")
            return {}
    return {}

def normalize_host_url(host):
    if not host:
        return host
    host = host.strip()
    if not host.startswith(('http://', 'https://')):
        host = f"https://{host}"
    return host

def query_genie_api(host, token, space_id, content, conversation_id=None):
    """
    Interacts with Databricks Genie Space API (Corrected Endpoints).
    1. Starts conversation via 'start-conversation' endpoint.
    2. Or sends message to existing conversation via 'messages' endpoint.
    3. Polls for completion and extracts attachments (Text/SQL).
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    try:
        # ------------------------------------------------------------------
        # SCENARIO 1: START A NEW CONVERSATION
        # Endpoint: POST .../spaces/{space_id}/start-conversation
        # ------------------------------------------------------------------
        if not conversation_id:
            start_url = f"{host}/api/2.0/genie/spaces/{space_id}/start-conversation"
            payload = {"content": content}
            
            resp = requests.post(start_url, headers=headers, json=payload)
            if not resp.ok:
                return None, f"Failed to start conversation (Status {resp.status_code}): {resp.text}"
            
            data = resp.json()
            conversation_id = data.get("conversation_id")
            # The start response contains the first message object
            message_obj = data.get("message", {})
            message_id = message_obj.get("id") or message_obj.get("message_id")

        # ------------------------------------------------------------------
        # SCENARIO 2: CONTINUE EXISTING CONVERSATION
        # Endpoint: POST .../spaces/{space_id}/conversations/{conv_id}/messages
        # ------------------------------------------------------------------
        else:
            msg_url = f"{host}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages"
            payload = {"content": content}
            
            resp = requests.post(msg_url, headers=headers, json=payload)
            if not resp.ok:
                 return conversation_id, f"Error sending message: {resp.text}"
            
            data = resp.json()
            # The response is the message object directly
            message_id = data.get("id") or data.get("message_id")

        # ------------------------------------------------------------------
        # STEP 3: POLL FOR COMPLETION
        # Endpoint: GET .../spaces/{space_id}/conversations/{conv_id}/messages/{msg_id}
        # ------------------------------------------------------------------
        if not message_id:
            return conversation_id, "Error: No message ID returned to track response."

        max_retries = 30 # Wait up to 60 seconds
        final_text = ""
        
        with st.spinner("Genie is thinking..."):
            for _ in range(max_retries):
                poll_url = f"{host}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}"
                poll_resp = requests.get(poll_url, headers=headers)
                
                if poll_resp.ok:
                    poll_data = poll_resp.json()
                    status = poll_data.get("status", "COMPLETED")
                    
                    if status == "COMPLETED":
                        final_text = _parse_genie_attachments(poll_data)
                        break
                    elif status in ["FAILED", "CANCELLED"]:
                        final_text = f"Genie stopped with status: {status}"
                        break
                
                time.sleep(2)
        
        if not final_text:
            final_text = "No response content received (Timed out)."

        return conversation_id, final_text

    except Exception as e:
        return conversation_id, f"API Exception: {str(e)}"

def _parse_genie_attachments(data):
    """Helper to extract AI answers from Genie attachments"""
    text_accum = ""
    attachments = data.get("attachments", [])
    
    if not attachments:
        return "Command completed (No output details provided)."

    for attachment in attachments:
        # Check for Text attachment
        if "text" in attachment:
            text_content = attachment["text"].get("content", "")
            if text_content:
                text_accum += text_content + "\n\n"
        
        # Check for SQL Query attachment
        if "query" in attachment:
            query_sql = attachment["query"].get("query", "")
            if query_sql:
                text_accum += f"```sql\n{query_sql}\n```\n\n"
                
        # Handle generic/older schema if needed
        if "content" in attachment and "text" not in attachment:
             text_accum += str(attachment["content"]) + "\n"

    return text_accum.strip()

def execute_sql_statement(host, token, warehouse_id, sql_statement, timeout=30):
    """Execute a SQL statement and return results"""
    try:
        url = f"{host}/api/2.0/sql/statements"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        payload = {
            "warehouse_id": warehouse_id,
            "statement": sql_statement,
            "wait_timeout": f"{timeout}s"
        }
        
        response = requests.post(url, headers=headers, json=payload, timeout=timeout+5)
        
        if not response.ok:
            error_text = response.text
            try:
                error_json = response.json()
                error_message = error_json.get('message', error_text)
            except:
                error_message = error_text
            return False, f"API Error {response.status_code}: {error_message}", None
        
        result = response.json()
        status = result.get("status", {}).get("state", "UNKNOWN")
        
        if status == "FAILED":
            error_message = result.get("status", {}).get("error", {})
            error_msg = error_message.get("message", "Unknown error")
            return False, f"Query failed: {error_msg}", None
        
        if status != "SUCCEEDED":
            return False, f"Query did not succeed: {status}", None
        
        return True, "Success", result
            
    except requests.Timeout:
        return False, f"Request timed out after {timeout} seconds", None
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        return False, f"Exception: {str(e)}\n{error_details}", None
    
def get_table_data_simple(host, token, warehouse_id, table_name):
    """Clean table data fetching - used for Discovery tabs"""
    try:
        url = f"{host}/api/2.0/sql/statements"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        payload = {
            "warehouse_id": warehouse_id,
            "statement": f"SELECT * FROM {table_name} LIMIT 100",
            "wait_timeout": "30s"
        }
        
        response = requests.post(url, headers=headers, json=payload, timeout=35)
        
        if not response.ok:
            st.error(f"API Error {response.status_code}")
            return pd.DataFrame()
        
        result = response.json()
        status = result.get("status", {}).get("state", "UNKNOWN")
        
        if status != "SUCCEEDED":
            st.error(f"Query did not succeed: {status}")
            return pd.DataFrame()
        
        manifest = result.get("manifest", {})
        schema = manifest.get("schema", {})
        columns = [col.get("name", f"col_{i}") for i, col in enumerate(schema.get("columns", []))]
        
        result_obj = result.get("result", {})
        data_array = result_obj.get("data_array", [])
        
        if data_array and columns:
            return pd.DataFrame(data_array, columns=columns)
        elif data_array:
            return pd.DataFrame(data_array)
        else:
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Exception: {str(e)}")
        return pd.DataFrame()

def get_conversion_tracker_data(host, token, warehouse_id, run_id):
    """Get data from pbi_conversion_tracker for a specific run_id"""
    try:
        # Assuming run_id is stored/logged in the table, adjust the WHERE clause as needed for your table schema
        # If the table doesn't have a run_id column, remove the WHERE clause
        query = f"SELECT * FROM dbx_migration_poc.dbx_migration_pbi.pbi_conversion_tracker WHERE run_id = '{run_id}'"
        
        success, message, result = execute_sql_statement(host, token, warehouse_id, query)
        
        if not success:
            # Fallback to get all data if filtering fails or just strictly last 50
            query_fallback = "SELECT * FROM dbx_migration_poc.dbx_migration_pbi.pbi_conversion_tracker ORDER BY conversion_timestamp DESC LIMIT 50"
            success, message, result = execute_sql_statement(host, token, warehouse_id, query_fallback)
            if not success:
                 return pd.DataFrame()

        manifest = result.get("manifest", {})
        schema = manifest.get("schema", {})
        columns = [col.get("name", f"col_{i}") for i, col in enumerate(schema.get("columns", []))]
        
        result_obj = result.get("result", {})
        data_array = result_obj.get("data_array", [])
        
        if data_array and columns:
            return pd.DataFrame(data_array, columns=columns)
        else:
            return pd.DataFrame()

    except Exception as e:
        return pd.DataFrame()

def get_table_data_for_config(host, token, warehouse_id, table_name):
    """Get table data for config manager - higher limit"""
    try:
        success, message, result = execute_sql_statement(
            host, token, warehouse_id, 
            f"SELECT * FROM {table_name} LIMIT 10000"
        )
        
        if not success:
            st.error(message)
            return pd.DataFrame()
        
        manifest = result.get("manifest", {})
        schema = manifest.get("schema", {})
        columns = [col.get("name", f"col_{i}") for i, col in enumerate(schema.get("columns", []))]
        
        result_obj = result.get("result", {})
        data_array = result_obj.get("data_array", [])
        
        if data_array and columns:
            return pd.DataFrame(data_array, columns=columns)
        elif data_array:
            return pd.DataFrame(data_array)
        else:
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Exception: {str(e)}")
        return pd.DataFrame()
    
def write_table_data_sql(host, token, warehouse_id, table_name, df):
    """Write DataFrame back to Unity Catalog table using SQL API - Simplified approach"""
    try:
        if df.empty:
            return False, "Cannot write empty DataFrame"
        
        st.info(f"Starting write operation for {len(df)} rows...")
        
        # Step 1: Create a backup by selecting into a temp location first (optional safety)
        # Step 2: Truncate the table
        truncate_sql = f"DELETE FROM {table_name}"  # Using DELETE instead of TRUNCATE
        st.info(f"Deleting existing rows from {table_name}...")
        
        success, message, _ = execute_sql_statement(host, token, warehouse_id, truncate_sql, timeout=40)
        
        if not success:
            return False, f"Failed to delete from table: {message}"
        
        st.success("Existing rows deleted successfully")
        
        # Step 3: Get the actual column types from the table
        describe_sql = f"DESCRIBE TABLE {table_name}"
        success, message, result = execute_sql_statement(host, token, warehouse_id, describe_sql, timeout=30)
        
        if not success:
            return False, f"Failed to describe table: {message}"
        
        # Parse column types
        schema_data = result.get("result", {}).get("data_array", [])
        column_types = {}
        for row in schema_data:
            if len(row) >= 2:
                column_types[row[0]] = row[1].upper()
        
        st.info(f"Table schema: {column_types}")
        
        # Step 4: Insert data row by row for better error handling
        columns = df.columns.tolist()
        total_rows = len(df)
        successful_inserts = 0
        
        # Try batch insert first (faster)
        batch_size = 100
        for batch_start in range(0, total_rows, batch_size):
            batch_end = min(batch_start + batch_size, total_rows)
            batch_df = df.iloc[batch_start:batch_end]
            
            values_list = []
            for idx, row in batch_df.iterrows():
                row_values = []
                for col_name in columns:
                    val = row[col_name]
                    col_type = column_types.get(col_name, "STRING")
                    
                    # Handle NULL values
                    if pd.isna(val) or val is None or (isinstance(val, str) and val.strip() == ''):
                        row_values.append("NULL")
                        continue
                    
                    # Handle based on column type
                    if "INT" in col_type or "LONG" in col_type or "BIGINT" in col_type:
                        try:
                            row_values.append(str(int(val)))
                        except:
                            row_values.append("NULL")
                    elif "DOUBLE" in col_type or "FLOAT" in col_type or "DECIMAL" in col_type:
                        try:
                            row_values.append(str(float(val)))
                        except:
                            row_values.append("NULL")
                    elif "BOOLEAN" in col_type or "BOOL" in col_type:
                        if isinstance(val, bool):
                            row_values.append("TRUE" if val else "FALSE")
                        elif isinstance(val, str):
                            row_values.append("TRUE" if val.lower() in ['true', '1', 'yes'] else "FALSE")
                        else:
                            row_values.append("FALSE")
                    else:  # STRING, VARCHAR, etc.
                        # Escape special characters
                        str_val = str(val)
                        # Replace backslash first, then single quotes
                        str_val = str_val.replace("\\", "\\\\")
                        str_val = str_val.replace("'", "''")
                        # Also handle newlines and tabs
                        str_val = str_val.replace("\n", "\\n")
                        str_val = str_val.replace("\t", "\\t")
                        row_values.append(f"'{str_val}'")
                
                values_list.append(f"({', '.join(row_values)})")
            
            # Build INSERT statement
            insert_sql = f"""
            INSERT INTO {table_name} 
            ({', '.join(columns)}) 
            VALUES {', '.join(values_list)}
            """
            
            st.info(f"Inserting batch {batch_start//batch_size + 1} ({batch_end - batch_start} rows)...")
            
            success, message, _ = execute_sql_statement(host, token, warehouse_id, insert_sql, timeout=50)
            
            if not success:
                st.error(f"Batch insert failed: {message}")
                # Try individual inserts for this batch
                st.warning("Trying individual row inserts for failed batch...")
                
                for idx, row in batch_df.iterrows():
                    row_values = []
                    for col_name in columns:
                        val = row[col_name]
                        col_type = column_types.get(col_name, "STRING")
                        
                        if pd.isna(val) or val is None or (isinstance(val, str) and val.strip() == ''):
                            row_values.append("NULL")
                            continue
                        
                        if "INT" in col_type or "LONG" in col_type or "BIGINT" in col_type:
                            try:
                                row_values.append(str(int(val)))
                            except:
                                row_values.append("NULL")
                        elif "DOUBLE" in col_type or "FLOAT" in col_type or "DECIMAL" in col_type:
                            try:
                                row_values.append(str(float(val)))
                            except:
                                row_values.append("NULL")
                        elif "BOOLEAN" in col_type or "BOOL" in col_type:
                            if isinstance(val, bool):
                                row_values.append("TRUE" if val else "FALSE")
                            else:
                                row_values.append("FALSE")
                        else:
                            str_val = str(val).replace("\\", "\\\\").replace("'", "''").replace("\n", "\\n").replace("\t", "\\t")
                            row_values.append(f"'{str_val}'")
                    
                    single_insert_sql = f"""
                    INSERT INTO {table_name} 
                    ({', '.join(columns)}) 
                    VALUES ({', '.join(row_values)})
                    """
                    
                    success_single, message_single, _ = execute_sql_statement(host, token, warehouse_id, single_insert_sql, timeout=50)
                    
                    if success_single:
                        successful_inserts += 1
                    else:
                        st.warning(f"Failed to insert row {idx}: {message_single}")
            else:
                successful_inserts += (batch_end - batch_start)
                st.success(f"Batch {batch_start//batch_size + 1} inserted successfully")
        
        # Step 5: Verify the data
        verify_sql = f"SELECT COUNT(*) as cnt FROM {table_name}"
        success, message, result = execute_sql_statement(host, token, warehouse_id, verify_sql, timeout=30)
        
        if success:
            verify_data = result.get("result", {}).get("data_array", [[0]])
            actual_count = verify_data[0][0] if verify_data else 0
            try:
                actual_count = int(actual_count)
            except (ValueError, TypeError):
                actual_count = 0
            
            st.info(f"Verification: {actual_count} rows in table (expected {total_rows})")
            
            if actual_count == total_rows:
                return True, f"Successfully updated {table_name} - All {actual_count} rows written and verified"
            elif actual_count > 0:
                return True, f"Partially updated {table_name} - {actual_count}/{total_rows} rows written"
            else:
                return False, f"No rows were written to {table_name}"
        else:
            if successful_inserts > 0:
                return True, f"Successfully inserted {successful_inserts}/{total_rows} rows (verification skipped)"
            else:
                return False, "No rows were successfully inserted"
        
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        st.error(f"Exception details: {error_details}")
        return False, f"Error writing to table: {str(e)}"

def trigger_job(host, token, job_id, params=None):
    url = f"{host}/api/2.1/jobs/run-now"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"job_id": int(job_id)}
    if params:
        payload["notebook_params"] = params
    response = requests.post(url, headers=headers, json=payload)
    if not response.ok:
        try:
            error_detail = response.json()
            raise Exception(f"API Error: {error_detail.get('message', 'Unknown error')}")
        except:
            response.raise_for_status()
    return response.json()["run_id"]

def get_run_status(host, token, run_id):
    url = f"{host}/api/2.1/jobs/runs/get"
    response = requests.get(url, headers={"Authorization": f"Bearer {token}"}, params={"run_id": run_id})
    response.raise_for_status()
    return response.json()

def get_run_output(host, token, run_id):
    url = f"{host}/api/2.1/jobs/runs/get-output"
    response = requests.get(url, headers={"Authorization": f"Bearer {token}"}, params={"run_id": run_id})
    response.raise_for_status()
    return response.json()

def test_connection(host, token, job_id):
    try:
        url = f"{host}/api/2.1/jobs/get"
        response = requests.get(url, headers={"Authorization": f"Bearer {token}"}, params={"job_id": int(job_id)})
        if not response.ok:
            return False, f"Error: {response.status_code}"
        job_info = response.json()
        job_name = job_info.get('settings', {}).get('name', 'Unknown')
        return True, f"Connected. Job: {job_name}"
    except Exception as e:
        return False, f"Error: {str(e)}"

def read_volume_file(host, token, file_path):
    """Read file from Databricks volume using Files API"""
    try:
        url = f"{host}/api/2.0/fs/files{file_path}"
        headers = {"Authorization": f"Bearer {token}"}
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 404:
            return None, f"File not found: {file_path}"
        
        response.raise_for_status()
        content = response.text
        
        try:
            json_content = json.loads(content)
            return json.dumps(json_content, indent=2), None
        except:
            return content, None
    except Exception as e:
        return None, f"Error reading file: {str(e)}"
    
# ============================================================================
# VALIDATOR MODULE (Updated: Removed Modal Decorator)
# ============================================================================

class SmartPDF(FPDF):
    def header(self):
        self.set_font('Arial', 'B', 12)
        self.set_text_color(100, 100, 100)
        self.cell(0, 10, 'BrickShift Migration Report', 0, 0, 'R')
        self.ln(15)

    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f'Page {self.page_no()}', 0, 0, 'C')

    def write_markdown(self, text):
        self.set_text_color(0, 0, 0)
        # Basic markdown parser logic from prototype
        lines = text.split('\n')
        for line in lines:
            line = line.encode('latin-1', 'replace').decode('latin-1').strip()
            if not line:
                self.ln(2)
                continue
            if line.startswith('# '):
                self.ln(5)
                self.set_font("Arial", 'B', 16)
                self.set_text_color(37, 99, 235) 
                self.cell(0, 10, line.replace('# ', '').upper(), 0, 1)
                self.set_text_color(0, 0, 0) 
            elif line.startswith('## '):
                self.ln(3)
                self.set_font("Arial", 'B', 13)
                self.cell(0, 8, line.replace('## ', ''), 0, 1)
            elif line.startswith('- ') or line.startswith('* '):
                self.set_font("Arial", '', 10)
                self.cell(5)
                self.cell(3, 5, chr(149), 0, 0)
                self.multi_cell(0, 6, line[2:])
            else:
                self.set_font("Arial", '', 10)
                self.multi_cell(0, 6, line)

def generate_report_pdf(summary, details, session_id):
    pdf = SmartPDF()
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.set_font("Arial", 'B', 20)
    pdf.cell(0, 15, "Validation Analysis Result", 0, 1, 'L')
    pdf.set_fill_color(245, 247, 250)
    pdf.rect(10, pdf.get_y(), 190, 20, 'F')
    pdf.set_y(pdf.get_y() + 5)
    pdf.set_font("Arial", '', 10)
    pdf.set_x(15)
    pdf.cell(90, 6, f"Session ID: {session_id[:8]}...", 0, 0)
    pdf.cell(90, 6, f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}", 0, 1)
    pdf.ln(15)
    pdf.write_markdown(summary)
    pdf.ln(10)
    pdf.write_markdown(details)
    return pdf.output(dest='S').encode('latin-1')

def render_validator_section():
    """
    Renders the Validator UI logic directly into the page (moved from modal).
    """
    
    # --- REQUIREMENT: Model Information ---
    st.info("⚡ **Model:** Databricks Claude Opus")
    st.caption("Automated Visual Regression Testing: Power BI vs Databricks")
    
    # --- CONFIGURATION (Load from config.json in production) ---
    config = load_config() # Helper to access config if needed locally
    
    PBI_TENANT_ID = config.get("pbi_tenant_id", "b080f019-905d-405c-960a-013fccc761dd")
    PBI_CLIENT_ID = config.get("pbi_client_id", "890f9089-9d42-4e9b-b4a6-18957f4b87bd")
    PBI_CLIENT_SECRET = os.getenv("PBI_CLIENT_SECRET")
    
    DB_TOKEN = os.getenv("DATABRICKS_TOKEN")
    ENDPOINT_OPUS = config.get("endpoint_opus", "https://adb-3666479212731434.14.azuredatabricks.net/serving-endpoints/databricks-claude-opus-4-6/invocations")
    ENDPOINT_HAIKU = config.get("endpoint_haiku", "https://adb-3666479212731434.14.azuredatabricks.net/serving-endpoints/databricks-claude-haiku-4-5/invocations")

    col_export, col_validate = st.columns(2)

    # --- COLUMN 1: EXPORT ---
    with col_export:
        st.markdown("### 1. Source Snapshot")
        pbi_url = st.text_input("Power BI Report URL", placeholder="https://app.powerbi.com/...", key="val_input_url")
        
        if st.button("🚀 Start Export", type="primary", use_container_width=True, key="val_export_btn"):
            if not pbi_url:
                st.warning("Please provide a URL.")
            else:
                with st.spinner("Snapshotting Source..."):
                    try:
                        result = powerbi_export.export_from_url(pbi_url, PBI_TENANT_ID, PBI_CLIENT_ID, PBI_CLIENT_SECRET)
                        if result.get("export_status") == "success":
                            st.session_state['export_result'] = result
                            st.success("Export Complete!")
                        else:
                            st.error(f"Error: {result.get('error')}")
                    except Exception as e:
                        st.error(f"Critical: {str(e)}")

        if 'export_result' in st.session_state:
            res = st.session_state['export_result']
            # Using a container for consistent styling
            st.markdown(f"""
            <div style="margin-top: 10px; padding: 10px; border: 1px solid #334155; border-radius: 5px; background: #1e293b;">
                <div style="font-size: 0.8em; color: #94a3b8;">READY FOR ANALYSIS</div>
                <div style="color: #f8fafc;">📄 {res['filename']}</div>
            </div>
            """, unsafe_allow_html=True)

    # --- COLUMN 2: VALIDATE ---
    with col_validate:
        st.markdown("### 2. AI Validation")
        
        if 'export_result' not in st.session_state:
            st.info("👈 Complete Step 1 first.")
        else:
            source_path = st.session_state['export_result']['file_path']
            session_id = st.session_state['export_result']['session_id']
            target_pdf = st.file_uploader("Upload Databricks PDF", type=["pdf"], key="val_upload")
            
            if st.button("🤖 Run Comparison", type="primary", use_container_width=True, key="val_run_btn"):
                if not target_pdf:
                    st.warning("Upload target PDF.")
                else:
                    os.makedirs("exports", exist_ok=True)
                    target_path = os.path.join("exports", target_pdf.name)
                    with open(target_path, "wb") as f: f.write(target_pdf.getbuffer())
                    
                    try:
                        with st.spinner("Analyzing pixels & data..."):
                            # 1. Encode
                            source_b64 = vision_engine.encode_pdf_to_base64(source_path)
                            target_b64 = vision_engine.encode_pdf_to_base64(target_path)
                            
                            # 2. Vision Analysis
                            s1 = vision_engine.run_stage1_analysis(source_b64, target_b64, ENDPOINT_OPUS, DB_TOKEN, "PowerBI", "Claude Opus")
                            if s1["status"] == "error": raise Exception(s1["error"])
                            
                            # 3. Summarization
                            s2 = vision_engine.run_stage2_summarization(s1["detailed_analysis"], ENDPOINT_HAIKU, DB_TOKEN, "Claude Haiku")
                            
                            final_result = {
                                "session_id": session_id,
                                "detailed_analysis": s1["detailed_analysis"],
                                "executive_summary": s2["summary"]
                            }
                            st.session_state['current_report'] = final_result
                            st.success("Validation Complete!")
                    except Exception as e:
                        st.error(f"Validation Failed: {str(e)}")

    st.markdown("---")

    # --- REPORTING SECTION ---
    if 'current_report' in st.session_state:
        report = st.session_state['current_report']
        st.markdown("### 📊 Analysis Report")
        
        # Download Button
        pdf_bytes = generate_report_pdf(report['executive_summary'], report['detailed_analysis'], report['session_id'])
        st.download_button(
            label="📥 Download PDF Report",
            data=pdf_bytes,
            file_name="Migration_Report.pdf",
            mime="application/pdf",
            use_container_width=True
        )

        with st.expander("Executive Summary", expanded=True):
            st.markdown(report['executive_summary'])
        with st.expander("Detailed Findings"):
            st.markdown(report['detailed_analysis'])

# The Modal Dialog Function for Genie (unchanged)
@st.dialog("✨ Genie Assistant", width="large")
def open_genie_chat():
    # Header (Only Caption, New Chat button removed)
    st.caption("Powered by Databricks Genie Space")

    # Display Chat History
    for message in st.session_state.genie_messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Chat Input
    if prompt := st.chat_input("Ask a question about your data..."):
        # 1. Append User Message
        st.session_state.genie_messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # 2. Generate Response
        config = load_config()
        genie_space_id = config.get("genie_space_id", "")
        databricks_host = normalize_host_url(config.get("databricks_host", ""))
        databricks_token = os.getenv("DATABRICKS_TOKEN")

        if not genie_space_id:
            error_msg = "⚠️ Error: 'genie_space_id' is missing in config.json."
            st.session_state.genie_messages.append({"role": "assistant", "content": error_msg})
            with st.chat_message("assistant"):
                st.error(error_msg)
        else:
            # Call Backend API
            conv_id, response_text = query_genie_api(
                databricks_host, 
                databricks_token, 
                genie_space_id, 
                prompt, 
                st.session_state.genie_conversation_id
            )
            
            # Update ID for context
            if conv_id:
                st.session_state.genie_conversation_id = conv_id
            
            # Append Assistant Message
            st.session_state.genie_messages.append({"role": "assistant", "content": response_text})
            with st.chat_message("assistant"):
                st.markdown(response_text)

# Initialize session state
if 'run_history' not in st.session_state:
    st.session_state.run_history = []

if 'main_panel' not in st.session_state:
    st.session_state.main_panel = 'thoughtspot'

if 'ts_active_tab' not in st.session_state:
    st.session_state.ts_active_tab = 'conversion'

if 'pbi_active_tab' not in st.session_state:
    st.session_state.pbi_active_tab = 'conversion'

if 'tableau_active_tab' not in st.session_state:
    st.session_state.tableau_active_tab = 'conversion'

config = load_config()

databricks_host = normalize_host_url(config.get("databricks_host", ""))
#databricks_token = config.get("databricks_token", "")
databricks_token = os.getenv("DATABRICKS_TOKEN")
warehouse_id = config.get("warehouse_id", "")
genie_space_id = config.get("genie_space_id", "")
pbi_client_secret = os.getenv("PBI_CLIENT_SECRET")

# ThoughtSpot Job IDs
ts_visual_job_id = config.get("visual_job_id", "")
ts_data_job_id = config.get("data_job_id", "")
ts_data_output_volume = config.get("data_output_volume", "/Volumes/catalog/schema/volume_name/output/")

# Power BI Job IDs (UPDATED)
# Fallback to general conversion ID if specific visual ID not present
pbi_conversion_job_id = config.get("pbi_conversion_job_id", "")
pbi_visual_job_id = config.get("pbi_visual_job_id", pbi_conversion_job_id)
pbi_data_job_id = config.get("pbi_data_job_id", "")
pbi_discovery_job_id = config.get("pbi_discovery_job_id", "")

# Tableau Job IDs (UPDATED)
# Fallback to general conversion ID if specific visual ID not present
tableau_conversion_job_id = config.get("tableau_conversion_job_id", "")
tableau_visual_job_id = config.get("tableau_visual_job_id", tableau_conversion_job_id)
tableau_data_job_id = config.get("tableau_data_job_id", "")

auto_refresh = config.get("auto_refresh", True)
show_logs = config.get("show_logs", True)

if not all([databricks_host, databricks_token]):
    st.error("Configuration required. Please check config.json")
    st.stop()

# Main Header with Test Connection button
header_col1, header_col2 = st.columns([6, 1])

with header_col1:
    st.markdown("""
    <div style="text-align: center; padding: 0.5rem 0 0.25rem 0; margin-top: 0;">
        <div style="font-size: 1.75rem; font-weight: 700; color: #f8fafc; letter-spacing: 1px;">BrickShift</div>
        <div style="font-size: 0.85rem; color: #94a3b8; margin-top: 0.15rem;">Multi-Platform Dashboard Migration to Databricks AI/BI</div>
    </div>
    """, unsafe_allow_html=True)

with header_col2:
    test_job_id = None
    if st.session_state.main_panel == 'thoughtspot':
        test_job_id = ts_visual_job_id if ts_visual_job_id else ts_data_job_id
    elif st.session_state.main_panel == 'powerbi':
        test_job_id = pbi_visual_job_id if pbi_visual_job_id else (pbi_data_job_id if pbi_data_job_id else pbi_discovery_job_id)
    elif st.session_state.main_panel == 'tableau':
        test_job_id = tableau_visual_job_id if tableau_visual_job_id else tableau_data_job_id

    if test_job_id:
        if st.button("Test Connection", type="secondary", key="global_test_conn"):
            with st.spinner("Testing..."):
                success, message = test_connection(databricks_host, databricks_token, test_job_id)
                if success:
                    st.toast("Connection successful")
                else:
                    st.toast("Connection failed")

# Panel selection buttons with active state styling
col1, col2, col3 = st.columns(3)

with col1:
    ts_button_type = "primary" if st.session_state.main_panel == 'thoughtspot' else "secondary"
    if st.button("ThoughtSpot", key="select_thoughtspot", type=ts_button_type, use_container_width=True):
        st.session_state.main_panel = 'thoughtspot'
        st.rerun()

with col2:
    pbi_button_type = "primary" if st.session_state.main_panel == 'powerbi' else "secondary"
    if st.button("Power BI", key="select_powerbi", type=pbi_button_type, use_container_width=True):
        st.session_state.main_panel = 'powerbi'
        st.rerun()

with col3:
    # UPDATED: Enabled Tableau Button
    tableau_button_type = "primary" if st.session_state.main_panel == 'tableau' else "secondary"
    if st.button("Tableau", key="select_tableau", type=tableau_button_type, use_container_width=True):
        st.session_state.main_panel = 'tableau'
        st.rerun()

st.markdown("---")

# ============================================================================
# SIDEBAR NAVIGATION
# ============================================================================
with st.sidebar:
    st.markdown("""
    <div style="text-align: center; padding: 1rem 0 1.5rem 0; border-bottom: 1px solid #334155; margin-bottom: 1.5rem;">
        <div style="font-size: 1.1rem; font-weight: 600; color: #f8fafc;">Navigation</div>
    </div>
    """, unsafe_allow_html=True)
    
    # ---------------------------------------
    # Regular Navigation Buttons
    # ---------------------------------------
    if st.session_state.main_panel == 'thoughtspot':
        st.markdown("### ThoughtSpot Migration")
        if st.button("Conversion", key="ts_tab_conversion", use_container_width=True):
            st.session_state.ts_active_tab = 'conversion'
        if st.button("Config Manager", key="ts_tab_config", use_container_width=True):
            st.session_state.ts_active_tab = 'config'
        if st.button("History", key="ts_tab_history", use_container_width=True):
            st.session_state.ts_active_tab = 'history'
        if st.button("Discovery Details", key="ts_tab_discovery", use_container_width=True):
            st.session_state.ts_active_tab = 'discovery'
    
    elif st.session_state.main_panel == 'powerbi':
        st.markdown("### Power BI Migration")
        if st.button("Conversion", key="pbi_tab_conversion", use_container_width=True):
            st.session_state.pbi_active_tab = 'conversion'
        if st.button("Config Manager", key="pbi_tab_config", use_container_width=True):
            st.session_state.pbi_active_tab = 'config'
        if st.button("History", key="pbi_tab_history", use_container_width=True):
            st.session_state.pbi_active_tab = 'history'
        if st.button("Discovery Details", key="pbi_tab_discovery", use_container_width=True):
            st.session_state.pbi_active_tab = 'discovery'

    elif st.session_state.main_panel == 'tableau':
        st.markdown("### Tableau Migration")
        if st.button("Conversion", key="tableau_tab_conversion", use_container_width=True):
            st.session_state.tableau_active_tab = 'conversion'
        if st.button("Config Manager", key="tableau_tab_config", use_container_width=True):
            st.session_state.tableau_active_tab = 'config'
        if st.button("History", key="tableau_tab_history", use_container_width=True):
            st.session_state.tableau_active_tab = 'history'

    # ---------------------------------------
    # GENIE CHAT LOGIC INIT
    # ---------------------------------------
    
    # Initialize Chat State
    if "genie_messages" not in st.session_state:
        st.session_state.genie_messages = [{"role": "assistant", "content": "Hello! I am connected to your Databricks Dataset Space. Ask me anything about your data."}]
    if "genie_conversation_id" not in st.session_state:
        st.session_state.genie_conversation_id = None

    # ---------------------------------------
    # SIDEBAR FOOTER (Floating Buttons)
    # ---------------------------------------
    
    # 1. Spacer to prevent overlap
    st.markdown("<div style='height: 140px;'></div>", unsafe_allow_html=True)
    
    # 2. Container
    st.markdown('<div class="genie-widget-container">', unsafe_allow_html=True)
    
    # BUTTON 1: GENIE
    if st.button("✨ Ask Genie", key="genie_trigger_btn", type="secondary", use_container_width=True):
        open_genie_chat()

    # NOTE: VALIDATOR BUTTON REMOVED FROM HERE as requested
        
    st.markdown('</div>', unsafe_allow_html=True)

# ============================================================================
# THOUGHTSPOT MIGRATION PANEL
# ============================================================================
if st.session_state.main_panel == 'thoughtspot':
    
    # CONVERSION TAB
    if st.session_state.ts_active_tab == 'conversion':
        st.markdown('<h1 style="color: white; text-align: center; margin: 0; padding: 0.5rem 0;">ThoughtSpot to Databricks</h1>', unsafe_allow_html=True)
        st.markdown('<p style="color: rgba(255,255,255,0.9); text-align: center; margin: 0 0 1.5rem 0; font-size: 0.95rem;">Dashboard Migration</p>', unsafe_allow_html=True)
        
        # UPDATED: Added Validator Tab
        conversion_tab1, conversion_tab2, conversion_tab3 = st.tabs(["Data Conversion", "Visual Conversion", "Migration Validator"])
        
        # DATA CONVERSION TAB
        with conversion_tab1:
            st.markdown('<p style="margin: 1rem 0;">Convert ThoughtSpot data models to Databricks format</p>', unsafe_allow_html=True)
            
            if not ts_data_job_id:
                st.warning("Data conversion job ID not configured in config.json")
            else:
                col1, col2, col3 = st.columns([1, 1, 1])
                with col2:
                    start_data_button = st.button("Start Data Conversion", type="primary", use_container_width=True, key="start_ts_data_conv")
                
        
        # VISUAL CONVERSION TAB
        with conversion_tab2:
            st.markdown('<p style="margin: 1rem 0;">Convert ThoughtSpot dashboards to Databricks AI/BI</p>', unsafe_allow_html=True)
            
            if not ts_visual_job_id:
                st.warning("Visual conversion job ID not configured in config.json")
            else:
                col1, col2, col3 = st.columns([1, 1, 1])
                with col2:
                    start_visual_button = st.button("Start Visual Conversion", type="primary", use_container_width=True, key="start_ts_visual_conv")
                    
        # VALIDATOR TAB (Placeholder)
        with conversion_tab3:
            st.markdown('<div class="coming-soon-screen"><div class="coming-soon-title">ThoughtSpot Validator</div><div class="coming-soon-text">Visual regression testing and automated validation for ThoughtSpot migrations is currently under development.</div><div class="coming-soon-badge">Coming Soon</div></div>', unsafe_allow_html=True)
                
    # CONFIG MANAGER TAB
    elif st.session_state.ts_active_tab == 'config':
        st.markdown('<h1 style="color: white;">ThoughtSpot Config Manager</h1>', unsafe_allow_html=True)
        st.markdown('<p>View and manage configuration tables</p>', unsafe_allow_html=True)
        
        if not warehouse_id:
            st.warning("Warehouse ID not configured in config.json")
        else:
            st.info(f"Using warehouse: {warehouse_id}")
            
            tab1, tab2, tab3 = st.tabs(["Chart Type Mappings", "Liveboard Migration Config", "TML DBX Metadata Mapping"])
            
            # Chart Type Mappings
            with tab1:
                st.markdown("### Chart Type Mappings")
                table_name = "dbx_migration_poc.dbx_migration_ts.chart_type_mappings"
                
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.markdown("Edit the configuration below and click Save to update the table.")
                with col2:
                    if st.button("Refresh", key="refresh_chart_mappings"):
                        st.rerun()
                
                df = get_table_data_for_config(databricks_host, databricks_token, warehouse_id, table_name)
                
                if not df.empty:
                    # Create editable dataframe
                    edited_df = st.data_editor(
                        df, 
                        use_container_width=True, 
                        height=400,
                        num_rows="dynamic",
                        key="chart_type_mappings_editor"
                    )
                    
                    col1, col2, col3 = st.columns([1, 1, 1])
                    with col2:
                        if st.button("Save Changes", type="primary", key="save_chart_mappings", use_container_width=True):
                            with st.spinner("Saving changes..."):
                                success, message = write_table_data_sql(
                                    databricks_host, 
                                    databricks_token, 
                                    warehouse_id, 
                                    table_name, 
                                    edited_df
                                )
                                
                                if success:
                                    st.success(message)
                                    time.sleep(1)
                                    st.rerun()
                                else:
                                    st.error(message)
                else:
                    st.warning("No data to display")
                    
            # Liveboard Migration Config
            with tab2:
                st.markdown("### Liveboard Migration Config")
                table_name = "dbx_migration_poc.dbx_migration_ts.liveboard_migration_config"
                
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.markdown("Edit the configuration below and click Save to update the table.")
                with col2:
                    if st.button("Refresh", key="refresh_liveboard_config"):
                        st.rerun()
                
                df = get_table_data_for_config(databricks_host, databricks_token, warehouse_id, table_name)
                
                if not df.empty:
                    # Show data info for debugging
                    st.caption(f"Loaded {len(df)} rows with columns: {', '.join(df.columns.tolist())}")
                    
                    # Create editable dataframe
                    edited_df = st.data_editor(
                        df, 
                        use_container_width=True, 
                        height=400,
                        num_rows="dynamic",
                        key="liveboard_config_editor"
                    )
                    
                    # Show edited data info
                    st.caption(f"Edited data: {len(edited_df)} rows")
                    
                    col1, col2, col3 = st.columns([1, 1, 1])
                    with col2:
                        if st.button("Save Changes", type="primary", key="save_liveboard_config", use_container_width=True):
                            # Add debug info
                            st.info(f"Attempting to save {len(edited_df)} rows to {table_name}")
                            st.info(f"DataFrame columns: {edited_df.columns.tolist()}")
                            st.info(f"DataFrame dtypes: {edited_df.dtypes.to_dict()}")
                            
                            # Show first row as sample
                            if len(edited_df) > 0:
                                st.info(f"Sample row: {edited_df.iloc[0].to_dict()}")
                            
                            with st.spinner("Saving changes..."):
                                success, message = write_table_data_sql(
                                    databricks_host, 
                                    databricks_token, 
                                    warehouse_id, 
                                    table_name, 
                                    edited_df
                                )
                                
                                if success:
                                    st.success(message)
                                    time.sleep(1)
                                    st.rerun()
                                else:
                                    st.error(f"Failed: {message}")
                                    # Add more detailed error info
                                    st.error("Please check if the table structure matches the data structure")
                else:
                    st.warning("No data to display")
            
            # TML DBX Metadata Mapping
            with tab3:
                st.markdown("### TML DBX Metadata Mapping")
                table_name = "dbx_migration_poc.dbx_migration_ts.tml_dbx_metadata_mapping"
                
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.markdown("Edit the configuration below and click Save to update the table.")
                with col2:
                    if st.button("Refresh", key="refresh_tml_mapping"):
                        st.rerun()
                
                df = get_table_data_for_config(databricks_host, databricks_token, warehouse_id, table_name)
                
                if not df.empty:
                    edited_df = st.data_editor(
                        df, 
                        use_container_width=True, 
                        height=400,
                        num_rows="dynamic",
                        key="tml_mapping_editor"
                    )
                    
                    col1, col2, col3 = st.columns([1, 1, 1])
                    with col2:
                        if st.button("Save Changes", type="primary", key="save_tml_mapping", use_container_width=True):
                            with st.spinner("Saving changes to Unity Catalog..."):
                                success, message = write_table_data_sql(databricks_host, databricks_token, warehouse_id, table_name, edited_df)
                                if success:
                                    st.success(message)
                                    time.sleep(1)
                                    st.rerun()
                                else:
                                    st.error(message)
                else:
                    st.warning("No data to display")
    
    # HISTORY TAB
    elif st.session_state.ts_active_tab == 'history':
        st.markdown('<h1 style="color: white;">ThoughtSpot Conversion History</h1>', unsafe_allow_html=True)
        st.markdown('<p>View past dashboard conversions from this session</p>', unsafe_allow_html=True)
        
        ts_history = [h for h in st.session_state.run_history if h.get('source') == 'thoughtspot']
        
        if ts_history:
            st.markdown(f"### Total Runs: {len(ts_history)}")
            st.table(pd.DataFrame(ts_history))
        else:
            st.info("No ThoughtSpot conversion history available in this session.")
    
    # DISCOVERY TAB
    elif st.session_state.ts_active_tab == 'discovery':
        st.markdown('<h1 style="color: white;">ThoughtSpot Discovery Details</h1>', unsafe_allow_html=True)
        st.markdown('<p>Analyze dependencies and complexity scores</p>', unsafe_allow_html=True)

        if not warehouse_id:
            st.warning("Warehouse ID not configured in config.json")
        else:
            tab1, tab2 = st.tabs(["Dependency Mapping", "Complexity Scoring"])

            with tab1:
                st.markdown("### TML Dependency Mapping")
                st.markdown("---")
                
                with st.spinner("Loading dependency data..."):
                    df_dep = get_table_data_simple(
                        databricks_host, 
                        databricks_token, 
                        warehouse_id, 
                        "dbx_migration_poc.dbx_migration_ts.tml_dependency"
                    )
                
                if not df_dep.empty:
                    st.dataframe(df_dep, use_container_width=True, height=500)
                    st.caption(f"Total dependencies found: {len(df_dep)}")
                else:
                    st.warning("No dependency data available.")

            with tab2:
                st.markdown("### Liveboard Complexity Score")
                st.markdown("---")
                
                with st.spinner("Loading complexity scores..."):
                    df_comp = get_table_data_simple(
                        databricks_host, 
                        databricks_token, 
                        warehouse_id, 
                        "dbx_migration_poc.dbx_migration_ts.liveboard_complexity_score"
                    )
                
                if not df_comp.empty:
                    score_col = next((col for col in df_comp.columns if 'score' in col.lower() or 'complexity' in col.lower()), None)
                    
                    if score_col:
                        avg_score = df_comp[score_col].mean() if pd.api.types.is_numeric_dtype(df_comp[score_col]) else 0
                        if avg_score > 0:
                            st.markdown(f'<div class="metric-card" style="width: 300px; margin-bottom: 20px;"><div class="metric-label">Average Complexity</div><div class="metric-value">{avg_score:.2f}</div></div>', unsafe_allow_html=True)

                    st.dataframe(df_comp, use_container_width=True, height=500)
                    st.caption(f"Total records: {len(df_comp)}")
                else:
                    st.warning("No complexity score data available.")

    # Process ThoughtSpot job actions
    if st.session_state.ts_active_tab == 'conversion':
        # Visual conversion handlers
        if ts_visual_job_id:
            if 'start_visual_button' in locals() and start_visual_button:
                try:
                    with st.spinner("Initiating visual conversion..."):
                        run_id = trigger_job(databricks_host, databricks_token, ts_visual_job_id, None)
                        st.session_state.ts_visual_run_id = run_id
                        st.session_state.ts_visual_start_time = datetime.now()
                        st.markdown(f'<div class="message-box message-success">Visual conversion started. Run ID: {run_id}</div>', unsafe_allow_html=True)
                        time.sleep(1)
                        st.rerun()
                except Exception as e:
                    st.markdown(f'<div class="message-box message-error">Error: {str(e)}</div>', unsafe_allow_html=True)

            if st.session_state.get("ts_visual_run_id"):
                with conversion_tab2:
                    try:
                        run_id = st.session_state.ts_visual_run_id
                        run_status = get_run_status(databricks_host, databricks_token, run_id)
                        state = run_status.get("state", {})
                        life_cycle_state = state.get("life_cycle_state", "UNKNOWN")
                        result_state = state.get("result_state", "")
                        tasks = run_status.get("tasks", [])
                    
                        visual_tab_out1, visual_tab_out2 = st.tabs(["Dashboard Output", "Execution Details"])
                    
                        # -----------------------------------------------------------
                        # UPDATED LOGIC HERE: Improved Task Output Discovery
                        # -----------------------------------------------------------
                        with visual_tab_out1:
                            if life_cycle_state == "TERMINATED":
                                dashboard_found = False
                                
                                # Iterate through ALL tasks (reversed to find latest) to find dashboard output
                                for task in reversed(tasks):
                                    if task.get("run_id"):
                                        try:
                                            task_output = get_run_output(databricks_host, databricks_token, task["run_id"])
                                            
                                            # Check if notebook output exists
                                            notebook_output = task_output.get("notebook_output", {})
                                            result_str = notebook_output.get("result")
                                            
                                            if result_str:
                                                try:
                                                    result_data = json.loads(result_str)
                                                    
                                                    # Validate if this is the correct JSON payload containing dashboard info
                                                    if "dashboard_id" in result_data or "dashboard_url" in result_data:
                                                        st.markdown('<div class="dashboard-card"><div class="dashboard-title">Conversion Complete</div>', unsafe_allow_html=True)
                                                        col1, col2, col3 = st.columns(3)
                                                        with col1:
                                                            st.markdown(f'<div class="dashboard-item"><div class="dashboard-label">Dashboard ID</div><div class="dashboard-value">{result_data.get("dashboard_id", "N/A")}</div></div>', unsafe_allow_html=True)
                                                        with col2:
                                                            st.markdown(f'<div class="dashboard-item"><div class="dashboard-label">Dashboard Name</div><div class="dashboard-value">{result_data.get("dashboard_name", "N/A")}</div></div>', unsafe_allow_html=True)
                                                        with col3:
                                                            url = result_data.get("dashboard_url", "")
                                                            link_html = f'<a href="{url}" target="_blank" class="dashboard-link">Open Dashboard</a>' if url else "Not Available"
                                                            st.markdown(f'<div class="dashboard-item"><div class="dashboard-label">Link</div><div class="dashboard-value">{link_html}</div></div>', unsafe_allow_html=True)
                                                        st.markdown('</div>', unsafe_allow_html=True)
                                                        
                                                        dashboard_found = True
                                                        break # Stop searching once found
                                                except json.JSONDecodeError:
                                                    continue
                                        except Exception as e:
                                            continue
                                
                                # If loop finishes without finding dashboard info
                                if not dashboard_found:
                                    if result_state == "SUCCESS":
                                        st.markdown('<div class="message-box message-info">Job succeeded, but no dashboard output details found in logs.</div>', unsafe_allow_html=True)
                                    else:
                                        st.markdown('<div class="message-box message-info">Dashboard information not available (Job Failed or Cancelled)</div>', unsafe_allow_html=True)
                                
                            else:
                                st.markdown('<div class="message-box message-info">Conversion in progress...</div>', unsafe_allow_html=True)
                    
                        with visual_tab_out2:
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.markdown(f'<div class="metric-card"><div class="metric-label">Run ID</div><div class="metric-value">{run_id}</div></div>', unsafe_allow_html=True)
                            with col2:
                                status_display = result_state if result_state else life_cycle_state
                                status_class = "status-success" if result_state == "SUCCESS" else "status-error" if result_state in ["FAILED", "TIMEDOUT"] else "status-running"
                                st.markdown(f'<div class="metric-card"><div class="metric-label">Status</div><div class="metric-value {status_class}">{status_display}</div></div>', unsafe_allow_html=True)
                            # --- FIX APPLIED HERE ---
                            with col3:
                                elapsed = 0
                                # If job is finished, calculate total duration from server times
                                if life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                                    if run_status.get("start_time") and run_status.get("end_time"):
                                        elapsed = int((run_status["end_time"] - run_status["start_time"]) / 1000)
                                    elif st.session_state.get("ts_visual_start_time"):
                                        # Fallback if server times missing (locks at current time)
                                        elapsed = (datetime.now() - st.session_state.ts_visual_start_time).seconds
                                else:
                                    # Job is running, use live counter
                                    if st.session_state.get("ts_visual_start_time"):
                                        elapsed = (datetime.now() - st.session_state.ts_visual_start_time).seconds
                                
                                st.markdown(f'<div class="metric-card"><div class="metric-label">Elapsed Time</div><div class="metric-value">{elapsed}s</div></div>', unsafe_allow_html=True)
                            with col4:
                                url = run_status.get("run_page_url", "")
                                if url:
                                    st.markdown(f'<div class="metric-card"><div class="metric-label">Details</div><div class="metric-value"><a href="{url}" target="_blank" style="color: #667eea;">View Logs</a></div></div>', unsafe_allow_html=True)
                        
                            st.markdown("### Task Execution")
                            for task in tasks:
                                task_state = task.get("state", {})
                                task_name = task.get("task_key", "Unknown")
                                task_result = task_state.get("result_state", "")
                                symbol = "[SUCCESS]" if task_result == "SUCCESS" else "[FAILED]" if task_result in ["FAILED", "TIMEDOUT"] else "[PENDING]"
                                with st.expander(f"{symbol} {task_name}", expanded=False):
                                    st.write(f"**Status:** {task_result if task_result else task_state.get('life_cycle_state', 'UNKNOWN')}")
                                    if task.get("start_time") and task.get("end_time"):
                                        st.write(f"**Duration:** {(task['end_time'] - task['start_time']) / 1000:.2f}s")
                    
                        if auto_refresh and life_cycle_state in ["PENDING", "RUNNING"]:
                            time.sleep(config.get("refresh_interval_seconds", 5))
                            st.rerun()
                    except Exception as e:
                        st.markdown(f'<div class="message-box message-error">Error: {str(e)}</div>', unsafe_allow_html=True)

        # Data conversion handlers
        if ts_data_job_id:
            if 'start_data_button' in locals() and start_data_button:
                try:
                    with st.spinner("Initiating data conversion..."):
                        run_id = trigger_job(databricks_host, databricks_token, ts_data_job_id, None)
                        st.session_state.ts_data_run_id = run_id
                        st.session_state.ts_data_start_time = datetime.now()
                        st.markdown(f'<div class="message-box message-success">Data conversion started. Run ID: {run_id}</div>', unsafe_allow_html=True)
                        time.sleep(1)
                        st.rerun()
                except Exception as e:
                    st.markdown(f'<div class="message-box message-error">Error: {str(e)}</div>', unsafe_allow_html=True)

            if st.session_state.get("ts_data_run_id"):
                with conversion_tab1:
                    try:
                        run_id = st.session_state.ts_data_run_id
                        run_status = get_run_status(databricks_host, databricks_token, run_id)
                        state = run_status.get("state", {})
                        life_cycle_state = state.get("life_cycle_state", "UNKNOWN")
                        result_state = state.get("result_state", "")
                        tasks = run_status.get("tasks", [])
                    
                        data_tab_out1, data_tab_out2 = st.tabs(["Output", "Execution Details"])
                    
                        with data_tab_out1:
                            if life_cycle_state == "TERMINATED":
                                # 1. Attempt to fetch notebook output first (for both Success and Failure scenarios)
                                notebook_data = {}
                                task = tasks[-1] if tasks else None
                                
                                if task and task.get("run_id"):
                                    try:
                                        task_output = get_run_output(databricks_host, databricks_token, task["run_id"])
                                        notebook_result_raw = task_output.get("notebook_output", {}).get("result")
                                        if notebook_result_raw:
                                            # Try to parse the exit string as JSON
                                            try:
                                                notebook_data = json.loads(notebook_result_raw)
                                            except json.JSONDecodeError:
                                                # If exit string isn't JSON, treat it as a simple string or ignore
                                                pass
                                    except Exception as e:
                                        # Handle API errors quietly here, will catch in generic failure if needed
                                        pass

                                # 2. CHECK FOR CUSTOM ERROR FIRST (Missing Tables)
                                if "Error_Message" in notebook_data:
                                    error_msg = notebook_data["Error_Message"]
                                    st.markdown(f'''
                                        <div class="message-box message-error">
                                            <div style="font-weight: bold; font-size: 1.1em; margin-bottom: 0.5rem;">🛑 Validation Failure</div>
                                            {error_msg}
                                        </div>
                                    ''', unsafe_allow_html=True)

                                # 3. CHECK FOR STANDARD SUCCESS (No Error_Message found)
                                elif result_state == "SUCCESS":
                                    st.markdown('<div class="message-box message-success">Data conversion completed successfully.</div>', unsafe_allow_html=True)
                                    
                                    generated_query = notebook_data.get("Query", "No query returned")
                                    generated_filepath = notebook_data.get("Filepath", "")
                                    
                                    st.markdown('<div class="dashboard-card"><div class="dashboard-title">Generated SQL</div>', unsafe_allow_html=True)
                                    
                                    st.markdown("##### Final SQL Query")
                                    st.code(generated_query, language="sql")
                                    
                                    if generated_filepath:
                                        st.markdown(f'<div class="message-box message-info">File Path: <strong>{generated_filepath}</strong></div>', unsafe_allow_html=True)
                                        
                                        with st.spinner("Reading generated file..."):
                                            content, error = read_volume_file(databricks_host, databricks_token, generated_filepath)
                                            
                                            if error:
                                                st.error(error)
                                            else:
                                                with st.expander("View File Content", expanded=True):
                                                    st.text(content)
                                                    st.download_button(
                                                        label="Download SQL File",
                                                        data=content,
                                                        file_name=os.path.basename(generated_filepath),
                                                        mime="application/sql"
                                                    )
                                    st.markdown('</div>', unsafe_allow_html=True)

                                # 4. CHECK FOR GENERIC FAILURE (Job failed, but no specific JSON exit message)
                                elif result_state in ["FAILED", "TIMEDOUT", "CANCELED"]:
                                    st.markdown(f'<div class="message-box message-error">Data conversion failed. Status: {result_state}</div>', unsafe_allow_html=True)
                                    if state.get("state_message"):
                                        st.error(f"Error Details: {state.get('state_message')}")
                                else:
                                    st.markdown(f'<div class="message-box message-error">Job ended with unexpected status: {result_state}</div>', unsafe_allow_html=True)
                            
                            else:
                                # RUNNING STATE
                                st.markdown(f'<div class="message-box message-info">Data conversion in progress... (State: {life_cycle_state})</div>', unsafe_allow_html=True)
                                st.progress(0.5)
                    
                        with data_tab_out2:
                            # ... (Keep existing execution details logic) ...
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.markdown(f'<div class="metric-card"><div class="metric-label">Run ID</div><div class="metric-value">{run_id}</div></div>', unsafe_allow_html=True)
                            with col2:
                                status_display = result_state if result_state else life_cycle_state
                                status_class = "status-success" if result_state == "SUCCESS" else "status-error" if result_state in ["FAILED", "TIMEDOUT"] else "status-running"
                                st.markdown(f'<div class="metric-card"><div class="metric-label">Status</div><div class="metric-value {status_class}">{status_display}</div></div>', unsafe_allow_html=True)
                            with col3:
                                elapsed = 0
                                if life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                                    if run_status.get("start_time") and run_status.get("end_time"):
                                        elapsed = int((run_status["end_time"] - run_status["start_time"]) / 1000)
                                    elif st.session_state.get("ts_data_start_time"):
                                        elapsed = (datetime.now() - st.session_state.ts_data_start_time).seconds
                                else:
                                    if st.session_state.get("ts_data_start_time"):
                                        elapsed = (datetime.now() - st.session_state.ts_data_start_time).seconds
                                
                                st.markdown(f'<div class="metric-card"><div class="metric-label">Elapsed Time</div><div class="metric-value">{elapsed}s</div></div>', unsafe_allow_html=True)
                            with col4:
                                url = run_status.get("run_page_url", "")
                                if url:
                                    st.markdown(f'<div class="metric-card"><div class="metric-label">Details</div><div class="metric-value"><a href="{url}" target="_blank" style="color: #667eea;">View Logs</a></div></div>', unsafe_allow_html=True)
                            
                            st.markdown("### Task Execution")
                            for task in tasks:
                                task_state = task.get("state", {})
                                task_name = task.get("task_key", "Unknown")
                                task_result = task_state.get("result_state", "")
                                symbol = "[SUCCESS]" if task_result == "SUCCESS" else "[FAILED]" if task_result in ["FAILED", "TIMEDOUT"] else "[PENDING]"
                                with st.expander(f"{symbol} {task_name}", expanded=False):
                                    st.write(f"**Status:** {task_result if task_result else task_state.get('life_cycle_state', 'UNKNOWN')}")
                                    if task.get("start_time") and task.get("end_time"):
                                        st.write(f"**Duration:** {(task['end_time'] - task['start_time']) / 1000:.2f}s")

                        if auto_refresh and life_cycle_state in ["PENDING", "RUNNING"]:
                            time.sleep(config.get("refresh_interval_seconds", 5))
                            st.rerun()
                            
                    except Exception as e:
                        st.markdown(f'<div class="message-box message-error">Error: {str(e)}</div>', unsafe_allow_html=True)

# ============================================================================
# POWER BI MIGRATION PANEL
# ============================================================================
elif st.session_state.main_panel == 'powerbi':
    
    # CONVERSION TAB
    if st.session_state.pbi_active_tab == 'conversion':
        st.markdown('<h1 style="color: white; text-align: center; margin: 0; padding: 0.5rem 0;">Power BI to Databricks</h1>', unsafe_allow_html=True)
        st.markdown('<p style="color: rgba(255,255,255,0.9); text-align: center; margin: 0 0 1.5rem 0; font-size: 0.95rem;">Dashboard Migration</p>', unsafe_allow_html=True)
        
        # New Tab structure for PBI (Includes Validator)
        pbi_conv_tab1, pbi_conv_tab2, pbi_conv_tab3 = st.tabs(["Data Conversion", "Visual Conversion", "Migration Validator"])

        # PBI DATA CONVERSION
        with pbi_conv_tab1:
            st.markdown('<p style="margin: 1rem 0;">Convert Power BI Data Models (DAX/M) to Databricks SQL</p>', unsafe_allow_html=True)
            
            if not pbi_data_job_id:
                st.warning("Power BI Data conversion job ID not configured (pbi_data_job_id)")
            else:
                col1, col2, col3 = st.columns([1, 1, 1])
                with col2:
                    start_pbi_data_button = st.button("Start Data Conversion", type="primary", use_container_width=True, key="start_pbi_data_conv")

        # PBI VISUAL CONVERSION
        with pbi_conv_tab2:
            st.markdown('<p style="margin: 1rem 0;">Convert Power BI Reports to Databricks AI/BI</p>', unsafe_allow_html=True)
            
            if not pbi_visual_job_id:
                st.warning("Power BI Visual conversion job ID not configured (pbi_visual_job_id)")
            else:
                col1, col2, col3 = st.columns([1, 1, 1])
                with col2:
                    start_pbi_visual_button = st.button("Start Visual Conversion", type="primary", use_container_width=True, key="start_pbi_visual_conv")

        # PBI VALIDATOR (NEW SECTION)
        with pbi_conv_tab3:
            st.markdown('<p style="margin: 1rem 0;">Validate migration accuracy using Databricks AI</p>', unsafe_allow_html=True)
            # Call the refactored validator function here
            render_validator_section()

    # CONFIG MANAGER TAB
    elif st.session_state.pbi_active_tab == 'config':
        st.markdown('<h1 style="color: white;">Power BI Config Manager</h1>', unsafe_allow_html=True)
        st.markdown('<p>View and manage configuration tables</p>', unsafe_allow_html=True)
        
        if not warehouse_id:
            st.warning("Warehouse ID not configured in config.json")
        else:
            st.info(f"Using warehouse: {warehouse_id}")
            
            config_tables = [
                ("Chart Type Mappings", "dbx_migration_poc.dbx_migration_pbi.chart_type_mappings"),
                ("Expression Transformations", "dbx_migration_poc.dbx_migration_pbi.expression_transformations"),
                ("Scale Type Detection", "dbx_migration_poc.dbx_migration_pbi.scale_type_detection"),
                ("Widget Size Config", "dbx_migration_poc.dbx_migration_pbi.widget_size_config"),
                ("DAX to SQL Mapping", "dbx_migration_poc.dbx_migration_pbi.dax_to_sql_mapping_v3")
            ]
            
            tabs = st.tabs([name for name, _ in config_tables])
            
            for idx, (tab_name, table_name) in enumerate(config_tables):
                with tabs[idx]:
                    st.markdown(f"### {tab_name}")
                    
                    col1, col2 = st.columns([3, 1])
                    with col1:
                        st.markdown("Edit the configuration below and click Save to update the table.")
                    with col2:
                        if st.button("Refresh", key=f"refresh_pbi_{idx}"):
                            st.rerun()
                    
                    df = get_table_data_for_config(databricks_host, databricks_token, warehouse_id, table_name)
                    
                    if not df.empty:
                        edited_df = st.data_editor(
                            df, 
                            use_container_width=True, 
                            height=400,
                            num_rows="dynamic",
                            key=f"pbi_config_editor_{idx}"
                        )
                        
                        col1, col2, col3 = st.columns([1, 1, 1])
                        with col2:
                            if st.button("Save Changes", type="primary", key=f"save_pbi_config_{idx}", use_container_width=True):
                                with st.spinner("Saving changes to Unity Catalog..."):
                                    success, message = write_table_data_sql(databricks_host, databricks_token, warehouse_id, table_name, edited_df)
                                    if success:
                                        st.success(message)
                                        time.sleep(1)
                                        st.rerun()
                                    else:
                                        st.error(message)
                    else:
                        st.warning("No data to display")

    # HISTORY TAB
    elif st.session_state.pbi_active_tab == 'history':
        st.markdown('<h1 style="color: white;">Power BI Conversion History</h1>', unsafe_allow_html=True)
        st.markdown('<p>View past dashboard conversions from this session</p>', unsafe_allow_html=True)
        
        pbi_history = [h for h in st.session_state.run_history if h.get('source') == 'powerbi']
        
        if pbi_history:
            st.markdown(f"### Total Runs: {len(pbi_history)}")
            st.table(pd.DataFrame(pbi_history))
        else:
            st.info("No Power BI conversion history available in this session.")
    
    # DISCOVERY TAB
    elif st.session_state.pbi_active_tab == 'discovery':
        st.markdown('<h1 style="color: white;">Power BI Discovery Details</h1>', unsafe_allow_html=True)
        st.markdown('<p>Analyze report complexity and visual details</p>', unsafe_allow_html=True)

        if not warehouse_id:
            st.warning("Warehouse ID not configured in config.json")
        elif not pbi_discovery_job_id:
            st.warning("Power BI discovery job ID not configured in config.json")
            st.info("Please add 'pbi_discovery_job_id' to your config.json file")
        else:
            col1, col2, col3 = st.columns([1, 1, 1])
            with col2:
                start_discovery_button = st.button("Start Discovery", type="primary", use_container_width=True, key="start_pbi_discovery")
            
            st.markdown("---")
            
            st.markdown("### Report Complexity Summary")
            st.markdown("---")
            
            with st.spinner("Loading complexity summary..."):
                df_complexity = get_table_data_simple(
                    databricks_host, 
                    databricks_token, 
                    warehouse_id, 
                    "dbx_migration_poc.dbx_migration_pbi.report_complexity_summary"
                )
            
            if not df_complexity.empty:
                score_col = next((col for col in df_complexity.columns if 'score' in col.lower() or 'complexity' in col.lower()), None)
                
                if score_col and pd.api.types.is_numeric_dtype(df_complexity[score_col]):
                    avg_score = df_complexity[score_col].mean()
                    if avg_score > 0:
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.markdown(f'<div class="metric-card"><div class="metric-label">Average Complexity</div><div class="metric-value">{avg_score:.2f}</div></div>', unsafe_allow_html=True)
                        with col2:
                            st.markdown(f'<div class="metric-card"><div class="metric-label">Total Reports</div><div class="metric-value">{len(df_complexity)}</div></div>', unsafe_allow_html=True)
                        with col3:
                            max_score = df_complexity[score_col].max()
                            st.markdown(f'<div class="metric-card"><div class="metric-label">Max Complexity</div><div class="metric-value">{max_score:.2f}</div></div>', unsafe_allow_html=True)
                
                st.dataframe(df_complexity, use_container_width=True, height=500)
                st.caption(f"Total records: {len(df_complexity)}")
            else:
                st.warning("No complexity summary data available.")

    # Process Power BI Discovery Job
    if st.session_state.pbi_active_tab == 'discovery' and pbi_discovery_job_id:
        
        if 'start_discovery_button' in locals() and start_discovery_button:
            try:
                with st.spinner("Initiating Power BI discovery..."):
                    run_id = trigger_job(databricks_host, databricks_token, pbi_discovery_job_id, None)
                    st.session_state.pbi_discovery_run_id = run_id
                    st.session_state.pbi_discovery_start_time = datetime.now()
                    st.markdown(f'<div class="message-box message-success">Power BI discovery started. Run ID: {run_id}</div>', unsafe_allow_html=True)
                    time.sleep(1)
                    st.rerun()
            except Exception as e:
                st.markdown(f'<div class="message-box message-error">Error starting discovery: {str(e)}</div>', unsafe_allow_html=True)

        if st.session_state.get("pbi_discovery_run_id"):
            try:
                run_id = st.session_state.pbi_discovery_run_id
                run_status = get_run_status(databricks_host, databricks_token, run_id)
                state = run_status.get("state", {})
                life_cycle_state = state.get("life_cycle_state", "UNKNOWN")
                result_state = state.get("result_state", "")
                tasks = run_status.get("tasks", [])
            
                st.markdown("---")
                st.markdown("### Discovery Execution Status")
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.markdown(f'<div class="metric-card"><div class="metric-label">Run ID</div><div class="metric-value">{run_id}</div></div>', unsafe_allow_html=True)
                with col2:
                    status_display = result_state if result_state else life_cycle_state
                    status_class = "status-success" if result_state == "SUCCESS" else "status-error" if result_state in ["FAILED", "TIMEDOUT"] else "status-running"
                    st.markdown(f'<div class="metric-card"><div class="metric-label">Status</div><div class="metric-value {status_class}">{status_display}</div></div>', unsafe_allow_html=True)
                with col3:
                    elapsed = 0
                    if life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                        if run_status.get("start_time") and run_status.get("end_time"):
                            elapsed = int((run_status["end_time"] - run_status["start_time"]) / 1000)
                        elif st.session_state.get("pbi_discovery_start_time"):
                            elapsed = (datetime.now() - st.session_state.pbi_discovery_start_time).seconds
                    else:
                        if st.session_state.get("pbi_discovery_start_time"):
                            elapsed = (datetime.now() - st.session_state.pbi_discovery_start_time).seconds
                    st.markdown(f'<div class="metric-card"><div class="metric-label">Elapsed Time</div><div class="metric-value">{elapsed}s</div></div>', unsafe_allow_html=True)
                with col4:
                    url = run_status.get("run_page_url", "")
                    if url:
                        st.markdown(f'<div class="metric-card"><div class="metric-label">Details</div><div class="metric-value"><a href="{url}" target="_blank" style="color: #667eea;">View Logs</a></div></div>', unsafe_allow_html=True)
            
                if life_cycle_state == "TERMINATED":
                    if result_state == "SUCCESS":
                        st.markdown('<div class="message-box message-success">Discovery completed successfully. Refresh the tables above to see updated data.</div>', unsafe_allow_html=True)
                    elif result_state in ["FAILED", "TIMEDOUT"]:
                        st.markdown(f'<div class="message-box message-error">Discovery failed with status: {result_state}</div>', unsafe_allow_html=True)
                
                st.markdown("### Task Execution")
                for task in tasks:
                    task_state = task.get("state", {})
                    task_name = task.get("task_key", "Unknown")
                    task_result = task_state.get("result_state", "")
                    symbol = "[SUCCESS]" if task_result == "SUCCESS" else "[FAILED]" if task_result in ["FAILED", "TIMEDOUT"] else "[PENDING]"
                    with st.expander(f"{symbol} {task_name}", expanded=False):
                        st.write(f"**Status:** {task_result if task_result else task_state.get('life_cycle_state', 'UNKNOWN')}")
                        if task.get("start_time") and task.get("end_time"):
                            st.write(f"**Duration:** {(task['end_time'] - task['start_time']) / 1000:.2f}s")
            
                if auto_refresh and life_cycle_state in ["PENDING", "RUNNING"]:
                    time.sleep(config.get("refresh_interval_seconds", 5))
                    st.rerun()
            except Exception as e:
                st.markdown(f'<div class="message-box message-error">Error: {str(e)}</div>', unsafe_allow_html=True)

    # Process Power BI Conversion Jobs (Data & Visual)
    if st.session_state.pbi_active_tab == 'conversion':
        
        # --- PBI VISUAL JOB HANDLER ---
        if pbi_visual_job_id:
            if 'start_pbi_visual_button' in locals() and start_pbi_visual_button:
                try:
                    with st.spinner("Initiating Power BI Visual conversion..."):
                        run_id = trigger_job(databricks_host, databricks_token, pbi_visual_job_id, None)
                        st.session_state.pbi_visual_run_id = run_id
                        st.session_state.pbi_visual_start_time = datetime.now()
                        st.session_state.run_history.append({
                            'source': 'powerbi', 'run_id': run_id, 'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'status': 'Started (Visual)'
                        })
                        st.markdown(f'<div class="message-box message-success">PBI Visual conversion started. Run ID: {run_id}</div>', unsafe_allow_html=True)
                        time.sleep(1)
                        st.rerun()
                except Exception as e:
                    st.markdown(f'<div class="message-box message-error">Error starting visual conversion: {str(e)}</div>', unsafe_allow_html=True)

            if st.session_state.get("pbi_visual_run_id"):
                with pbi_conv_tab2:
                    try:
                        run_id = st.session_state.pbi_visual_run_id
                        run_status = get_run_status(databricks_host, databricks_token, run_id)
                        state = run_status.get("state", {})
                        life_cycle_state = state.get("life_cycle_state", "UNKNOWN")
                        result_state = state.get("result_state", "")
                        tasks = run_status.get("tasks", [])
                        
                        st.markdown("---")
                        pv_out1, pv_out2 = st.tabs(["Dashboard Output", "Execution Details"])
                        
                        with pv_out1:
                            if life_cycle_state == "TERMINATED":
                                dashboard_found = False
                                for task in reversed(tasks):
                                    if task.get("run_id"):
                                        try:
                                            task_output = get_run_output(databricks_host, databricks_token, task["run_id"])
                                            notebook_output = task_output.get("notebook_output", {})
                                            result_str = notebook_output.get("result")
                                            if result_str:
                                                result_data = json.loads(result_str)
                                                if "dashboard_id" in result_data or "dashboard_url" in result_data:
                                                    st.markdown('<div class="dashboard-card"><div class="dashboard-title">Conversion Complete</div>', unsafe_allow_html=True)
                                                    c1, c2, c3 = st.columns(3)
                                                    with c1: st.markdown(f'<div class="dashboard-item"><div class="dashboard-label">ID</div><div class="dashboard-value">{result_data.get("dashboard_id", "N/A")}</div></div>', unsafe_allow_html=True)
                                                    with c2: st.markdown(f'<div class="dashboard-item"><div class="dashboard-label">Name</div><div class="dashboard-value">{result_data.get("dashboard_name", "N/A")}</div></div>', unsafe_allow_html=True)
                                                    with c3:
                                                        url = result_data.get("dashboard_url", "")
                                                        link = f'<a href="{url}" target="_blank" class="dashboard-link">Open</a>' if url else "N/A"
                                                        st.markdown(f'<div class="dashboard-item"><div class="dashboard-label">Link</div><div class="dashboard-value">{link}</div></div>', unsafe_allow_html=True)
                                                    st.markdown('</div>', unsafe_allow_html=True)
                                                    dashboard_found = True
                                                    break
                                        except: continue
                                if not dashboard_found:
                                    if result_state == "SUCCESS": 
                                        st.markdown('<div class="message-box message-info">Job succeeded, but no dashboard output details found.</div>', unsafe_allow_html=True)
                                    else: 
                                        st.markdown(f'<div class="message-box message-error">Job failed: {result_state}</div>', unsafe_allow_html=True)
                                
                                # -----------------------------------------------------------
                                # MOVED: Conversion Tracker Table to Dashboard Output Tab
                                # -----------------------------------------------------------
                                st.markdown("### Conversion Tracker Details")
                                with st.spinner("Fetching tracker results..."):
                                    tracker_df = get_conversion_tracker_data(
                                        databricks_host, 
                                        databricks_token, 
                                        warehouse_id, 
                                        run_id
                                    )
                                    if not tracker_df.empty:
                                        st.dataframe(tracker_df, use_container_width=True)
                                    else:
                                        st.info("No tracking data available for this run.")

                            else:
                                st.markdown('<div class="message-box message-info">Visual Conversion in progress...</div>', unsafe_allow_html=True)
                        
                        with pv_out2:
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.markdown(f'<div class="metric-card"><div class="metric-label">Run ID</div><div class="metric-value">{run_id}</div></div>', unsafe_allow_html=True)
                            with col2:
                                status_display = result_state if result_state else life_cycle_state
                                status_class = "status-success" if result_state == "SUCCESS" else "status-error" if result_state in ["FAILED", "TIMEDOUT"] else "status-running"
                                st.markdown(f'<div class="metric-card"><div class="metric-label">Status</div><div class="metric-value {status_class}">{status_display}</div></div>', unsafe_allow_html=True)
                            with col3:
                                elapsed = 0
                                if life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                                    if run_status.get("start_time") and run_status.get("end_time"):
                                        elapsed = int((run_status["end_time"] - run_status["start_time"]) / 1000)
                                    elif st.session_state.get("pbi_visual_start_time"):
                                        elapsed = (datetime.now() - st.session_state.pbi_visual_start_time).seconds
                                else:
                                    if st.session_state.get("pbi_visual_start_time"):
                                        elapsed = (datetime.now() - st.session_state.pbi_visual_start_time).seconds
                                st.markdown(f'<div class="metric-card"><div class="metric-label">Elapsed Time</div><div class="metric-value">{elapsed}s</div></div>',   unsafe_allow_html=True)
                            with col4:
                                url = run_status.get("run_page_url", "")
                                if url:
                                    st.markdown(f'<div class="metric-card"><div class="metric-label">Details</div><div class="metric-value"><a href="{url}" target="_blank" style="color: #667eea;">View Logs</a></div></div>', unsafe_allow_html=True)

                            st.markdown("### Task Execution")
                            for task in tasks:
                                task_state = task.get("state", {})
                                task_name = task.get("task_key", "Unknown")
                                task_result = task_state.get("result_state", "")
                                symbol = "[SUCCESS]" if task_result == "SUCCESS" else "[FAILED]" if task_result in ["FAILED", "TIMEDOUT"] else "[PENDING]"
                                with st.expander(f"{symbol} {task_name}", expanded=False):
                                    st.write(f"**Status:** {task_result if task_result else task_state.get('life_cycle_state', 'UNKNOWN')}")
                                    if task.get("start_time") and task.get("end_time"):
                                        st.write(f"**Duration:** {(task['end_time'] - task['start_time']) / 1000:.2f}s")

                        if auto_refresh and life_cycle_state in ["PENDING", "RUNNING"]:
                            time.sleep(config.get("refresh_interval_seconds", 5))
                            st.rerun()
                    except Exception as e:
                        st.markdown(f'<div class="message-box message-error">Error checking status: {str(e)}</div>', unsafe_allow_html=True)

        # --- PBI DATA JOB HANDLER ---
        if pbi_data_job_id:
            if 'start_pbi_data_button' in locals() and start_pbi_data_button:
                try:
                    with st.spinner("Initiating Power BI Data conversion..."):
                        run_id = trigger_job(databricks_host, databricks_token, pbi_data_job_id, None)
                        st.session_state.pbi_data_run_id = run_id
                        st.session_state.pbi_data_start_time = datetime.now()
                        st.session_state.run_history.append({
                            'source': 'powerbi', 'run_id': run_id, 'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'status': 'Started (Data)'
                        })
                        st.markdown(f'<div class="message-box message-success">PBI Data conversion started. Run ID: {run_id}</div>', unsafe_allow_html=True)
                        time.sleep(1)
                        st.rerun()
                except Exception as e:
                    st.markdown(f'<div class="message-box message-error">Error starting data conversion: {str(e)}</div>', unsafe_allow_html=True)

            if st.session_state.get("pbi_data_run_id"):
                with pbi_conv_tab1:
                    try:
                        run_id = st.session_state.pbi_data_run_id
                        run_status = get_run_status(databricks_host, databricks_token, run_id)
                        state = run_status.get("state", {})
                        life_cycle_state = state.get("life_cycle_state", "UNKNOWN")
                        result_state = state.get("result_state", "")
                        tasks = run_status.get("tasks", [])
                        
                        st.markdown("---")
                        pd_out1, pd_out2 = st.tabs(["SQL Output", "Execution Details"])
                        
                        with pd_out1:
                            if life_cycle_state == "TERMINATED":
                                if result_state == "SUCCESS":
                                    st.markdown('<div class="message-box message-success">Data conversion completed successfully.</div>', unsafe_allow_html=True)
                                    
                                    task = tasks[-1] if tasks else None
                                    if task and task.get("run_id"):
                                        try:
                                            task_output = get_run_output(databricks_host, databricks_token, task["run_id"])
                                            notebook_result_raw = task_output.get("notebook_output", {}).get("result")
                                            
                                            if notebook_result_raw:
                                                notebook_data = json.loads(notebook_result_raw)
                                                generated_query = notebook_data.get("Query", "No query returned")
                                                generated_filepath = notebook_data.get("Filepath", "")
                                                
                                                st.markdown('<div class="dashboard-card"><div class="dashboard-title">Generated SQL</div>', unsafe_allow_html=True)
                                                
                                                st.markdown("##### Final SQL Query")
                                                st.code(generated_query, language="sql")
                                                
                                                if generated_filepath:
                                                    st.markdown(f'<div class="message-box message-info">File Path: <strong>{generated_filepath}</strong></div>', unsafe_allow_html=True)
                                                    
                                                    with st.spinner("Reading generated file..."):
                                                        content, error = read_volume_file(databricks_host, databricks_token, generated_filepath)
                                                        
                                                        if error:
                                                            st.error(error)
                                                        else:
                                                            with st.expander("View File Content", expanded=True):
                                                                st.text(content)
                                                                st.download_button(
                                                                    label="Download SQL File",
                                                                    data=content,
                                                                    file_name=os.path.basename(generated_filepath),
                                                                    mime="application/sql"
                                                                )
                                                st.markdown('</div>', unsafe_allow_html=True)
                                                
                                            else:
                                                st.warning("Job finished but no notebook output was returned.")
                                                
                                        except Exception as e:
                                            st.error(f"Error parsing job output: {str(e)}")
                                
                                # EXPLICIT FAILURE HANDLING
                                elif result_state in ["FAILED", "TIMEDOUT", "CANCELED"]:
                                    st.markdown(f'<div class="message-box message-error">Data conversion failed. Status: {result_state}</div>', unsafe_allow_html=True)
                                    if state.get("state_message"):
                                        st.error(f"Error Details: {state.get('state_message')}")
                                else:
                                    st.markdown(f'<div class="message-box message-error">Job ended with unexpected status: {result_state}</div>', unsafe_allow_html=True)
                            else:
                                st.markdown(f'<div class="message-box message-info">Data conversion in progress... (State: {life_cycle_state})</div>', unsafe_allow_html=True)
                                st.progress(0.5)
                        
                        with pd_out2:
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.markdown(f'<div class="metric-card"><div class="metric-label">Run ID</div><div class="metric-value">{run_id}</div></div>', unsafe_allow_html=True)
                            with col2:
                                status_display = result_state if result_state else life_cycle_state
                                status_class = "status-success" if result_state == "SUCCESS" else "status-error" if result_state in ["FAILED", "TIMEDOUT"] else "status-running"
                                st.markdown(f'<div class="metric-card"><div class="metric-label">Status</div><div class="metric-value {status_class}">{status_display}</div></div>', unsafe_allow_html=True)
                            with col3:
                                elapsed = 0
                                if life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                                    if run_status.get("start_time") and run_status.get("end_time"):
                                        elapsed = int((run_status["end_time"] - run_status["start_time"]) / 1000)
                                    elif st.session_state.get("pbi_data_start_time"):
                                        elapsed = (datetime.now() - st.session_state.pbi_data_start_time).seconds
                                else:
                                    if st.session_state.get("pbi_data_start_time"):
                                        elapsed = (datetime.now() - st.session_state.pbi_data_start_time).seconds
                                st.markdown(f'<div class="metric-card"><div class="metric-label">Elapsed Time</div><div class="metric-value">{elapsed}s</div></div>',   unsafe_allow_html=True)
                            with col4:
                                url = run_status.get("run_page_url", "")
                                if url:
                                    st.markdown(f'<div class="metric-card"><div class="metric-label">Details</div><div class="metric-value"><a href="{url}" target="_blank" style="color: #667eea;">View Logs</a></div></div>', unsafe_allow_html=True)
                            
                            st.markdown("### Task Execution")
                            for task in tasks:
                                task_state = task.get("state", {})
                                task_name = task.get("task_key", "Unknown")
                                task_result = task_state.get("result_state", "")
                                symbol = "[SUCCESS]" if task_result == "SUCCESS" else "[FAILED]" if task_result in ["FAILED", "TIMEDOUT"] else "[PENDING]"
                                with st.expander(f"{symbol} {task_name}", expanded=False):
                                    st.write(f"**Status:** {task_result if task_result else task_state.get('life_cycle_state', 'UNKNOWN')}")
                                    if task.get("start_time") and task.get("end_time"):
                                        st.write(f"**Duration:** {(task['end_time'] - task['start_time']) / 1000:.2f}s")

                        if auto_refresh and life_cycle_state in ["PENDING", "RUNNING"]:
                            time.sleep(config.get("refresh_interval_seconds", 5))
                            st.rerun()
                    except Exception as e:
                        st.markdown(f'<div class="message-box message-error">Error checking status: {str(e)}</div>', unsafe_allow_html=True)

# ============================================================================
# TABLEAU MIGRATION PANEL
# ============================================================================
elif st.session_state.main_panel == 'tableau':
    
    # CONVERSION TAB
    if st.session_state.tableau_active_tab == 'conversion':
        st.markdown('<h1 style="color: white; text-align: center; margin: 0; padding: 0.5rem 0;">Tableau to Databricks</h1>', unsafe_allow_html=True)
        st.markdown('<p style="color: rgba(255,255,255,0.9); text-align: center; margin: 0 0 1.5rem 0; font-size: 0.95rem;">Workbook Migration</p>', unsafe_allow_html=True)
        
        # New Tab structure for Tableau
        tab_conv_tab1, tab_conv_tab2, tab_conv_tab3 = st.tabs(["Data Conversion", "Visual Conversion", "Migration Validator"])

        # TABLEAU DATA CONVERSION
        with tab_conv_tab1:
            st.markdown('<p style="margin: 1rem 0;">Convert Tableau Data Sources (TDS/Extracts) to Databricks SQL</p>', unsafe_allow_html=True)
            if not tableau_data_job_id:
                st.warning("Tableau Data conversion job ID not configured (tableau_data_job_id)")
            else:
                col1, col2, col3 = st.columns([1, 1, 1])
                with col2:
                    start_tableau_data_button = st.button("Start Data Conversion", type="primary", use_container_width=True, key="start_tableau_data_conv")

        # TABLEAU VISUAL CONVERSION
        with tab_conv_tab2:
            st.markdown('<p style="margin: 1rem 0;">Convert Tableau Workbooks (TWB/TWBX) to Databricks AI/BI</p>', unsafe_allow_html=True)
            if not tableau_visual_job_id:
                st.warning("Tableau Visual conversion job ID not configured (tableau_visual_job_id)")
            else:
                col1, col2, col3 = st.columns([1, 1, 1])
                with col2:
                    start_tableau_visual_button = st.button("Start Visual Conversion", type="primary", use_container_width=True, key="start_tableau_visual_conv")

        # TABLEAU VALIDATOR (Placeholder)
        with tab_conv_tab3:
            st.markdown('<div class="coming-soon-screen"><div class="coming-soon-title">Tableau Validator</div><div class="coming-soon-text">Visual regression testing and automated validation for Tableau migrations is currently under development.</div><div class="coming-soon-badge">Coming Soon</div></div>', unsafe_allow_html=True)

    # CONFIG MANAGER TAB
    elif st.session_state.tableau_active_tab == 'config':
        st.markdown('<h1 style="color: white;">Tableau Config Manager</h1>', unsafe_allow_html=True)
        st.markdown('<p>View and manage configuration tables</p>', unsafe_allow_html=True)
        
        if not warehouse_id:
            st.warning("Warehouse ID not configured in config.json")
        else:
            st.info(f"Using warehouse: {warehouse_id}")
            
            # NOTE: Assuming Table names here. Please ensure these exist in your Unity Catalog
            config_tables = [
                ("Chart Type Mappings", "dbx_migration_poc.dbx_migration_tableau.chart_type_mappings"),
                ("Calculated Fields", "dbx_migration_poc.dbx_migration_tableau.calculated_field_transformations"),
                ("Widget Size Config", "dbx_migration_poc.dbx_migration_tableau.widget_size_config"),
                ("VizQL to SQL Mapping", "dbx_migration_poc.dbx_migration_tableau.vizql_to_sql_mapping")
            ]
            
            tabs = st.tabs([name for name, _ in config_tables])
            
            for idx, (tab_name, table_name) in enumerate(config_tables):
                with tabs[idx]:
                    st.markdown(f"### {tab_name}")
                    
                    col1, col2 = st.columns([3, 1])
                    with col1:
                        st.markdown("Edit the configuration below and click Save to update the table.")
                    with col2:
                        if st.button("Refresh", key=f"refresh_tableau_{idx}"):
                            st.rerun()
                    
                    df = get_table_data_for_config(databricks_host, databricks_token, warehouse_id, table_name)
                    
                    if not df.empty:
                        edited_df = st.data_editor(
                            df, 
                            use_container_width=True, 
                            height=400,
                            num_rows="dynamic",
                            key=f"tableau_config_editor_{idx}"
                        )
                        
                        col1, col2, col3 = st.columns([1, 1, 1])
                        with col2:
                            if st.button("Save Changes", type="primary", key=f"save_tableau_config_{idx}", use_container_width=True):
                                with st.spinner("Saving changes to Unity Catalog..."):
                                    success, message = write_table_data_sql(databricks_host, databricks_token, warehouse_id, table_name, edited_df)
                                    if success:
                                        st.success(message)
                                        time.sleep(1)
                                        st.rerun()
                                    else:
                                        st.error(message)
                    else:
                        st.warning(f"No data found or table does not exist: {table_name}")

    # HISTORY TAB
    elif st.session_state.tableau_active_tab == 'history':
        st.markdown('<h1 style="color: white;">Tableau Conversion History</h1>', unsafe_allow_html=True)
        st.markdown('<p>View past workbook conversions from this session</p>', unsafe_allow_html=True)
        
        tableau_history = [h for h in st.session_state.run_history if h.get('source') == 'tableau']
        
        if tableau_history:
            st.markdown(f"### Total Runs: {len(tableau_history)}")
            st.table(pd.DataFrame(tableau_history))
        else:
            st.info("No Tableau conversion history available in this session.")

    # Process Tableau Conversion Jobs (Data & Visual)
    if st.session_state.tableau_active_tab == 'conversion':

        # --- TABLEAU VISUAL JOB HANDLER ---
        if tableau_visual_job_id:
            if 'start_tableau_visual_button' in locals() and start_tableau_visual_button:
                try:
                    with st.spinner("Initiating Tableau Visual conversion..."):
                        run_id = trigger_job(databricks_host, databricks_token, tableau_visual_job_id, None)
                        st.session_state.tableau_visual_run_id = run_id
                        st.session_state.tableau_visual_start_time = datetime.now()
                        st.session_state.run_history.append({
                            'source': 'tableau', 'run_id': run_id, 'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'status': 'Started (Visual)'
                        })
                        st.markdown(f'<div class="message-box message-success">Tableau Visual conversion started. Run ID: {run_id}</div>', unsafe_allow_html=True)
                        time.sleep(1)
                        st.rerun()
                except Exception as e:
                    st.markdown(f'<div class="message-box message-error">Error starting visual conversion: {str(e)}</div>', unsafe_allow_html=True)

            if st.session_state.get("tableau_visual_run_id"):
                with tab_conv_tab2:
                    try:
                        run_id = st.session_state.tableau_visual_run_id
                        run_status = get_run_status(databricks_host, databricks_token, run_id)
                        state = run_status.get("state", {})
                        life_cycle_state = state.get("life_cycle_state", "UNKNOWN")
                        result_state = state.get("result_state", "")
                        tasks = run_status.get("tasks", [])
                        
                        st.markdown("---")
                        tv_out1, tv_out2 = st.tabs(["Dashboard Output", "Execution Details"])
                        
                        with tv_out1:
                            if life_cycle_state == "TERMINATED":
                                dashboard_found = False
                                for task in reversed(tasks):
                                    if task.get("run_id"):
                                        try:
                                            task_output = get_run_output(databricks_host, databricks_token, task["run_id"])
                                            notebook_output = task_output.get("notebook_output", {})
                                            result_str = notebook_output.get("result")
                                            if result_str:
                                                result_data = json.loads(result_str)
                                                if "dashboard_id" in result_data or "dashboard_url" in result_data:
                                                    st.markdown('<div class="dashboard-card"><div class="dashboard-title">Conversion Complete</div>', unsafe_allow_html=True)
                                                    c1, c2, c3 = st.columns(3)
                                                    with c1: st.markdown(f'<div class="dashboard-item"><div class="dashboard-label">ID</div><div class="dashboard-value">{result_data.get("dashboard_id", "N/A")}</div></div>', unsafe_allow_html=True)
                                                    with c2: st.markdown(f'<div class="dashboard-item"><div class="dashboard-label">Name</div><div class="dashboard-value">{result_data.get("dashboard_name", "N/A")}</div></div>', unsafe_allow_html=True)
                                                    with c3:
                                                        url = result_data.get("dashboard_url", "")
                                                        link = f'<a href="{url}" target="_blank" class="dashboard-link">Open</a>' if url else "N/A"
                                                        st.markdown(f'<div class="dashboard-item"><div class="dashboard-label">Link</div><div class="dashboard-value">{link}</div></div>', unsafe_allow_html=True)
                                                    st.markdown('</div>', unsafe_allow_html=True)
                                                    dashboard_found = True
                                                    break
                                        except: continue
                                if not dashboard_found:
                                    if result_state == "SUCCESS": 
                                        st.markdown('<div class="message-box message-info">Job succeeded, but no dashboard output details found.</div>', unsafe_allow_html=True)
                                    else: 
                                        st.markdown(f'<div class="message-box message-error">Job failed: {result_state}</div>', unsafe_allow_html=True)
                            else:
                                st.markdown('<div class="message-box message-info">Visual Conversion in progress...</div>', unsafe_allow_html=True)
                        
                        with tv_out2:
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.markdown(f'<div class="metric-card"><div class="metric-label">Run ID</div><div class="metric-value">{run_id}</div></div>', unsafe_allow_html=True)
                            with col2:
                                status_display = result_state if result_state else life_cycle_state
                                status_class = "status-success" if result_state == "SUCCESS" else "status-error" if result_state in ["FAILED", "TIMEDOUT"] else "status-running"
                                st.markdown(f'<div class="metric-card"><div class="metric-label">Status</div><div class="metric-value {status_class}">{status_display}</div></div>', unsafe_allow_html=True)
                            with col3:
                                elapsed = 0
                                if life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                                    if run_status.get("start_time") and run_status.get("end_time"):
                                        elapsed = int((run_status["end_time"] - run_status["start_time"]) / 1000)
                                    elif st.session_state.get("tableau_visual_start_time"):
                                        elapsed = (datetime.now() - st.session_state.tableau_visual_start_time).seconds
                                else:
                                    if st.session_state.get("tableau_visual_start_time"):
                                        elapsed = (datetime.now() - st.session_state.tableau_visual_start_time).seconds
                                st.markdown(f'<div class="metric-card"><div class="metric-label">Elapsed Time</div><div class="metric-value">{elapsed}s</div></div>', unsafe_allow_html=True)
                            with col4:
                                url = run_status.get("run_page_url", "")
                                if url:
                                    st.markdown(f'<div class="metric-card"><div class="metric-label">Details</div><div class="metric-value"><a href="{url}" target="_blank" style="color: #667eea;">View Logs</a></div></div>', unsafe_allow_html=True)
                            
                            st.markdown("### Task Execution")
                            for task in tasks:
                                task_state = task.get("state", {})
                                task_name = task.get("task_key", "Unknown")
                                task_result = task_state.get("result_state", "")
                                symbol = "[SUCCESS]" if task_result == "SUCCESS" else "[FAILED]" if task_result in ["FAILED", "TIMEDOUT"] else "[PENDING]"
                                with st.expander(f"{symbol} {task_name}", expanded=False):
                                    st.write(f"**Status:** {task_result if task_result else task_state.get('life_cycle_state', 'UNKNOWN')}")
                                    if task.get("start_time") and task.get("end_time"):
                                        st.write(f"**Duration:** {(task['end_time'] - task['start_time']) / 1000:.2f}s")

                        if auto_refresh and life_cycle_state in ["PENDING", "RUNNING"]:
                            time.sleep(config.get("refresh_interval_seconds", 5))
                            st.rerun()
                    except Exception as e:
                        st.markdown(f'<div class="message-box message-error">Error checking status: {str(e)}</div>', unsafe_allow_html=True)

        # --- TABLEAU DATA JOB HANDLER ---
        if tableau_data_job_id:
            if 'start_tableau_data_button' in locals() and start_tableau_data_button:
                try:
                    with st.spinner("Initiating Tableau Data conversion..."):
                        run_id = trigger_job(databricks_host, databricks_token, tableau_data_job_id, None)
                        st.session_state.tableau_data_run_id = run_id
                        st.session_state.tableau_data_start_time = datetime.now()
                        st.session_state.run_history.append({
                            'source': 'tableau', 'run_id': run_id, 'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'status': 'Started (Data)'
                        })
                        st.markdown(f'<div class="message-box message-success">Tableau Data conversion started. Run ID: {run_id}</div>', unsafe_allow_html=True)
                        time.sleep(1)
                        st.rerun()
                except Exception as e:
                    st.markdown(f'<div class="message-box message-error">Error starting data conversion: {str(e)}</div>', unsafe_allow_html=True)

            if st.session_state.get("tableau_data_run_id"):
                with tab_conv_tab1:
                    try:
                        run_id = st.session_state.tableau_data_run_id
                        run_status = get_run_status(databricks_host, databricks_token, run_id)
                        state = run_status.get("state", {})
                        life_cycle_state = state.get("life_cycle_state", "UNKNOWN")
                        result_state = state.get("result_state", "")
                        tasks = run_status.get("tasks", [])
                        
                        st.markdown("---")
                        td_out1, td_out2 = st.tabs(["SQL Output", "Execution Details"])
                        
                        with td_out1:
                            if life_cycle_state == "TERMINATED":
                                if result_state == "SUCCESS":
                                    st.markdown('<div class="message-box message-success">Data conversion completed successfully.</div>', unsafe_allow_html=True)
                                    
                                    task = tasks[-1] if tasks else None
                                    if task and task.get("run_id"):
                                        try:
                                            task_output = get_run_output(databricks_host, databricks_token, task["run_id"])
                                            notebook_result_raw = task_output.get("notebook_output", {}).get("result")
                                            
                                            if notebook_result_raw:
                                                notebook_data = json.loads(notebook_result_raw)
                                                generated_query = notebook_data.get("Query", "No query returned")
                                                generated_filepath = notebook_data.get("Filepath", "")
                                                
                                                st.markdown('<div class="dashboard-card"><div class="dashboard-title">Generated SQL</div>', unsafe_allow_html=True)
                                                
                                                st.markdown("##### Final SQL Query")
                                                st.code(generated_query, language="sql")
                                                
                                                if generated_filepath:
                                                    st.markdown(f'<div class="message-box message-info">File Path: <strong>{generated_filepath}</strong></div>', unsafe_allow_html=True)
                                                    
                                                    with st.spinner("Reading generated file..."):
                                                        content, error = read_volume_file(databricks_host, databricks_token, generated_filepath)
                                                        
                                                        if error:
                                                            st.error(error)
                                                        else:
                                                            with st.expander("View File Content", expanded=True):
                                                                st.text(content)
                                                                st.download_button(
                                                                    label="Download SQL File",
                                                                    data=content,
                                                                    file_name=os.path.basename(generated_filepath),
                                                                    mime="application/sql"
                                                                )
                                                st.markdown('</div>', unsafe_allow_html=True)
                                                
                                            else:
                                                st.warning("Job finished but no notebook output was returned.")
                                                
                                        except Exception as e:
                                            st.error(f"Error parsing job output: {str(e)}")
                                
                                # EXPLICIT FAILURE HANDLING
                                elif result_state in ["FAILED", "TIMEDOUT", "CANCELED"]:
                                    st.markdown(f'<div class="message-box message-error">Data conversion failed. Status: {result_state}</div>', unsafe_allow_html=True)
                                    if state.get("state_message"):
                                        st.error(f"Error Details: {state.get('state_message')}")
                                else:
                                    st.markdown(f'<div class="message-box message-error">Job ended with unexpected status: {result_state}</div>', unsafe_allow_html=True)
                            else:
                                st.markdown(f'<div class="message-box message-info">Data conversion in progress... (State: {life_cycle_state})</div>', unsafe_allow_html=True)
                                st.progress(0.5)
                        
                        with td_out2:
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.markdown(f'<div class="metric-card"><div class="metric-label">Run ID</div><div class="metric-value">{run_id}</div></div>', unsafe_allow_html=True)
                            with col2:
                                status_display = result_state if result_state else life_cycle_state
                                status_class = "status-success" if result_state == "SUCCESS" else "status-error" if result_state in ["FAILED", "TIMEDOUT"] else "status-running"
                                st.markdown(f'<div class="metric-card"><div class="metric-label">Status</div><div class="metric-value {status_class}">{status_display}</div></div>', unsafe_allow_html=True)
                            with col3:
                                elapsed = 0
                                if life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                                    if run_status.get("start_time") and run_status.get("end_time"):
                                        elapsed = int((run_status["end_time"] - run_status["start_time"]) / 1000)
                                    elif st.session_state.get("tableau_data_start_time"):
                                        elapsed = (datetime.now() - st.session_state.tableau_data_start_time).seconds
                                else:
                                    if st.session_state.get("tableau_data_start_time"):
                                        elapsed = (datetime.now() - st.session_state.tableau_data_start_time).seconds
                                st.markdown(f'<div class="metric-card"><div class="metric-label">Elapsed Time</div><div class="metric-value">{elapsed}s</div></div>', unsafe_allow_html=True)
                            with col4:
                                url = run_status.get("run_page_url", "")
                                if url:
                                    st.markdown(f'<div class="metric-card"><div class="metric-label">Details</div><div class="metric-value"><a href="{url}" target="_blank" style="color: #667eea;">View Logs</a></div></div>', unsafe_allow_html=True)
                            
                            st.markdown("### Task Execution")
                            for task in tasks:
                                task_state = task.get("state", {})
                                task_name = task.get("task_key", "Unknown")
                                task_result = task_state.get("result_state", "")
                                symbol = "[SUCCESS]" if task_result == "SUCCESS" else "[FAILED]" if task_result in ["FAILED", "TIMEDOUT"] else "[PENDING]"
                                with st.expander(f"{symbol} {task_name}", expanded=False):
                                    st.write(f"**Status:** {task_result if task_result else task_state.get('life_cycle_state', 'UNKNOWN')}")
                                    if task.get("start_time") and task.get("end_time"):
                                        st.write(f"**Duration:** {(task['end_time'] - task['start_time']) / 1000:.2f}s")

                        if auto_refresh and life_cycle_state in ["PENDING", "RUNNING"]:
                            time.sleep(config.get("refresh_interval_seconds", 5))
                            st.rerun()
                    except Exception as e:
                        st.markdown(f'<div class="message-box message-error">Error checking status: {str(e)}</div>', unsafe_allow_html=True)