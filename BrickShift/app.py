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
import extra_streamlit_components as stx
# Import the utils from the prototype
sys.path.append(os.path.join(os.path.dirname(__file__), '..')) # Adjust based on folder structure
# Initialize modules to None to prevent NameError later
powerbi_export = None
vision_engine = None
ThoughtSpotPDFExport = None
try:
    from utils import powerbi_export
    from utils import vision_engine
    from utils import ThoughtSpotPDFExport
except ImportError as e:
    # Log the specific error to the console/driver logs so you can fix the root cause (e.g., missing library)
    print(f"⚠️ Warning: Modules failed to import. Details: {e}")
    # We pass here so the app still loads, but modules will be None
    pass
# Page configuration
st.set_page_config(
    page_title="BrickShift - Dashboard Converter",
    layout="wide",
    initial_sidebar_state="expanded"
)
# Material Design 3 Theme — BrickShift Dashboard
st.markdown("""
<style>
    /* ── Google Fonts ── */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');
    @import url('https://fonts.googleapis.com/icon?family=Material+Icons');

    /* ── M3 Design Tokens ── */
    :root {
        --primary: #1565C0; --primary-hover: #0D47A1; --primary-container: #D1E4FF;
        --on-primary: #FFFFFF; --on-primary-container: #001D36;
        --secondary: #535F70; --secondary-container: #D7E3F7; --on-secondary-container: #101C2B;
        --tertiary: #6B5778;
        --surface: #F7F9FB; --surface-container-lowest: #FFFFFF; --surface-container-low: #F1F3F5;
        --surface-container: #EBEEF0; --surface-container-high: #E5E8EA;
        --on-surface: #1A1C1E; --on-surface-variant: #44474E;
        --outline: #74777F; --outline-variant: #C4C6CF;
        --success: #1B7D3A; --success-container: #B2F1BF;
        --error: #BA1A1A; --error-container: #FFDAD6;
        --warning: #7D5700; --warning-container: #FFDDB3;
        --sidebar-bg: #0E1525; --sidebar-bg-high: #182034; --sidebar-text: #D8DEE9;
        --sidebar-muted: #7E8A9E; --sidebar-accent: #90CAF9; --sidebar-border: #2A3548;
        --elevation-1: 0 1px 2px rgba(0,0,0,0.06), 0 1px 3px rgba(0,0,0,0.1);
        --elevation-2: 0 2px 4px rgba(0,0,0,0.06), 0 4px 6px rgba(0,0,0,0.08);
        --radius-xs: 4px; --radius-sm: 8px; --radius-md: 12px; --radius-lg: 16px; --radius-full: 999px;
    }

    /* ── Global Reset & Layout Fixes ── */
    .stApp { background-color: var(--surface) !important; font-family: 'Inter', sans-serif !important; color: var(--on-surface) !important; }
    
    /* 1. Remove the black bar by making the Streamlit header transparent */
    [data-testid="stHeader"] { background: transparent !important; }
    
    /* Optional: Hide the default deploy button for a cleaner look */
    .stDeployButton { display: none !important; }
    #MainMenu {visibility: hidden;}

    /* 2. Move the content up to act as a true top header */
    .main .block-container { 
        padding-top: 0rem !important; /* Forces the content to the absolute top edge */
        padding-bottom: 4rem !important; 
        max-width: 1200px; 
    }
    
    /* Ensure no hidden Streamlit elements push the content down */
    div[data-testid="stVerticalBlock"] > div:first-child {
        padding-top: 0 !important;
        margin-top: 0 !important;
    }
    
    /* ── Sidebar & Navigation ── */
    [data-testid="stSidebar"] { 
        background-color: #0F172A !important; /* Deep Slate 900 */
        border-right: 1px solid #1E293B !important; 
    }
    
    [data-testid="stSidebar"] .block-container { padding-top: 1.5rem !important; }
    
    [data-testid="stSidebar"] p, [data-testid="stSidebar"] div, [data-testid="stSidebar"] span { 
        color: #F8FAFC !important; 
    }

    /* Sidebar Section Labels */
    [data-testid="stSidebar"] h3 {
        font-size: 11px !important;
        font-weight: 700 !important;
        color: #64748B !important; /* Slate 500 */
        text-transform: uppercase !important;
        letter-spacing: 0.08em !important;
        padding: 24px 12px 12px !important;
        margin: 0 !important;
    }

    /* Base Sidebar Nav Buttons (Inactive) */
    /* Target both generic button and secondary button specifically to override global borders */
    [data-testid="stSidebar"] .stButton > button,
    [data-testid="stSidebar"] .stButton > button[kind="secondary"] {
        background: transparent !important; 
        border: none !important; /* Removes the default white border */
        border-radius: 999px !important; 
        padding: 10px 16px !important; 
        font-size: 14px !important; 
        font-weight: 500 !important;
        text-align: left !important; 
        justify-content: flex-start !important; /* Aligns to left */
        color: #E2E8F0 !important; 
        transition: all var(--duration-fast) var(--ease) !important;
        box-shadow: none !important;
    }

    /* ── Reduce Gap Between Sidebar Buttons ── */
    [data-testid="stSidebar"] div[data-testid="stVerticalBlock"] {
        gap: 0.2rem !important; /* Reduces the default 1rem Streamlit gap */
    }
    
    /* Ensure the wrapper doesn't add hidden margins */
    [data-testid="stSidebar"] div.stButton {
        margin-bottom: 0 !important;
        padding-bottom: 0 !important;
    }
    
    /* Force inner Streamlit text/icon containers to align left uniformly */
    [data-testid="stSidebar"] .stButton > button div,
    [data-testid="stSidebar"] .stButton > button p {
        display: flex !important;
        justify-content: flex-start !important;
        text-align: left !important;
        width: 100% !important;
        margin: 0 !important;
    }

    [data-testid="stSidebar"] .stButton > button:hover,
    [data-testid="stSidebar"] .stButton > button[kind="secondary"]:hover { 
        background: rgba(255, 255, 255, 0.05) !important; 
        color: #FFFFFF !important; 
    }
    
    [data-testid="stSidebar"] .stButton > button:hover * { color: #FFFFFF !important; }

    /* Active Sidebar Nav Button (Mapped to Primary type) */
    [data-testid="stSidebar"] .stButton > button[kind="primary"] {
        background: #1E293B !important; 
        color: #FFFFFF !important;
        font-weight: 600 !important;
        border: none !important; /* Ensure no border on active either */
    }
    
    /* BUGFIX: Forces the text and icon to be white for the active tab */
    [data-testid="stSidebar"] .stButton > button[kind="primary"] p,
    [data-testid="stSidebar"] .stButton > button[kind="primary"] span,
    [data-testid="stSidebar"] .stButton > button[kind="primary"] div { 
        color: #FFFFFF !important; 
    }

    /* ── Ask Genie Button (Natural Bottom Docking) ── */
    
    /* Target the button's outer Streamlit container to push it down and add a separator */
    .st-key-genie_trigger_btn {
        margin-top: 3rem !important; /* Pushes the button down from the nav links */
        padding-top: 1.5rem !important; /* Adds breathing room inside */
        border-top: 1px solid #1E293B !important; /* Separator line */
    }
    
    /* Style the actual button to match the mockup */
    .st-key-genie_trigger_btn > button {
        background: transparent !important;
        border: 1px solid #334155 !important; 
        color: #BAE6FD !important; 
        font-weight: 600 !important;
        border-radius: 999px !important;
        justify-content: center !important; /* Centers icon and text */
        padding: 12px !important;
    }
    
    .st-key-genie_trigger_btn > button:hover { 
        background: rgba(59, 130, 246, 0.1) !important; 
        border-color: #3B82F6 !important;
        color: #93C5FD !important;
    }
    
    .st-key-genie_trigger_btn > button p, 
    .st-key-genie_trigger_btn > button span { 
        color: inherit !important; 
    }
    
    /* ── Streamlit Buttons & Overrides ── */
    .stButton > button { font-family: 'Inter', sans-serif !important; font-weight: 600 !important; border-radius: var(--radius-full) !important; }
    .stButton > button[kind="primary"] { background: var(--primary) !important; color: var(--on-primary) !important; border: none !important; box-shadow: var(--elevation-1) !important; }
    .stButton > button[kind="secondary"] { background: transparent !important; color: var(--on-surface-variant) !important; border: 1px solid var(--outline-variant) !important; }
    .stButton > button[kind="secondary"]:hover { background: rgba(0,0,0,0.04) !important; }

    /* ── Segmented Platform Selector ── */
    .st-key-select_thoughtspot .stButton > button, .st-key-select_powerbi .stButton > button, .st-key-select_tableau .stButton > button {
        font-size: 13.5px !important; padding: 10px 12px !important; border: none !important;
    }
    .st-key-select_thoughtspot .stButton > button[kind="primary"], .st-key-select_powerbi .stButton > button[kind="primary"], .st-key-select_tableau .stButton > button[kind="primary"] {
        background: var(--primary) !important; color: var(--on-primary) !important; box-shadow: var(--elevation-1) !important;
    }

    /* ── Custom HTML Components (From Preview) ── */

    /* ── Conversion Complete Result Card ── */
    .result-card {
        background: var(--surface-container-lowest);
        border-radius: var(--radius-lg);
        box-shadow: var(--elevation-2);
        overflow: hidden;
        margin-bottom: 20px;
        border: 1px solid var(--outline-variant);
    }
    .result-bar {
        height: 4px;
        background: linear-gradient(90deg, var(--success), #4CAF50);
    }
    .result-head {
        display: flex; align-items: center; gap: 10px;
        padding: 16px 20px 12px;
    }
    .result-icon {
        width: 30px; height: 30px;
        border-radius: var(--radius-full);
        background: var(--success-container);
        display: grid; place-items: center;
        flex-shrink: 0;
    }
    .result-icon .material-icons { font-size: 17px; color: var(--success); }
    .result-grid {
        display: grid; grid-template-columns: repeat(3, 1fr);
        gap: 12px; padding: 0 20px 20px;
    }
    .result-cell {
        background: var(--surface-container-low);
        border: 1px solid var(--outline-variant);
        border-radius: var(--radius-md);
        padding: 16px 12px; text-align: center;
    }
    .result-cell-label {
        font-size: 10px; font-weight: 700;
        color: var(--on-surface-variant);
        text-transform: uppercase; letter-spacing: 0.08em;
        margin-bottom: 6px;
    }
    .result-cell-val {
        font-size: 14px; font-weight: 600; color: var(--on-surface);
    }
    .result-cell-val a { color: var(--primary); text-decoration: none; }
    .result-cell-val a:hover { text-decoration: underline; }
    
    .chip-success { display: inline-flex; align-items: center; gap: 5px; padding: 4px 12px; border-radius: var(--radius-sm); font-size: 11.5px; font-weight: 600; background: var(--success-container); color: var(--success); }
    
    .metric-card { background: var(--surface-container-lowest); border: 1px solid var(--outline-variant); border-radius: var(--radius-md); padding: 14px 16px; text-align: center; }
    .metric-label { font-size: 10px; font-weight: 700; color: var(--on-surface-variant); text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 4px; }
    .metric-value { font-size: 20px; font-weight: 700; color: var(--on-surface); }
    
    .task { display: flex; align-items: center; gap: 10px; padding: 10px 14px; background: var(--surface-container-lowest); border: 1px solid var(--outline-variant); border-radius: var(--radius-md); margin-bottom: 4px; }
    .task-ico { width: 26px; height: 26px; border-radius: var(--radius-full); display: grid; place-items: center; flex-shrink: 0; }
    .task-ico.ok { background: var(--success-container); color: var(--success); }
    .task-ico.fail { background: var(--error-container); color: var(--error); }
    .task-ico.wait { background: var(--warning-container); color: var(--warning); }
    .task-name { flex: 1; font-family: 'SF Mono', monospace; font-size: 12.5px; color: var(--on-surface); }
    .badge-ok { font-size: 10px; font-weight: 700; padding: 2px 10px; border-radius: var(--radius-full); text-transform: uppercase; background: var(--success-container); color: var(--success); }
    .st-key-main_navigation_buttons_container {
    background-color: #f0f0f0; /* Light gray background */
    border: 1px solid rgb(156, 156, 156); /* Green border */
    border-radius: 20px; /* Rounded corners */
    padding: 2px; /* Inner spacing */
    margin-bottom: 5px; /* Spacing below the container */
    }
    /* 1. Main Card Container */
    .st-key-my_blue_container {
        background: var(--surface-container-lowest) !important;
        border: 1px solid var(--outline-variant) !important;
        border-radius: var(--radius-lg) !important;
        box-shadow: var(--elevation-1) !important;
        overflow: hidden !important;
        position: relative !important;
        padding: 1.5rem !important; /* Adds breathing room inside the card */
        margin-top: 1rem !important;
    }

    /* 2. The Animated Blue Stripe at the Top */
    .st-key-my_blue_container::before {
        content: "";
        position: absolute;
        top: 0; 
        left: 0; 
        right: 0;
        height: 3px;
        background: linear-gradient(90deg, var(--primary), #42A5F5, var(--primary));
        background-size: 200% 100%;
        animation: barShift 4s ease-in-out infinite;
    }

    /* 3. The Animation Keyframes (Required for the movement effect) */
    @keyframes barShift {
        0%, 100% { background-position: 0% 50%; }
        50% { background-position: 100% 50%; }
    }
    /* ── Full-Width Evenly Distributed Tabs ── */
    [data-testid="stTabs"] [data-baseweb="tab-list"] {
        display: flex !important;
        width: 100% !important;
        gap: 0 !important; /* Removes default gap so they touch edge-to-edge */
    }
    
    [data-testid="stTabs"] button[data-baseweb="tab"] {
        flex: 1 1 0px !important; /* Forces all tabs to share the width equally */
        display: flex !important;
        justify-content: center !important; /* Centers the text and icon horizontally */
        padding-top: 12px !important;
        padding-bottom: 12px !important;
        margin: 0 !important;
    }
    /* ── Execution Results Header (From Preview) ── */
    .section-title { 
        display: flex; align-items: center; gap: 10px; 
        margin-bottom: 14px; margin-top: 32px; 
    }
    .section-title::before { 
        content: ""; width: 3px; height: 18px; border-radius: 2px; 
        background: linear-gradient(180deg, var(--primary), #42A5F5); flex-shrink: 0; 
    }
    .section-title h3 { 
        font-size: 14px !important; font-weight: 700 !important; 
        color: var(--on-surface) !important; letter-spacing: -0.01em !important; margin: 0 !important; 
    }
    /* ── Validator Panels ── */
    .st-key-val_panel_export, .st-key-val_panel_validate,
    .st-key-ts_val_panel_export, .st-key-ts_val_panel_validate {
        background-color: var(--surface-container-low) !important;
        border: 1px solid var(--outline-variant) !important;
        border-radius: var(--radius-md) !important;
        padding: 20px !important;
    }
    
    /* ── Purple Run Comparison Button ── */
    .st-key-val_run_btn > button, .st-key-ts_val_run_btn > button {
        background-color: var(--tertiary) !important; /* Plum/Purple color */
        color: var(--on-primary) !important;
        border: none !important;
    }
    .st-key-val_run_btn > button:hover, .st-key-ts_val_run_btn > button:hover {
        background-color: #55445F !important; /* Darker purple on hover */
        box-shadow: var(--elevation-2) !important;
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
    st.info("**Model:** Databricks Claude Opus")
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
        with st.container(key="val_panel_export"):
            # Custom HTML Headers
            st.markdown('<div style="margin-bottom: 12px;"><div style="font-size: 14px; font-weight: 700; color: #1A1C1E; margin-bottom: 4px;">1. Source Snapshot</div><div style="font-size: 12px; color: #44474E;">Export the original dashboard for comparison</div></div>', unsafe_allow_html=True)
            
            # Input with label collapsed to save space
            pbi_url = st.text_input("PBI URL Hidden", placeholder="https://app.powerbi.com/...", key="val_input_url", label_visibility="collapsed")
            
            if st.button("Start Export", type="primary", use_container_width=True, key="val_export_btn"):
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
                st.markdown(f"""
                <div style="margin-top: 14px; padding: 10px; border: 1px solid #334155; border-radius: 5px; background: #1e293b;">
                    <div style="font-size: 0.8em; color: #94a3b8;">READY FOR ANALYSIS</div>
                    <div style="color: #f8fafc;">📄 {res['filename']}</div>
                </div>
                """, unsafe_allow_html=True)

    # --- COLUMN 2: VALIDATE ---
    with col_validate:
        with st.container(key="val_panel_validate"):
            # Custom HTML Headers
            st.markdown('<div style="margin-bottom: 12px;"><div style="font-size: 14px; font-weight: 700; color: #1A1C1E; margin-bottom: 4px;">2. AI Validation</div><div style="font-size: 12px; color: #44474E;">Upload target PDF for pixel-level comparison</div></div>', unsafe_allow_html=True)
            
            if 'export_result' not in st.session_state:
                st.info("Complete Step 1 first.")
            else:
                source_path = st.session_state['export_result']['file_path']
                session_id = st.session_state['export_result']['session_id']
                
                # Uploader with label collapsed 
                target_pdf = st.file_uploader("Upload PDF Hidden", type=["pdf"], key="val_upload", label_visibility="collapsed")
                
                if st.button("Run Comparison", type="primary", use_container_width=True, key="val_run_btn", icon=":material/smart_toy:"):
                    if not target_pdf:
                        st.warning("Upload target PDF.")
                    else:
                        os.makedirs("exports", exist_ok=True)
                        target_path = os.path.join("exports", target_pdf.name)
                        with open(target_path, "wb") as f: f.write(target_pdf.getbuffer())
                        
                        try:
                            with st.spinner("Analyzing pixels & data..."):
                                source_b64 = vision_engine.encode_pdf_to_base64(source_path)
                                target_b64 = vision_engine.encode_pdf_to_base64(target_path)
                                
                                s1 = vision_engine.run_stage1_analysis(source_b64, target_b64, ENDPOINT_OPUS, DB_TOKEN, "PowerBI", "Claude Opus")
                                if s1["status"] == "error": raise Exception(s1["error"])
                                
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
        # Wrap the button in a div with a custom class/id
        st.markdown('<div class="download-report-container">', unsafe_allow_html=True)
        st.download_button(
            label="📥 Download PDF Report",
            data=pdf_bytes,
            file_name="Migration_Report.pdf",
            mime="application/pdf",
            use_container_width=True
        )
        st.markdown('</div>', unsafe_allow_html=True)
        with st.expander("Executive Summary", expanded=True):
            st.markdown(report['executive_summary'])
        with st.expander("Detailed Findings"):
            st.markdown(report['detailed_analysis'])
def render_thoughtspot_validator_section():
    """
    Renders the Validator UI logic specifically for ThoughtSpot.
    """
    
    # --- REQUIREMENT: Model Information ---
    st.info("**Model:** Databricks Claude Opus")
    st.caption("Automated Visual Regression Testing: ThoughtSpot vs Databricks")
    
    # --- CONFIGURATION ---
    config = load_config()
    
    # ThoughtSpot Specific Credentials
    TS_BASE_URL = config.get("thoughtspot_url", "https://my-thoughtspot-instance.com")
    TS_USERNAME = os.getenv("TS_CLIENT_USERNAME")
    TS_PASSWORD = os.getenv("TS_CLIENT_PWD")
    
    # Databricks Config for writing metadata
    DB_HOST = normalize_host_url(config.get("databricks_host", ""))
    DB_TOKEN = os.getenv("DATABRICKS_TOKEN")
    WAREHOUSE_ID = config.get("warehouse_id", "")
    METADATA_TABLE = "dbx_migration_poc.migrationvalidation.thoughtspot_pdf_exports_metadata"
    ENDPOINT_OPUS = config.get("endpoint_opus", "https://adb-3666479212731434.14.azuredatabricks.net/serving-endpoints/databricks-claude-opus-4-6/invocations")
    ENDPOINT_HAIKU = config.get("endpoint_haiku", "https://adb-3666479212731434.14.azuredatabricks.net/serving-endpoints/databricks-claude-haiku-4-5/invocations")
    col_export, col_validate = st.columns(2)
    
    # --- COLUMN 1: EXPORT ---
    with col_export:
        with st.container(key="ts_val_panel_export"):
            st.markdown('<div style="margin-bottom: 12px;"><div style="font-size: 14px; font-weight: 700; color: #1A1C1E; margin-bottom: 4px;">1. Source Snapshot</div><div style="font-size: 12px; color: #44474E;">Export the original dashboard for comparison</div></div>', unsafe_allow_html=True)
            
            ts_url = st.text_input("TS URL Hidden", placeholder="https://thoughtspot.com/#/pinboard/...", key="ts_val_input_url", label_visibility="collapsed")
            
            if st.button("Start Export", type="primary", use_container_width=True, key="ts_val_export_btn"):
                if ThoughtSpotPDFExport is None:
                    st.error("The 'ThoughtSpotPDFExport' module failed to load. Check imports/logs.")
                elif not ts_url:
                    st.warning("Please provide a Liveboard URL.")
                elif not all([TS_USERNAME, TS_PASSWORD]):
                    st.error("ThoughtSpot credentials (TS_USERNAME, TS_PASSWORD) missing in environment.")
                else:
                    with st.spinner("Snapshotting Liveboard..."):
                        try:
                            result = ThoughtSpotPDFExport.export_from_url(ts_url, TS_USERNAME, TS_PASSWORD)
                            if result:
                                try:
                                    df_log = pd.DataFrame([result])
                                    write_table_data_sql(DB_HOST, DB_TOKEN, WAREHOUSE_ID, METADATA_TABLE, df_log)
                                except Exception as db_e:
                                    st.warning(f"Export succeeded, but failed to log to DB: {db_e}")
                            
                            if result.get("export_status") == "success":
                                st.session_state['ts_export_result'] = result
                                st.success("Export Complete!")
                            else:
                                st.error(f"Error: {result.get('error')}")
                        except Exception as e:
                            st.error(f"Critical: {str(e)}")
                            
            if 'ts_export_result' in st.session_state:
                res = st.session_state['ts_export_result']
                st.markdown(f"""
                <div style="margin-top: 14px; padding: 10px; border: 1px solid #334155; border-radius: 5px; background: #1e293b;">
                    <div style="font-size: 0.8em; color: #94a3b8;">READY FOR ANALYSIS</div>
                    <div style="color: #f8fafc;">📄 {res.get('filename', 'export.pdf')}</div>
                </div>
                """, unsafe_allow_html=True)

    # --- COLUMN 2: VALIDATE ---
    with col_validate:
        with st.container(key="ts_val_panel_validate"):
            st.markdown('<div style="margin-bottom: 12px;"><div style="font-size: 14px; font-weight: 700; color: #1A1C1E; margin-bottom: 4px;">2. AI Validation</div><div style="font-size: 12px; color: #44474E;">Upload target PDF for pixel-level comparison</div></div>', unsafe_allow_html=True)
            
            if 'ts_export_result' not in st.session_state:
                st.info("Complete Step 1 first.")
            else:
                source_path = st.session_state['ts_export_result']['file_path']
                session_id = st.session_state['ts_export_result'].get('session_id', str(uuid.uuid4()))
                
                target_pdf = st.file_uploader("Upload Target PDF Hidden", type=["pdf"], key="ts_val_upload", label_visibility="collapsed")
                
                if st.button("Run Comparison", type="primary", use_container_width=True, key="ts_val_run_btn"):
                    if not target_pdf:
                        st.warning("Upload target PDF.")
                    else:
                        os.makedirs("exports", exist_ok=True)
                        target_path = os.path.join("exports", f"ts_target_{session_id}.pdf")
                        with open(target_path, "wb") as f: f.write(target_pdf.getbuffer())
                        
                        try:
                            with st.spinner("Analyzing pixels & data..."):
                                source_b64 = vision_engine.encode_pdf_to_base64(source_path)
                                target_b64 = vision_engine.encode_pdf_to_base64(target_path)
                                
                                s1 = vision_engine.run_stage1_analysis(source_b64, target_b64, ENDPOINT_OPUS, DB_TOKEN, "ThoughtSpot", "Claude Opus")
                                if s1["status"] == "error": raise Exception(s1["error"])
                                
                                s2 = vision_engine.run_stage2_summarization(s1["detailed_analysis"], ENDPOINT_HAIKU, DB_TOKEN, "Claude Haiku")
                                
                                final_result = {
                                    "session_id": session_id,
                                    "detailed_analysis": s1["detailed_analysis"],
                                    "executive_summary": s2["summary"]
                                }
                                st.session_state['ts_current_report'] = final_result
                                st.success("Validation Complete!")
                        except Exception as e:
                            st.error(f"Validation Failed: {str(e)}")
    st.markdown("---")
    # --- REPORTING SECTION ---
    if 'ts_current_report' in st.session_state:
        report = st.session_state['ts_current_report']
        st.markdown("### 📊 Analysis Report")
        
        pdf_bytes = generate_report_pdf(report['executive_summary'], report['detailed_analysis'], report['session_id'])
        st.download_button(
            label="📥 Download PDF Report",
            data=pdf_bytes,
            file_name=f"TS_Migration_Report_{report['session_id'][:8]}.pdf",
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
# Use 3 columns and vertically align them so everything sits perfectly centered
header_col1, header_col2, header_col3 = st.columns([5.5, 1.2, 1.5], vertical_alignment="center")

with header_col1:
    st.markdown("""
    <div style="display: flex; align-items: center; gap: 14px;">
        <div style="width: 40px; height: 40px; border-radius: 12px; background: linear-gradient(135deg, #1565C0, #42A5F5); display: grid; place-items: center; box-shadow: 0 2px 4px rgba(0,0,0,0.06), 0 4px 6px rgba(0,0,0,0.08); flex-shrink: 0;">
            <span style="font-size: 22px; color: white;">&#x2B21;</span>
        </div>
        <div>
            <div style="font-family: 'Inter', sans-serif; font-size: 22px; font-weight: 800; letter-spacing: -0.03em; line-height: 1.15;">
                <span style="background: linear-gradient(135deg, #1565C0, #42A5F5); -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text;">BrickShift</span>
            </div>
            <div style="font-family: 'Inter', sans-serif; font-size: 12.5px; color: #44474E; margin-top: 1px;">
                Intelligent Dashboard Migration to Databricks AI/BI
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

with header_col2:
    st.markdown("""
    <div style="display: flex; justify-content: flex-end; align-items: center;">
        <span class="chip-success">
            <span class="material-icons" style="font-size: 13px;">radio_button_checked</span> Connected
        </span>
    </div>
    """, unsafe_allow_html=True)
    
with header_col3:
    test_job_id = None
    if st.session_state.main_panel == 'thoughtspot':
        test_job_id = ts_visual_job_id if ts_visual_job_id else ts_data_job_id
    elif st.session_state.main_panel == 'powerbi':
        test_job_id = pbi_visual_job_id if pbi_visual_job_id else (pbi_data_job_id if pbi_data_job_id else pbi_discovery_job_id)
    elif st.session_state.main_panel == 'tableau':
        test_job_id = tableau_visual_job_id if tableau_visual_job_id else tableau_data_job_id
    
    if test_job_id:
        if st.button("Test Connection", type="secondary", key="global_test_conn", icon=":material/cable:", use_container_width=True):
            with st.spinner("Testing..."):
                success, message = test_connection(databricks_host, databricks_token, test_job_id)
                if success:
                    st.toast("Connection successful", icon="✅")
                else:
                    st.toast("Connection failed", icon="❌")

# Add a full-width divider under the completed 3-column header
st.markdown('<hr style="margin: 1.2rem 0 1.5rem 0; border: none; height: 1px; background-color: #C4C6CF;" />', unsafe_allow_html=True)


# Panel selection buttons with active state styling
with st.container(key="main_navigation_buttons_container"):
    col1, col2, col3 = st.columns(3)

with col1:
    ts_button_type = "primary" if st.session_state.main_panel == 'thoughtspot' else "secondary"
    if st.button("ThoughtSpot", key="select_thoughtspot", type=ts_button_type, use_container_width=True, icon=":material/analytics:"):
        st.session_state.main_panel = 'thoughtspot'
        st.rerun()
with col2:
    pbi_button_type = "primary" if st.session_state.main_panel == 'powerbi' else "secondary"
    if st.button("Power BI", key="select_powerbi", type=pbi_button_type, use_container_width=True, icon=":material/bar_chart:"):
        st.session_state.main_panel = 'powerbi'
        st.rerun()
with col3:
    tableau_button_type = "primary" if st.session_state.main_panel == 'tableau' else "secondary"
    if st.button("Tableau", key="select_tableau", type=tableau_button_type, use_container_width=True, icon=":material/monitoring:"):
        st.session_state.main_panel = 'tableau'
        st.rerun()
st.markdown("---")
# ============================================================================
# SIDEBAR NAVIGATION
# ============================================================================
with st.sidebar:
    st.markdown("""
    <div style="display: flex; align-items: center; gap: 12px; padding: 0px 8px 16px; border-bottom: 1px solid #1E293B; margin-bottom: 0.5rem;">
        <div style="width: 32px; height: 32px; border-radius: 8px; background: #2563EB; display: grid; place-items: center; flex-shrink: 0;">
            <span style="font-size: 18px; color: white;">&#x2B21;</span>
        </div>
        <div>
            <div style="font-size: 16px; font-weight: 700; color: #F8FAFC; letter-spacing: -0.01em; line-height: 1.2; font-family: 'Inter', sans-serif;">BrickShift</div>
            <div style="font-size: 12px; color: #94A3B8; line-height: 1.3; font-family: 'Inter', sans-serif;">Migration Console</div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # ---------------------------------------
    # Regular Navigation Buttons
    # ---------------------------------------
    if st.session_state.main_panel == 'thoughtspot':
        st.markdown("### ThoughtSpot Migration")
        if st.button("Conversion", key="ts_tab_conversion", use_container_width=True,icon=":material/conversion_path:",icon_position="left"):
            st.session_state.ts_active_tab = 'conversion'
        if st.button("Config Manager", key="ts_tab_config", use_container_width=True,icon=":material/settings:",icon_position="left"):
            st.session_state.ts_active_tab = 'config'
        if st.button("History", key="ts_tab_history", use_container_width=True,icon=":material/history:",icon_position="left"):
            st.session_state.ts_active_tab = 'history'
        if st.button("Discovery Details", key="ts_tab_discovery", use_container_width=True,icon=":material/radar:",icon_position="left"):
            st.session_state.ts_active_tab = 'discovery'
    
    elif st.session_state.main_panel == 'powerbi':
        st.markdown("### Power BI Migration")
        if st.button("Conversion", key="pbi_tab_conversion", use_container_width=True,icon=":material/conversion_path:",icon_position="left"):
            st.session_state.pbi_active_tab = 'conversion'
        if st.button("Config Manager", key="pbi_tab_config", use_container_width=True,icon=":material/settings:",icon_position="left"):
            st.session_state.pbi_active_tab = 'config'
        if st.button("History", key="pbi_tab_history", use_container_width=True,icon=":material/history:",icon_position="left"):
            st.session_state.pbi_active_tab = 'history'
        if st.button("Discovery Details", key="pbi_tab_discovery", use_container_width=True,icon=":material/radar:",icon_position="left"):
            st.session_state.pbi_active_tab = 'discovery'
    elif st.session_state.main_panel == 'tableau':
        st.markdown("### Tableau Migration")
        if st.button("Conversion", key="tableau_tab_conversion", use_container_width=True,icon=":material/conversion_path:",icon_position="left"):
            st.session_state.tableau_active_tab = 'conversion'
        if st.button("Config Manager", key="tableau_tab_config", use_container_width=True,icon=":material/settings:",icon_position="left"):
            st.session_state.tableau_active_tab = 'config'
        if st.button("History", key="tableau_tab_history", use_container_width=True,icon=":material/history:",icon_position="left"):
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
    
    if st.button("Ask Genie", key="genie_trigger_btn", type="secondary", use_container_width=True, icon=":material/smart_toy:"):
        open_genie_chat()
# ============================================================================
# THOUGHTSPOT MIGRATION PANEL
# ============================================================================
if st.session_state.main_panel == 'thoughtspot':
    
    # CONVERSION TAB
    if st.session_state.ts_active_tab == 'conversion':
        with st.container(key="my_blue_container"):
            st.markdown('<h1 style="color: rgb(51, 65, 85); text-align: left; margin: 0; padding: 0.5rem 0;">ThoughtSpot to Databricks</h1>', unsafe_allow_html=True)
            st.markdown('<p style="color: rgb(51, 65, 85); text-align: left; margin: 0 0 1.5rem 0; font-size: 0.95rem;">Dashboard Migration</p>', unsafe_allow_html=True)
        
        # UPDATED: Added Validator Tab
            conversion_tab1, conversion_tab2, conversion_tab3 = st.tabs(["Data Conversion", "Visual Conversion", "Migration Validator"],width="stretch")
            #conversion_tab1, conversion_tab2, conversion_tab3 = stx.tab_bar(data=[stx.TabBarItemData(id=1, title="Data Conversion", description="Tasks to take care of"),stx.TabBarItemData(id=2, title="Visual Conversion", description="Tasks taken care of"),stx.TabBarItemData(id=3, title="Migration Validator", description="Tasks missed out"),],default=1)
        
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
            # Removed the "Coming Soon" placeholder
            st.markdown('<p style="margin: 1rem 0;">Validate migration accuracy using Databricks AI</p>', unsafe_allow_html=True)
            
            # Call the new ThoughtSpot Validator function
            render_thoughtspot_validator_section()
                
    # CONFIG MANAGER TAB
    elif st.session_state.ts_active_tab == 'config':
        st.markdown('<h1 style="color: rgb(51, 65, 85);">ThoughtSpot Config Manager</h1>', unsafe_allow_html=True)
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
        st.markdown('<h1 style="color: rgb(51, 65, 85);">ThoughtSpot Conversion History</h1>', unsafe_allow_html=True)
        st.markdown('<p>View past dashboard conversions from this session</p>', unsafe_allow_html=True)
        
        ts_history = [h for h in st.session_state.run_history if h.get('source') == 'thoughtspot']
        
        if ts_history:
            st.markdown(f"### Total Runs: {len(ts_history)}")
            st.table(pd.DataFrame(ts_history))
        else:
            st.info("No ThoughtSpot conversion history available in this session.")
    
    # DISCOVERY TAB
    elif st.session_state.ts_active_tab == 'discovery':
        st.markdown('<h1 style="color: rgb(51, 65, 85);">ThoughtSpot Discovery Details</h1>', unsafe_allow_html=True)
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
                st.markdown('<div class="section-title"><h3>Execution Results</h3></div>', unsafe_allow_html=True)
                try:
                    run_id = st.session_state.ts_visual_run_id
                    run_status = get_run_status(databricks_host, databricks_token, run_id)
                    state = run_status.get("state", {})
                    life_cycle_state = state.get("life_cycle_state", "UNKNOWN")
                    result_state = state.get("result_state", "")
                    tasks = run_status.get("tasks", [])
                
                    visual_tab_out1, visual_tab_out2 = st.tabs(["Dashboard Output", "Execution Details"])
                
                    with visual_tab_out1:
                        if life_cycle_state == "TERMINATED":
                            dashboard_found = False
                            
                            for task in reversed(tasks):
                                if task.get("run_id"):
                                    try:
                                            task_output = get_run_output(databricks_host, databricks_token, task["run_id"])
                                            notebook_output = task_output.get("notebook_output", {})
                                            result_str = notebook_output.get("result")
                                            
                                            if result_str:
                                                try:
                                                    result_data = json.loads(result_str)
                                                    
                                                    if "dashboard_id" in result_data or "dashboard_url" in result_data:
                                                        dash_id = result_data.get("dashboard_id", "N/A")
                                                        dash_name = result_data.get("dashboard_name", "N/A")
                                                        url = result_data.get("dashboard_url", "")
                                                        link_html = f'<a href="{url}" target="_blank">Open Dashboard &rarr;</a>' if url else "Not Available"
                                                        
                                                        st.markdown(f"""
                                                        <div class="result-card">
                                                            <div class="result-bar"></div>
                                                            <div class="result-head">
                                                                <div class="result-icon"><span class="material-icons">check_circle</span></div>
                                                                <h4 style="margin:0; font-size:15px; font-weight:700; color: var(--on-surface);">Conversion Complete</h4>
                                                            </div>
                                                            <div class="result-grid">
                                                                <div class="result-cell">
                                                                    <div class="result-cell-label">DASHBOARD ID</div>
                                                                    <div class="result-cell-val">{dash_id}</div>
                                                                </div>
                                                                <div class="result-cell">
                                                                    <div class="result-cell-label">DASHBOARD NAME</div>
                                                                    <div class="result-cell-val">{dash_name}</div>
                                                                </div>
                                                                <div class="result-cell">
                                                                    <div class="result-cell-label">LINK</div>
                                                                    <div class="result-cell-val">{link_html}</div>
                                                                </div>
                                                            </div>
                                                        </div>
                                                        """, unsafe_allow_html=True)
                                                        
                                                        dashboard_found = True
                                                        break # Stop searching once found
                                                        
                                                except json.JSONDecodeError: continue
                                    except Exception as e: continue                            
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
                        with col3:
                            elapsed = 0
                            if life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                                if run_status.get("start_time") and run_status.get("end_time"):
                                    elapsed = int((run_status["end_time"] - run_status["start_time"]) / 1000)
                                elif st.session_state.get("ts_visual_start_time"):
                                    elapsed = (datetime.now() - st.session_state.ts_visual_start_time).seconds
                            else:
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

                            if task_result == "SUCCESS":
                                ico_class = "ok"
                                ico_mat = "check"
                                badge_class = "badge-ok"
                                badge_text = "Success"
                            elif task_result in ["FAILED", "TIMEDOUT"]:
                                ico_class = "fail"
                                ico_mat = "close"
                                badge_class = "badge-fail"
                                badge_text = "Failed"
                            else:
                                ico_class = "wait"
                                ico_mat = "schedule"
                                badge_class = "badge-wait"
                                badge_text = "Pending"

                            duration = "—"
                            if task.get("start_time") and task.get("end_time"):
                                duration = f"{(task['end_time'] - task['start_time']) / 1000:.1f}s"

                            st.markdown(f"""
                            <div class="task">
                                <div class="task-ico {ico_class}"><span class="material-icons" style="font-size: 15px;">{ico_mat}</span></div>
                                <span class="task-name">{task_name}</span>
                                <span class="task-dur">{duration}</span>
                                <span class="badge {badge_class}">{badge_text}</span>
                            </div>
                            """, unsafe_allow_html=True)
                
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
                st.markdown('<div class="section-title"><h3>Execution Results</h3></div>', unsafe_allow_html=True)
                try:
                    run_id = st.session_state.ts_data_run_id
                    run_status = get_run_status(databricks_host, databricks_token, run_id)
                    state = run_status.get("state", {})
                    life_cycle_state = state.get("life_cycle_state", "UNKNOWN")
                    result_state = state.get("result_state", "")
                    tasks = run_status.get("tasks", [])
                
                    data_tab_out1, data_tab_out2 = st.tabs(["SQL Output", "Execution Details"])
                
                    with data_tab_out1:
                        if life_cycle_state == "TERMINATED":
                            notebook_data = {}
                            task = tasks[-1] if tasks else None
                            
                            if task and task.get("run_id"):
                                try:
                                    task_output = get_run_output(databricks_host, databricks_token, task["run_id"])
                                    notebook_result_raw = task_output.get("notebook_output", {}).get("result")
                                    if notebook_result_raw:
                                        try:
                                            notebook_data = json.loads(notebook_result_raw)
                                        except json.JSONDecodeError:
                                            pass
                                except Exception as e:
                                    pass
                            
                            if "Error_Message" in notebook_data:
                                error_msg = notebook_data["Error_Message"]
                                st.markdown(f'''
                                    <div class="message-box message-error">
                                        <div style="font-weight: bold; font-size: 1.1em; margin-bottom: 0.5rem;">🛑 Validation Failure</div>
                                        {error_msg}
                                    </div>
                                ''', unsafe_allow_html=True)
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
                            elif result_state in ["FAILED", "TIMEDOUT", "CANCELED"]:
                                st.markdown(f'<div class="message-box message-error">Data conversion failed. Status: {result_state}</div>', unsafe_allow_html=True)
                                if state.get("state_message"):
                                    st.error(f"Error Details: {state.get('state_message')}")
                            else:
                                st.markdown(f'<div class="message-box message-error">Job ended with unexpected status: {result_state}</div>', unsafe_allow_html=True)
                        else:
                            st.markdown(f'<div class="message-box message-info">Data conversion in progress... (State: {life_cycle_state})</div>', unsafe_allow_html=True)
                            st.progress(0.5)
                
                    with data_tab_out2:
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
                            
                            if task_result == "SUCCESS":
                                ico_class = "ok"
                                ico_mat = "check"
                                badge_class = "badge-ok"
                                badge_text = "Success"
                            elif task_result in ["FAILED", "TIMEDOUT"]:
                                ico_class = "fail"
                                ico_mat = "close"
                                badge_class = "badge-fail"
                                badge_text = "Failed"
                            else:
                                ico_class = "wait"
                                ico_mat = "schedule"
                                badge_class = "badge-wait"
                                badge_text = "Pending"
                                
                            duration = "—"
                            if task.get("start_time") and task.get("end_time"):
                                duration = f"{(task['end_time'] - task['start_time']) / 1000:.1f}s"
                        
                            st.markdown(f"""
                            <div class="task">
                                <div class="task-ico {ico_class}"><span class="material-icons" style="font-size: 15px;">{ico_mat}</span></div>
                                <span class="task-name">{task_name}</span>
                                <span class="task-dur">{duration}</span>
                                <span class="badge {badge_class}">{badge_text}</span>
                            </div>
                            """, unsafe_allow_html=True)
                
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
        with st.container(key="my_blue_container"):
            st.markdown('<h1 style="color: rgb(51, 65, 85); text-align: left; margin: 0; padding: 0.5rem 0;">Power BI to Databricks</h1>', unsafe_allow_html=True)
            st.markdown('<p style="color: rgb(51, 65, 85); text-align: left; margin: 0 0 1.5rem 0; font-size: 0.95rem;">Dashboard Migration</p>', unsafe_allow_html=True)
        
            pbi_conv_tab1, pbi_conv_tab2, pbi_conv_tab3 = st.tabs(["Data Conversion", "Visual Conversion", "Migration Validator"])
            
            with pbi_conv_tab1:
                st.markdown('<p style="margin: 1rem 0;">Convert Power BI Data Models (DAX/M) to Databricks SQL</p>', unsafe_allow_html=True)
                if not pbi_data_job_id:
                    st.warning("Power BI Data conversion job ID not configured (pbi_data_job_id)")
                else:
                    col1, col2, col3 = st.columns([1, 1, 1])
                    with col2:
                        start_pbi_data_button = st.button("Start Data Conversion", type="primary", use_container_width=True, key="start_pbi_data_conv")
            
            with pbi_conv_tab2:
                st.markdown('<p style="margin: 1rem 0;">Convert Power BI Reports to Databricks AI/BI</p>', unsafe_allow_html=True)
                if not pbi_visual_job_id:
                    st.warning("Power BI Visual conversion job ID not configured (pbi_visual_job_id)")
                else:
                    col1, col2, col3 = st.columns([1, 1, 1])
                    with col2:
                        start_pbi_visual_button = st.button("Start Visual Conversion", type="primary", use_container_width=True, key="start_pbi_visual_conv")
            
            with pbi_conv_tab3:
                st.markdown('<p style="margin: 1rem 0;">Validate migration accuracy using Databricks AI</p>', unsafe_allow_html=True)
                render_validator_section()

    # CONFIG MANAGER TAB
    elif st.session_state.pbi_active_tab == 'config':
        st.markdown('<h1 style="color: rgb(51, 65, 85);">Power BI Config Manager</h1>', unsafe_allow_html=True)
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
                        edited_df = st.data_editor(df, use_container_width=True, height=400, num_rows="dynamic", key=f"pbi_config_editor_{idx}")
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
        st.markdown('<h1 style="color: rgb(51, 65, 85);">Power BI Conversion History</h1>', unsafe_allow_html=True)
        st.markdown('<p>View past dashboard conversions from this session</p>', unsafe_allow_html=True)
        pbi_history = [h for h in st.session_state.run_history if h.get('source') == 'powerbi']
        if pbi_history:
            st.markdown(f"### Total Runs: {len(pbi_history)}")
            st.table(pd.DataFrame(pbi_history))
        else:
            st.info("No Power BI conversion history available in this session.")
    
    # DISCOVERY TAB
    elif st.session_state.pbi_active_tab == 'discovery':
        st.markdown('<h1 style="color: rgb(51, 65, 85);">Power BI Discovery Details</h1>', unsafe_allow_html=True)
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
                df_complexity = get_table_data_simple(databricks_host, databricks_token, warehouse_id, "dbx_migration_poc.dbx_migration_pbi.report_complexity_summary")
            
            if not df_complexity.empty:
                score_col = next((col for col in df_complexity.columns if 'score' in col.lower() or 'complexity' in col.lower()), None)
                if score_col and pd.api.types.is_numeric_dtype(df_complexity[score_col]):
                    avg_score = df_complexity[score_col].mean()
                    if avg_score > 0:
                        col1, col2, col3 = st.columns(3)
                        with col1: st.markdown(f'<div class="metric-card"><div class="metric-label">Average Complexity</div><div class="metric-value">{avg_score:.2f}</div></div>', unsafe_allow_html=True)
                        with col2: st.markdown(f'<div class="metric-card"><div class="metric-label">Total Reports</div><div class="metric-value">{len(df_complexity)}</div></div>', unsafe_allow_html=True)
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
            st.markdown('<div class="section-title"><h3>Execution Results</h3></div>', unsafe_allow_html=True)
            try:
                run_id = st.session_state.pbi_discovery_run_id
                run_status = get_run_status(databricks_host, databricks_token, run_id)
                state = run_status.get("state", {})
                life_cycle_state = state.get("life_cycle_state", "UNKNOWN")
                result_state = state.get("result_state", "")
                tasks = run_status.get("tasks", [])
            
                col1, col2, col3, col4 = st.columns(4)
                with col1: st.markdown(f'<div class="metric-card"><div class="metric-label">Run ID</div><div class="metric-value">{run_id}</div></div>', unsafe_allow_html=True)
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
                    
                    if task_result == "SUCCESS":
                        ico_class = "ok"
                        ico_mat = "check"
                        badge_class = "badge-ok"
                        badge_text = "Success"
                    elif task_result in ["FAILED", "TIMEDOUT"]:
                        ico_class = "fail"
                        ico_mat = "close"
                        badge_class = "badge-fail"
                        badge_text = "Failed"
                    else:
                        ico_class = "wait"
                        ico_mat = "schedule"
                        badge_class = "badge-wait"
                        badge_text = "Pending"
                        
                    duration = "—"
                    if task.get("start_time") and task.get("end_time"):
                        duration = f"{(task['end_time'] - task['start_time']) / 1000:.1f}s"
                
                    st.markdown(f"""
                    <div class="task">
                        <div class="task-ico {ico_class}"><span class="material-icons" style="font-size: 15px;">{ico_mat}</span></div>
                        <span class="task-name">{task_name}</span>
                        <span class="task-dur">{duration}</span>
                        <span class="badge {badge_class}">{badge_text}</span>
                    </div>
                    """, unsafe_allow_html=True)
            
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
                        st.session_state.run_history.append({'source': 'powerbi', 'run_id': run_id, 'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'status': 'Started (Visual)'})
                        st.markdown(f'<div class="message-box message-success">PBI Visual conversion started. Run ID: {run_id}</div>', unsafe_allow_html=True)
                        time.sleep(1)
                        st.rerun()
                except Exception as e:
                    st.markdown(f'<div class="message-box message-error">Error starting visual conversion: {str(e)}</div>', unsafe_allow_html=True)
            
            if st.session_state.get("pbi_visual_run_id"):
                st.markdown('<div class="section-title"><h3>Execution Results</h3></div>', unsafe_allow_html=True)
                try:
                    run_id = st.session_state.pbi_visual_run_id
                    run_status = get_run_status(databricks_host, databricks_token, run_id)
                    state = run_status.get("state", {})
                    life_cycle_state = state.get("life_cycle_state", "UNKNOWN")
                    result_state = state.get("result_state", "")
                    tasks = run_status.get("tasks", [])
                    
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
                                                try:
                                                    result_data = json.loads(result_str)
                                                    
                                                    if "dashboard_id" in result_data or "dashboard_url" in result_data:
                                                        dash_id = result_data.get("dashboard_id", "N/A")
                                                        dash_name = result_data.get("dashboard_name", "N/A")
                                                        url = result_data.get("dashboard_url", "")
                                                        link_html = f'<a href="{url}" target="_blank">Open Dashboard &rarr;</a>' if url else "Not Available"
                                                        
                                                        st.markdown(f"""
                                                        <div class="result-card">
                                                            <div class="result-bar"></div>
                                                            <div class="result-head">
                                                                <div class="result-icon"><span class="material-icons">check_circle</span></div>
                                                                <h4 style="margin:0; font-size:15px; font-weight:700; color: var(--on-surface);">Conversion Complete</h4>
                                                            </div>
                                                            <div class="result-grid">
                                                                <div class="result-cell">
                                                                    <div class="result-cell-label">DASHBOARD ID</div>
                                                                    <div class="result-cell-val">{dash_id}</div>
                                                                </div>
                                                                <div class="result-cell">
                                                                    <div class="result-cell-label">DASHBOARD NAME</div>
                                                                    <div class="result-cell-val">{dash_name}</div>
                                                                </div>
                                                                <div class="result-cell">
                                                                    <div class="result-cell-label">LINK</div>
                                                                    <div class="result-cell-val">{link_html}</div>
                                                                </div>
                                                            </div>
                                                        </div>
                                                        """, unsafe_allow_html=True)
                                                        
                                                        dashboard_found = True
                                                        break # Stop searching once found
                                                        
                                                except json.JSONDecodeError: continue
                                    except Exception as e: continue
                            if not dashboard_found:
                                if result_state == "SUCCESS": 
                                    st.markdown('<div class="message-box message-info">Job succeeded, but no dashboard output details found.</div>', unsafe_allow_html=True)
                                else: 
                                    st.markdown(f'<div class="message-box message-error">Job failed: {result_state}</div>', unsafe_allow_html=True)
                            
                            st.markdown("### Conversion Tracker Details")
                            with st.spinner("Fetching tracker results..."):
                                tracker_df = get_conversion_tracker_data(databricks_host, databricks_token, warehouse_id, run_id)
                                if not tracker_df.empty:
                                    st.dataframe(tracker_df, use_container_width=True)
                                else:
                                    st.info("No tracking data available for this run.")
                        else:
                            st.markdown('<div class="message-box message-info">Visual Conversion in progress...</div>', unsafe_allow_html=True)
                    
                    with pv_out2:
                        col1, col2, col3, col4 = st.columns(4)
                        with col1: st.markdown(f'<div class="metric-card"><div class="metric-label">Run ID</div><div class="metric-value">{run_id}</div></div>', unsafe_allow_html=True)
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
                            
                            if task_result == "SUCCESS":
                                ico_class = "ok"
                                ico_mat = "check"
                                badge_class = "badge-ok"
                                badge_text = "Success"
                            elif task_result in ["FAILED", "TIMEDOUT"]:
                                ico_class = "fail"
                                ico_mat = "close"
                                badge_class = "badge-fail"
                                badge_text = "Failed"
                            else:
                                ico_class = "wait"
                                ico_mat = "schedule"
                                badge_class = "badge-wait"
                                badge_text = "Pending"
                                
                            duration = "—"
                            if task.get("start_time") and task.get("end_time"):
                                duration = f"{(task['end_time'] - task['start_time']) / 1000:.1f}s"
                        
                            st.markdown(f"""
                            <div class="task">
                                <div class="task-ico {ico_class}"><span class="material-icons" style="font-size: 15px;">{ico_mat}</span></div>
                                <span class="task-name">{task_name}</span>
                                <span class="task-dur">{duration}</span>
                                <span class="badge {badge_class}">{badge_text}</span>
                            </div>
                            """, unsafe_allow_html=True)
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
                        st.session_state.run_history.append({'source': 'powerbi', 'run_id': run_id, 'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'status': 'Started (Data)'})
                        st.markdown(f'<div class="message-box message-success">PBI Data conversion started. Run ID: {run_id}</div>', unsafe_allow_html=True)
                        time.sleep(1)
                        st.rerun()
                except Exception as e:
                    st.markdown(f'<div class="message-box message-error">Error starting data conversion: {str(e)}</div>', unsafe_allow_html=True)
            
            if st.session_state.get("pbi_data_run_id"):
                st.markdown('<div class="section-title"><h3>Execution Results</h3></div>', unsafe_allow_html=True)
                try:
                    run_id = st.session_state.pbi_data_run_id
                    run_status = get_run_status(databricks_host, databricks_token, run_id)
                    state = run_status.get("state", {})
                    life_cycle_state = state.get("life_cycle_state", "UNKNOWN")
                    result_state = state.get("result_state", "")
                    tasks = run_status.get("tasks", [])
                    
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
                                                            st.download_button(label="Download SQL File", data=content, file_name=os.path.basename(generated_filepath), mime="application/sql")
                                            st.markdown('</div>', unsafe_allow_html=True)
                                    except Exception as e:
                                        st.error(f"Error parsing job output: {str(e)}")
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
                        with col1: st.markdown(f'<div class="metric-card"><div class="metric-label">Run ID</div><div class="metric-value">{run_id}</div></div>', unsafe_allow_html=True)
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
                            
                            if task_result == "SUCCESS":
                                ico_class = "ok"
                                ico_mat = "check"
                                badge_class = "badge-ok"
                                badge_text = "Success"
                            elif task_result in ["FAILED", "TIMEDOUT"]:
                                ico_class = "fail"
                                ico_mat = "close"
                                badge_class = "badge-fail"
                                badge_text = "Failed"
                            else:
                                ico_class = "wait"
                                ico_mat = "schedule"
                                badge_class = "badge-wait"
                                badge_text = "Pending"
                                
                            duration = "—"
                            if task.get("start_time") and task.get("end_time"):
                                duration = f"{(task['end_time'] - task['start_time']) / 1000:.1f}s"
                        
                            st.markdown(f"""
                            <div class="task">
                                <div class="task-ico {ico_class}"><span class="material-icons" style="font-size: 15px;">{ico_mat}</span></div>
                                <span class="task-name">{task_name}</span>
                                <span class="task-dur">{duration}</span>
                                <span class="badge {badge_class}">{badge_text}</span>
                            </div>
                            """, unsafe_allow_html=True)
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
        with st.container(key="my_blue_container"):
            st.markdown('<h1 style="color: rgb(51, 65, 85); text-align: left; margin: 0; padding: 0.5rem 0;">Tableau to Databricks</h1>', unsafe_allow_html=True)
            st.markdown('<p style="color: rgb(51, 65, 85); text-align: left; margin: 0 0 1.5rem 0; font-size: 0.95rem;">Workbook Migration</p>', unsafe_allow_html=True)
        
            tab_conv_tab1, tab_conv_tab2, tab_conv_tab3 = st.tabs(["Data Conversion", "Visual Conversion", "Migration Validator"])
            
            with tab_conv_tab1:
                st.markdown('<p style="margin: 1rem 0;">Convert Tableau Data Sources (TDS/Extracts) to Databricks SQL</p>', unsafe_allow_html=True)
                if not tableau_data_job_id:
                    st.warning("Tableau Data conversion job ID not configured (tableau_data_job_id)")
                else:
                    col1, col2, col3 = st.columns([1, 1, 1])
                    with col2:
                        start_tableau_data_button = st.button("Start Data Conversion", type="primary", use_container_width=True, key="start_tableau_data_conv")
            
            with tab_conv_tab2:
                st.markdown('<p style="margin: 1rem 0;">Convert Tableau Workbooks (TWB/TWBX) to Databricks AI/BI</p>', unsafe_allow_html=True)
                if not tableau_visual_job_id:
                    st.warning("Tableau Visual conversion job ID not configured (tableau_visual_job_id)")
                else:
                    col1, col2, col3 = st.columns([1, 1, 1])
                    with col2:
                        start_tableau_visual_button = st.button("Start Visual Conversion", type="primary", use_container_width=True, key="start_tableau_visual_conv")
            
            with tab_conv_tab3:
                st.markdown('<div class="coming-soon-screen"><div class="coming-soon-title">Tableau Validator</div><div class="coming-soon-text">Visual regression testing and automated validation for Tableau migrations is currently under development.</div><div class="coming-soon-badge">Coming Soon</div></div>', unsafe_allow_html=True)

    # CONFIG MANAGER TAB
    elif st.session_state.tableau_active_tab == 'config':
        st.markdown('<h1 style="color: rgb(51, 65, 85);">Tableau Config Manager</h1>', unsafe_allow_html=True)
        st.markdown('<p>View and manage configuration tables</p>', unsafe_allow_html=True)
        if not warehouse_id:
            st.warning("Warehouse ID not configured in config.json")
        else:
            st.info(f"Using warehouse: {warehouse_id}")
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
                        edited_df = st.data_editor(df, use_container_width=True, height=400, num_rows="dynamic", key=f"tableau_config_editor_{idx}")
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
        st.markdown('<h1 style="color: rgb(51, 65, 85);">Tableau Conversion History</h1>', unsafe_allow_html=True)
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
                        st.session_state.run_history.append({'source': 'tableau', 'run_id': run_id, 'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'status': 'Started (Visual)'})
                        st.markdown(f'<div class="message-box message-success">Tableau Visual conversion started. Run ID: {run_id}</div>', unsafe_allow_html=True)
                        time.sleep(1)
                        st.rerun()
                except Exception as e:
                    st.markdown(f'<div class="message-box message-error">Error starting visual conversion: {str(e)}</div>', unsafe_allow_html=True)
            
            if st.session_state.get("tableau_visual_run_id"):
                st.markdown('<div class="section-title"><h3>Execution Results</h3></div>', unsafe_allow_html=True)
                try:
                    run_id = st.session_state.tableau_visual_run_id
                    run_status = get_run_status(databricks_host, databricks_token, run_id)
                    state = run_status.get("state", {})
                    life_cycle_state = state.get("life_cycle_state", "UNKNOWN")
                    result_state = state.get("result_state", "")
                    tasks = run_status.get("tasks", [])
                    
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
                                                try:
                                                    result_data = json.loads(result_str)
                                                    
                                                    if "dashboard_id" in result_data or "dashboard_url" in result_data:
                                                        dash_id = result_data.get("dashboard_id", "N/A")
                                                        dash_name = result_data.get("dashboard_name", "N/A")
                                                        url = result_data.get("dashboard_url", "")
                                                        link_html = f'<a href="{url}" target="_blank">Open Dashboard &rarr;</a>' if url else "Not Available"
                                                        
                                                        st.markdown(f"""
                                                        <div class="result-card">
                                                            <div class="result-bar"></div>
                                                            <div class="result-head">
                                                                <div class="result-icon"><span class="material-icons">check_circle</span></div>
                                                                <h4 style="margin:0; font-size:15px; font-weight:700; color: var(--on-surface);">Conversion Complete</h4>
                                                            </div>
                                                            <div class="result-grid">
                                                                <div class="result-cell">
                                                                    <div class="result-cell-label">DASHBOARD ID</div>
                                                                    <div class="result-cell-val">{dash_id}</div>
                                                                </div>
                                                                <div class="result-cell">
                                                                    <div class="result-cell-label">DASHBOARD NAME</div>
                                                                    <div class="result-cell-val">{dash_name}</div>
                                                                </div>
                                                                <div class="result-cell">
                                                                    <div class="result-cell-label">LINK</div>
                                                                    <div class="result-cell-val">{link_html}</div>
                                                                </div>
                                                            </div>
                                                        </div>
                                                        """, unsafe_allow_html=True)
                                                        
                                                        dashboard_found = True
                                                        break # Stop searching once found
                                                        
                                                except json.JSONDecodeError: continue
                                    except Exception as e: continue
                            if not dashboard_found:
                                if result_state == "SUCCESS": 
                                    st.markdown('<div class="message-box message-info">Job succeeded, but no dashboard output details found.</div>', unsafe_allow_html=True)
                                else: 
                                    st.markdown(f'<div class="message-box message-error">Job failed: {result_state}</div>', unsafe_allow_html=True)
                        else:
                            st.markdown('<div class="message-box message-info">Visual Conversion in progress...</div>', unsafe_allow_html=True)
                    
                    with tv_out2:
                        col1, col2, col3, col4 = st.columns(4)
                        with col1: st.markdown(f'<div class="metric-card"><div class="metric-label">Run ID</div><div class="metric-value">{run_id}</div></div>', unsafe_allow_html=True)
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
                            
                            if task_result == "SUCCESS":
                                ico_class = "ok"
                                ico_mat = "check"
                                badge_class = "badge-ok"
                                badge_text = "Success"
                            elif task_result in ["FAILED", "TIMEDOUT"]:
                                ico_class = "fail"
                                ico_mat = "close"
                                badge_class = "badge-fail"
                                badge_text = "Failed"
                            else:
                                ico_class = "wait"
                                ico_mat = "schedule"
                                badge_class = "badge-wait"
                                badge_text = "Pending"
                                
                            duration = "—"
                            if task.get("start_time") and task.get("end_time"):
                                duration = f"{(task['end_time'] - task['start_time']) / 1000:.1f}s"
                        
                            st.markdown(f"""
                            <div class="task">
                                <div class="task-ico {ico_class}"><span class="material-icons" style="font-size: 15px;">{ico_mat}</span></div>
                                <span class="task-name">{task_name}</span>
                                <span class="task-dur">{duration}</span>
                                <span class="badge {badge_class}">{badge_text}</span>
                            </div>
                            """, unsafe_allow_html=True)
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
                        st.session_state.run_history.append({'source': 'tableau', 'run_id': run_id, 'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'status': 'Started (Data)'})
                        st.markdown(f'<div class="message-box message-success">Tableau Data conversion started. Run ID: {run_id}</div>', unsafe_allow_html=True)
                        time.sleep(1)
                        st.rerun()
                except Exception as e:
                    st.markdown(f'<div class="message-box message-error">Error starting data conversion: {str(e)}</div>', unsafe_allow_html=True)
            
            if st.session_state.get("tableau_data_run_id"):
                st.markdown('<div class="section-title"><h3>Execution Results</h3></div>', unsafe_allow_html=True)
                try:
                    run_id = st.session_state.tableau_data_run_id
                    run_status = get_run_status(databricks_host, databricks_token, run_id)
                    state = run_status.get("state", {})
                    life_cycle_state = state.get("life_cycle_state", "UNKNOWN")
                    result_state = state.get("result_state", "")
                    tasks = run_status.get("tasks", [])
                    
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
                                                            st.download_button(label="Download SQL File", data=content, file_name=os.path.basename(generated_filepath), mime="application/sql")
                                            st.markdown('</div>', unsafe_allow_html=True)
                                    except Exception as e:
                                        st.error(f"Error parsing job output: {str(e)}")
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
                        with col1: st.markdown(f'<div class="metric-card"><div class="metric-label">Run ID</div><div class="metric-value">{run_id}</div></div>', unsafe_allow_html=True)
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
                            
                            if task_result == "SUCCESS":
                                ico_class = "ok"
                                ico_mat = "check"
                                badge_class = "badge-ok"
                                badge_text = "Success"
                            elif task_result in ["FAILED", "TIMEDOUT"]:
                                ico_class = "fail"
                                ico_mat = "close"
                                badge_class = "badge-fail"
                                badge_text = "Failed"
                            else:
                                ico_class = "wait"
                                ico_mat = "schedule"
                                badge_class = "badge-wait"
                                badge_text = "Pending"
                                
                            duration = "—"
                            if task.get("start_time") and task.get("end_time"):
                                duration = f"{(task['end_time'] - task['start_time']) / 1000:.1f}s"
                        
                            st.markdown(f"""
                            <div class="task">
                                <div class="task-ico {ico_class}"><span class="material-icons" style="font-size: 15px;">{ico_mat}</span></div>
                                <span class="task-name">{task_name}</span>
                                <span class="task-dur">{duration}</span>
                                <span class="badge {badge_class}">{badge_text}</span>
                            </div>
                            """, unsafe_allow_html=True)
                    if auto_refresh and life_cycle_state in ["PENDING", "RUNNING"]:
                        time.sleep(config.get("refresh_interval_seconds", 5))
                        st.rerun()
                except Exception as e:
                    st.markdown(f'<div class="message-box message-error">Error checking status: {str(e)}</div>', unsafe_allow_html=True)