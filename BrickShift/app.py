import streamlit as st
import requests
import time
import json
from datetime import datetime
import os
import pandas as pd

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

def get_table_data_simple(host, token, warehouse_id, table_name):
    """Clean table data fetching"""
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

# Initialize session state
if 'run_history' not in st.session_state:
    st.session_state.run_history = []

if 'main_panel' not in st.session_state:
    st.session_state.main_panel = 'thoughtspot'

if 'ts_active_tab' not in st.session_state:
    st.session_state.ts_active_tab = 'conversion'

if 'pbi_active_tab' not in st.session_state:
    st.session_state.pbi_active_tab = 'conversion'

config = load_config()

databricks_host = normalize_host_url(config.get("databricks_host", ""))
#databricks_token = config.get("databricks_token", "")
databricks_token = os.getenv("DATABRICKS_TOKEN")
warehouse_id = config.get("warehouse_id", "")

# ThoughtSpot Job IDs
ts_visual_job_id = config.get("visual_job_id", "")
ts_data_job_id = config.get("data_job_id", "")
ts_data_output_volume = config.get("data_output_volume", "/Volumes/catalog/schema/volume_name/output/")

# Power BI Job IDs
pbi_conversion_job_id = config.get("pbi_conversion_job_id", "")
pbi_discovery_job_id = config.get("pbi_discovery_job_id", "")

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
    if st.session_state.main_panel == 'thoughtspot':
        test_job_id = ts_visual_job_id if ts_visual_job_id else ts_data_job_id
        if test_job_id:
            if st.button("Test Connection", type="secondary", key="global_test_conn"):
                with st.spinner("Testing..."):
                    success, message = test_connection(databricks_host, databricks_token, test_job_id)
                    if success:
                        st.toast("Connection successful")
                    else:
                        st.toast("Connection failed")
    elif st.session_state.main_panel == 'powerbi':
        test_job_id = pbi_conversion_job_id if pbi_conversion_job_id else pbi_discovery_job_id
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
    if st.button("Tableau (Coming Soon)", key="select_tableau", type="secondary", use_container_width=True, disabled=True):
        pass

st.markdown("---")

# Sidebar - Sub Navigation
with st.sidebar:
    st.markdown("""
    <div style="text-align: center; padding: 1rem 0 1.5rem 0; border-bottom: 1px solid #334155; margin-bottom: 1.5rem;">
        <div style="font-size: 1.1rem; font-weight: 600; color: #f8fafc;">Navigation</div>
    </div>
    """, unsafe_allow_html=True)
    
    # Show current section header
    if st.session_state.main_panel == 'thoughtspot':
        st.markdown("### ThoughtSpot Migration")
    elif st.session_state.main_panel == 'powerbi':
        st.markdown("### Power BI Migration")
    else:
        st.markdown("### Tableau Migration")
    
    # Sub-navigation based on active panel
    if st.session_state.main_panel == 'thoughtspot':
        if st.button("Conversion", key="ts_tab_conversion", use_container_width=True):
            st.session_state.ts_active_tab = 'conversion'
        
        if st.button("Config Manager", key="ts_tab_config", use_container_width=True):
            st.session_state.ts_active_tab = 'config'
        
        if st.button("History", key="ts_tab_history", use_container_width=True):
            st.session_state.ts_active_tab = 'history'
        
        if st.button("Discovery Details", key="ts_tab_discovery", use_container_width=True):
            st.session_state.ts_active_tab = 'discovery'
    
    elif st.session_state.main_panel == 'powerbi':
        if st.button("Conversion", key="pbi_tab_conversion", use_container_width=True):
            st.session_state.pbi_active_tab = 'conversion'
        
        if st.button("Config Manager", key="pbi_tab_config", use_container_width=True):
            st.session_state.pbi_active_tab = 'config'
        
        if st.button("History", key="pbi_tab_history", use_container_width=True):
            st.session_state.pbi_active_tab = 'history'
        
        if st.button("Discovery Details", key="pbi_tab_discovery", use_container_width=True):
            st.session_state.pbi_active_tab = 'discovery'

# ============================================================================
# THOUGHTSPOT MIGRATION PANEL
# ============================================================================
if st.session_state.main_panel == 'thoughtspot':
    
    # CONVERSION TAB
    if st.session_state.ts_active_tab == 'conversion':
        st.markdown('<h1 style="color: white; text-align: center; margin: 0; padding: 0.5rem 0;">ThoughtSpot to Databricks</h1>', unsafe_allow_html=True)
        st.markdown('<p style="color: rgba(255,255,255,0.9); text-align: center; margin: 0 0 1.5rem 0; font-size: 0.95rem;">Dashboard Migration</p>', unsafe_allow_html=True)
        
        conversion_tab1, conversion_tab2 = st.tabs(["Data Conversion", "Visual Conversion"])
        
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
                
    # CONFIG MANAGER TAB
    elif st.session_state.ts_active_tab == 'config':
        st.markdown('<h1 style="color: white;">ThoughtSpot Config Manager</h1>', unsafe_allow_html=True)
        st.markdown('<p>View and manage configuration tables</p>', unsafe_allow_html=True)
        
        if not warehouse_id:
            st.warning("Warehouse ID not configured in config.json")
        else:
            st.info(f"Using warehouse: {warehouse_id}")
            
            tab1, tab2, tab3 = st.tabs(["Chart Type Mappings", "Liveboard Migration Config", "TML DBX Metadata Mapping"])
            
            with tab1:
                st.markdown("### Chart Type Mappings")
                df = get_table_data_simple(databricks_host, databricks_token, warehouse_id, "dbx_migration_poc.dbx_migration_ts.chart_type_mappings")
                if not df.empty:
                    st.dataframe(df, use_container_width=True, height=400)
                else:
                    st.warning("No data to display")
            
            with tab2:
                st.markdown("### Liveboard Migration Config")
                df = get_table_data_simple(databricks_host, databricks_token, warehouse_id, "dbx_migration_poc.dbx_migration_ts.liveboard_migration_config")
                if not df.empty:
                    st.dataframe(df, use_container_width=True, height=400)
                else:
                    st.warning("No data to display")
            
            with tab3:
                st.markdown("### TML DBX Metadata Mapping")
                df = get_table_data_simple(databricks_host, databricks_token, warehouse_id, "dbx_migration_poc.dbx_migration_ts.tml_dbx_metadata_mapping")
                if not df.empty:
                    st.dataframe(df, use_container_width=True, height=400)
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
            if 'test_visual_button' in locals() and test_visual_button:
                with st.spinner("Testing connection..."):
                    success, message = test_connection(databricks_host, databricks_token, ts_visual_job_id)
                    st.markdown(f'<div class="message-box message-{"success" if success else "error"}">{message}</div>', unsafe_allow_html=True)

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
                    
                        with visual_tab_out1:
                            if life_cycle_state == "TERMINATED":
                                task = tasks[-1] if tasks else None
                                if task and task.get("run_id"):
                                    try:
                                        task_output = get_run_output(databricks_host, databricks_token, task["run_id"])
                                        if task_output.get("notebook_output", {}).get("result"):
                                            result_data = json.loads(task_output["notebook_output"]["result"])
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
                                        else:
                                            st.markdown('<div class="message-box message-info">No dashboard information available</div>', unsafe_allow_html=True)
                                    except:
                                        st.markdown('<div class="message-box message-info">Dashboard information not available</div>', unsafe_allow_html=True)
                                else:
                                    st.markdown('<div class="message-box message-info">Dashboard information not available</div>', unsafe_allow_html=True)
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
            if 'test_data_button' in locals() and test_data_button:
                with st.spinner("Testing connection..."):
                    success, message = test_connection(databricks_host, databricks_token, ts_data_job_id)
                    st.markdown(f'<div class="message-box message-{"success" if success else "error"}">{message}</div>', unsafe_allow_html=True)

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
                                
                                elif result_state in ["FAILED", "TIMEDOUT"]:
                                    st.markdown(f'<div class="message-box message-error">Data conversion failed with status: {result_state}</div>', unsafe_allow_html=True)
                            else:
                                st.markdown('<div class="message-box message-info">Data conversion in progress...</div>', unsafe_allow_html=True)
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
        
        if not pbi_conversion_job_id:
            st.warning("Power BI conversion job ID not configured in config.json")
            st.info("Please add 'pbi_conversion_job_id' to your config.json file")
        else:
            col1, col2, col3 = st.columns([1, 1, 1])
            with col2:
                start_pbi_conversion_button = st.button("Start Conversion", type="primary", use_container_width=True, key="start_pbi_conversion")

    # CONFIG MANAGER TAB
    elif st.session_state.pbi_active_tab == 'config':
        st.markdown('<h1 style="color: white;">Power BI Config Manager</h1>', unsafe_allow_html=True)
        st.markdown('<p>View and manage configuration tables</p>', unsafe_allow_html=True)
        
        if not warehouse_id:
            st.warning("Warehouse ID not configured in config.json")
        else:
            st.info(f"Using warehouse: {warehouse_id}")
            
            tab1, tab2, tab3, tab4, tab5 = st.tabs([
                "Chart Type Mappings", 
                "Expression Transformations", 
                "Scale Type Detection",
                "Widget Size Config",
                "DAX to SQL Mapping"
            ])
            
            with tab1:
                st.markdown("### Chart Type Mappings")
                df = get_table_data_simple(databricks_host, databricks_token, warehouse_id, "dbx_migration_poc.dbx_migration_pbi.chart_type_mappings")
                if not df.empty:
                    st.dataframe(df, use_container_width=True, height=400)
                else:
                    st.warning("No data to display")
            
            with tab2:
                st.markdown("### Expression Transformations")
                df = get_table_data_simple(databricks_host, databricks_token, warehouse_id, "dbx_migration_poc.dbx_migration_pbi.expression_transformations")
                if not df.empty:
                    st.dataframe(df, use_container_width=True, height=400)
                else:
                    st.warning("No data to display")
            
            with tab3:
                st.markdown("### Scale Type Detection")
                df = get_table_data_simple(databricks_host, databricks_token, warehouse_id, "dbx_migration_poc.dbx_migration_pbi.scale_type_detection")
                if not df.empty:
                    st.dataframe(df, use_container_width=True, height=400)
                else:
                    st.warning("No data to display")
            
            with tab4:
                st.markdown("### Widget Size Config")
                df = get_table_data_simple(databricks_host, databricks_token, warehouse_id, "dbx_migration_poc.dbx_migration_pbi.widget_size_config")
                if not df.empty:
                    st.dataframe(df, use_container_width=True, height=400)
                else:
                    st.warning("No data to display")
            
            with tab5:
                st.markdown("### DAX to SQL Mapping")
                df = get_table_data_simple(databricks_host, databricks_token, warehouse_id, "dbx_migration_poc.dbx_migration_pbi.dax_to_sql_mapping")
                if not df.empty:
                    st.dataframe(df, use_container_width=True, height=400)
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

    # Process Power BI conversion job
    if st.session_state.pbi_active_tab == 'conversion' and pbi_conversion_job_id:
        
        if 'test_pbi_button' in locals() and test_pbi_button:
            with st.spinner("Testing connection..."):
                success, message = test_connection(databricks_host, databricks_token, pbi_conversion_job_id)
                st.markdown(f'<div class="message-box message-{"success" if success else "error"}">{message}</div>', unsafe_allow_html=True)

        if 'start_pbi_conversion_button' in locals() and start_pbi_conversion_button:
            try:
                with st.spinner("Initiating Power BI conversion..."):
                    run_id = trigger_job(databricks_host, databricks_token, pbi_conversion_job_id, None)
                    st.session_state.pbi_conversion_run_id = run_id
                    st.session_state.pbi_conversion_start_time = datetime.now()
                    
                    st.session_state.run_history.append({
                        'source': 'powerbi',
                        'run_id': run_id,
                        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'status': 'Started'
                    })
                    
                    st.markdown(f'<div class="message-box message-success">Power BI conversion started successfully. Run ID: {run_id}</div>', unsafe_allow_html=True)
                    time.sleep(1)
                    st.rerun()
            except Exception as e:
                st.markdown(f'<div class="message-box message-error">Error starting conversion: {str(e)}</div>', unsafe_allow_html=True)

        if st.session_state.get("pbi_conversion_run_id"):
            try:
                run_id = st.session_state.pbi_conversion_run_id
                run_status = get_run_status(databricks_host, databricks_token, run_id)
                state = run_status.get("state", {})
                life_cycle_state = state.get("life_cycle_state", "UNKNOWN")
                result_state = state.get("result_state", "")
                tasks = run_status.get("tasks", [])
            
                st.markdown("---")
                
                pbi_tab_out1, pbi_tab_out2 = st.tabs(["Conversion Output", "Execution Details"])
            
                with pbi_tab_out1:
                    if life_cycle_state == "TERMINATED":
                        # Try to find the task that has notebook output with dashboard info
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
                                            # Check if this result has dashboard info
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
                                                break
                                        except:
                                            continue
                                except:
                                    continue
                        
                        if not dashboard_found:
                            st.markdown('<div class="message-box message-info">No dashboard information available</div>', unsafe_allow_html=True)
                    else:
                        st.markdown('<div class="message-box message-info">Conversion in progress...</div>', unsafe_allow_html=True)
                        st.progress(0.5)
            
                with pbi_tab_out2:
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.markdown(f'<div class="metric-card"><div class="metric-label">Run ID</div><div class="metric-value">{run_id}</div></div>', unsafe_allow_html=True)
                    with col2:
                        status_display = result_state if result_state else life_cycle_state
                        status_class = "status-success" if result_state == "SUCCESS" else "status-error" if result_state in ["FAILED", "TIMEDOUT"] else "status-running"
                        st.markdown(f'<div class="metric-card"><div class="metric-label">Status</div><div class="metric-value {status_class}">{status_display}</div></div>', unsafe_allow_html=True)
                    with col3:
                        if st.session_state.get("pbi_conversion_start_time"):
                            elapsed = (datetime.now() - st.session_state.pbi_conversion_start_time).seconds
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
# TABLEAU COMING SOON SCREEN
# ============================================================================
else:
    st.markdown("""
    <div class="coming-soon-screen">
        <div class="coming-soon-title">Tableau Migration</div>
        <div class="coming-soon-text">
            We're working hard to bring you Tableau workbook migration capabilities.<br><br>
            <strong>Planned Features:</strong><br>
            • Workbook conversion to Databricks AI/BI<br>
            • Calculated field transformation<br>
            • Data source migration<br>
            • Dashboard layout preservation<br><br>
            Stay tuned for updates!
        </div>
        <div class="coming-soon-badge">Coming Soon</div>
    </div>
    """, unsafe_allow_html=True)