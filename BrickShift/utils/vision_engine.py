import base64
import requests
import json
import os
import time
import uuid
from datetime import datetime
from typing import Dict, Optional, Any

CATALOG = "dbx_migration_poc"
SCHEMA = "migrationvalidation"
VALIDATION_TABLE = f"{CATALOG}.{SCHEMA}.validation_results"

try:
    from pyspark.sql import SparkSession, Row
    from pyspark.sql.types import *
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    print("[WARN] Spark not detected. Delta logging will be skipped.")

class SimpleLogger:
    def info(self, msg, **kwargs): print(f"[INFO] {msg}")
    def success(self, msg, **kwargs): print(f"[SUCCESS] {msg}")
    def error(self, msg, **kwargs): print(f"[ERROR] {msg}")
    def debug(self, msg, **kwargs): print(f"[DEBUG] {msg}")

logger = SimpleLogger()

def get_validation_results_schema():
    if not SPARK_AVAILABLE: return None
    return StructType([
        StructField("validation_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("source_tool", StringType(), True),
        StructField("source_file_path", StringType(), True),
        StructField("target_file_path", StringType(), True),
        StructField("stage1_model", StringType(), True),
        StructField("stage2_model", StringType(), True),
        StructField("detailed_analysis", StringType(), True),
        StructField("executive_summary", StringType(), True),
        StructField("validation_status", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("duration_ms", IntegerType(), True),
        StructField("error", StringType(), True)
    ])

def encode_pdf_to_base64(pdf_path: str) -> str:
    logger.info(f"Encoding PDF: {os.path.basename(pdf_path)}")
    with open(pdf_path, "rb") as pdf_file:
        return base64.b64encode(pdf_file.read()).decode('utf-8')

def save_validation_results(result_dict: Dict):
    if not SPARK_AVAILABLE:
        logger.info("Spark unavailable; skipping database write.")
        return

    try:
        spark = SparkSession.builder.getOrCreate()
        schema = get_validation_results_schema()
        df = spark.createDataFrame([Row(**result_dict)], schema=schema)
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(VALIDATION_TABLE)
        logger.success(f"Saved validation results to {VALIDATION_TABLE}")
    except Exception as e:
        logger.error(f"Failed to save validation results: {e}")

def get_stage1_analysis_prompt(source_tool: str) -> str:
    return f"""You are an expert dashboard analyzer performing deep visual inspection for migration validation.

**Your Task:** Analyze these two dashboards in extreme detail:
- **Dashboard 1 (Source):** {source_tool.title()} dashboard
- **Dashboard 2 (Target):** AI BI migrated dashboard

**CRITICAL: Perform comprehensive OCR and visual extraction on BOTH dashboards.**

## Required Analysis

### 1. OCR & TEXT EXTRACTION
Extract ALL text visible in both dashboards:
- **Dashboard titles and headers**
- **All KPI labels and their exact values** (e.g., "Revenue: $1.2M")
- **Chart titles and axis labels**
- **Legend items**
- **Filter labels and selected values**
- **Footnotes or annotations**
- **Any descriptive text**

Format as:
```
SOURCE DASHBOARD TEXT:
- Title: [exact text]
- KPI 1: [label] = [value]
- KPI 2: [label] = [value]
- Chart 1 Title: [text]
- X-axis labels: [list]
- Y-axis labels: [list]
...

TARGET DASHBOARD TEXT:
- [same structure]
```

### 2. VISUAL ELEMENT INVENTORY
List every visual component in both dashboards:
- **Chart Types:** Bar, Line, Pie, Table, Gauge, Map, etc.
- **Number of charts per dashboard**
- **Chart positions:** Top-left, center, bottom-right, etc.
- **Color schemes:** Primary colors used
- **Visual hierarchy:** Which elements are emphasized

### 3. LAYOUT STRUCTURE
- **Grid/column structure:** How is space divided?
- **Element spacing:** Tight vs. loose
- **Alignment:** Left, center, right, justified
- **White space usage**
- **Visual balance**

### 4. DATA ACCURACY CHECK
Compare extracted data values:
- Do KPI values match exactly?
- Do chart data points align?
- Are filters showing the same selections?
- Any discrepancies in numbers?

### 5. STRUCTURAL DIFFERENCES
Identify what changed:
- **Added elements** (present in target, not in source)
- **Removed elements** (present in source, not in target)
- **Modified elements** (present in both but changed)
- **Repositioned elements**

**Be extremely thorough. This detailed analysis will be summarized in the next stage.**"""

def get_stage2_summarization_prompt() -> str:
    return """You are a migration validation expert creating an executive summary.

You will receive detailed visual analysis findings from a dashboard migration comparison.

**Your Task:** Transform the technical analysis into a clear, actionable migration validation report.

## Required Report Structure

### EXECUTIVE SUMMARY
- **Migration Quality:** [Excellent/Good/Fair/Poor/Critical]
- **Overall Similarity Score:** [0-100%]
- **Ready for Production:** [Yes/No]
- **Top Priority Action:** [Most critical item to address]

### MIGRATION ACCURACY (Score: X/10)
- **Data Completeness:** Are all KPIs and metrics preserved?
- **Value Accuracy:** Do numbers match exactly?
- **Missing Elements:** List any charts/metrics not migrated
- **Data Integrity Issues:** Any calculation or display errors

### VISUAL CONSISTENCY (Score: X/10)
- **Layout Match:** How similar is the structure?
- **Color Scheme:** Consistency assessment
- **Typography:** Font and text rendering
- **Visual Hierarchy:** Information priority preserved?

### USER EXPERIENCE (Score: X/10)
- **Dashboard Clarity:** Is information easy to find?
- **Visual Balance:** Proper use of space
- **Readability:** Text legibility and contrast
- **Navigation:** Filter and interaction clarity

### DETAILED FINDINGS

#### ✅ Successfully Migrated
- [List elements that migrated perfectly]

#### ⚠️ Acceptable Differences
- [List minor differences that don't impact functionality]

#### ❌ Critical Issues
- [List problems that must be fixed before production]

### SCORING BREAKDOWN

| Category | Score | Weight | Weighted Score |
|----------|-------|--------|---------------|
| Data Accuracy | X/10 | 40% | X.X |
| Layout Consistency | X/10 | 30% | X.X |
| Visual Design | X/10 | 20% | X.X |
| User Experience | X/10 | 10% | X.X |
| **TOTAL** | | | **XX.X/100** |

### PRIORITY RECOMMENDATIONS

#### Must Fix (Critical - P0)
1. [Issue] - Impact: [High/Medium/Low]

#### Should Fix (Important - P1)
1. [Issue] - Impact: [High/Medium/Low]

#### Nice to Have (Enhancement - P2)
1. [Improvement suggestion]

### MIGRATION QUALITY VERDICT

Based on the overall score:
- **90-100%:** ✅ EXCELLENT - Production ready
- **75-89%:** ✅ GOOD - Minor fixes recommended
- **60-74%:** ⚠️ FAIR - Review and fixes needed
- **40-59%:** ⚠️ POOR - Significant rework required
- **0-39%:** ❌ CRITICAL - Migration failed, start over

**Final Recommendation:** [Clear go/no-go decision with justification]

**Be decisive, specific, and actionable. Include exact issues found and clear next steps.**
    """

def run_stage1_analysis(source_b64, target_b64, endpoint, token, source_tool, model):
    logger.info(f"Stage 1: Analysis with {model}")
    prompt = get_stage1_analysis_prompt(source_tool)
    
    payload = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text", 
                        "text": prompt
                    },
                    {
                        "type": "document", 
                        "source": {
                            "type": "base64", 
                            "media_type": "application/pdf", 
                            "data": source_b64
                        }
                    },
                    {
                        "type": "document", 
                        "source": {
                            "type": "base64", 
                            "media_type": "application/pdf", 
                            "data": target_b64
                        }
                    }
                ]
            }
        ],
        "max_tokens": 4000,
        "temperature": 0.1
    }
    
    headers = {
        "Authorization": f"Bearer {token}", 
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(endpoint, json=payload, headers=headers, timeout=180)
        response.raise_for_status()
        result = response.json()
        
        
        analysis_text = ""
        
        if "choices" in result:
             analysis_text = result["choices"][0]["message"]["content"]

        elif "content" in result:
            items = result["content"]
            if isinstance(items, list):
                for item in items:
                    if item.get("type") == "text":
                        analysis_text += item.get("text", "")
            else:
                analysis_text = str(items)
        else:
            analysis_text = json.dumps(result)
        
        return {"status": "success", "detailed_analysis": analysis_text}
        
    except Exception as e:
        logger.error(f"Stage 1 failed: {e}")
        return {"status": "error", "error": str(e)}


def run_stage2_summarization(analysis_text, endpoint, token, model):
    logger.info(f"Stage 2: Summarization with {model}")
    prompt = get_stage2_summarization_prompt()
    
    full_content = f"{prompt}\n\n--- TECHNICAL ANALYSIS FROM STAGE 1 ---\n{analysis_text}"
    
    payload = {
        "messages": [
            {
                "role": "user", 
                "content": full_content
            }
        ],
        "max_tokens": 2000,
        "temperature": 0.3
    }
    
    headers = {
        "Authorization": f"Bearer {token}", 
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(endpoint, json=payload, headers=headers, timeout=120)
        response.raise_for_status()
        result = response.json()
        
        summary_text = ""
        
        if "choices" in result:
            summary_text = result["choices"][0]["message"]["content"]
        elif "content" in result:
            items = result["content"]
            if isinstance(items, list):
                for item in items:
                    if item.get("type") == "text":
                        summary_text += item.get("text", "")
            else:
                summary_text = str(items)
        else:
            summary_text = str(result)
            
        return {"status": "success", "summary": summary_text}
        
    except Exception as e:
        logger.error(f"Stage 2 failed: {e}")
        return {"status": "error", "error": str(e)}
    
def compare_dashboards(
    source_pdf_path: str,
    target_pdf_path: str,
    stage1_endpoint: str, 
    stage2_endpoint: str,  
    token: str,
    session_id: str,
    source_tool: str = "PowerBI"
) -> Dict:
    
    validation_id = str(uuid.uuid4())
    start_time = time.time()
    timestamp = datetime.now()
    
    try:
        # 1. Encode PDFs
        source_b64 = encode_pdf_to_base64(source_pdf_path)
        target_b64 = encode_pdf_to_base64(target_pdf_path)

        # 2. Run Stage 1 (Detailed Analysis) with Endpoint 1 (Opus)
        s1 = run_stage1_analysis(source_b64, target_b64, stage1_endpoint, token, source_tool, "Claude Opus")
        if s1["status"] == "error": raise Exception(f"Stage 1 Failed: {s1['error']}")

        # 3. Run Stage 2 (Summarization) with Endpoint 2 (Haiku)
        s2 = run_stage2_summarization(s1["detailed_analysis"], stage2_endpoint, token, "Claude Haiku")
        if s2["status"] == "error": raise Exception(f"Stage 2 Failed: {s2['error']}")

        # 4. Compile Results
        duration = int((time.time() - start_time) * 1000)
        final_result = {
            "validation_id": validation_id,
            "session_id": session_id,
            "source_tool": source_tool,
            "source_file_path": source_pdf_path,
            "target_file_path": target_pdf_path,
            "stage1_model": "Claude Opus",
            "stage2_model": "Claude Haiku",
            "detailed_analysis": s1["detailed_analysis"],
            "executive_summary": s2["summary"],
            "validation_status": "success",
            "timestamp": timestamp,
            "duration_ms": duration,
            "error": None
        }
        
        save_validation_results(final_result)
        return final_result

    except Exception as e:
        duration = int((time.time() - start_time) * 1000)
        err_result = {
            "validation_id": validation_id,
            "session_id": session_id,
            "timestamp": timestamp,
            "validation_status": "failed",
            "error": str(e),
            "duration_ms": duration
        }
        save_validation_results(err_result)
        return err_result