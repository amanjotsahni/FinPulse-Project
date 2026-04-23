"""
FinPulse — Compliance Intelligence System
==========================================
Entry point: streamlit run ai_layer/finpulse_app.py

Four screens:
  1. Fraud Detection      — Live anomaly scatter plot (Isolation Forest)
  2. Account Investigation — Conversational data investigation (RAG)
  3. Risk Assessment       — Streaming regulatory classification (DeepSeek R1)
  4. Executive Briefing    — Board-ready compliance summary + PDF export
"""

import streamlit as st
import os
import dotenv
from datetime import datetime
from pathlib import Path
import sys

dotenv.load_dotenv(r"C:\FinPulse Project\.env")

# Allow importing config from project root
_PROJECT_ROOT = Path(__file__).parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from agent1_hunt import run_hunt  # noqa: E402

# Effortless integration for your module development
try:
    from agent2_investigate import run_investigate
except ImportError:
    run_investigate = None

st.set_page_config(
    page_title="FinPulse — Compliance Intelligence",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Design system ──────────────────────────────────────────────────────────────
# Surface:  #07090F (body) · #0D1117 (sidebar) · #111827 (card) · #161C2A (elevated)
# Text:     #F1F5F9 (primary) · #8B98A8 (secondary) · #4B5563 (muted)
# Accent:   #3B82F6 (electric blue — only hued colour)
# Danger:   #EF4444  Warning: #F59E0B  Clear: #10B981
# Border:   rgba(255,255,255,0.06) std · rgba(59,130,246,0.3) accent
# Radius:   2-4px — no pill shapes
# Type:     Inter 300-800 body · JetBrains Mono data values

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap');

/* Palette
   Body    #0F1A2E  deep navy base
   Card    #172340  medium navy — lighter than body, clearly distinct
   Raised  #1E2E50  hover / elevated surface
   Sidebar #090E18  darkest ink
   T1 #C8D8F0  T2 #7E9BB8  T3 #4A6080
   Accent  #60A5FA  sky blue (lighter — avoids dark-blue-on-dark-blue)
   Sky     #BAE6FD  lightest highlight text
*/
*,*::before,*::after{box-sizing:border-box}
/* Force main-area text to white. Using .main * wildcard but NOT span/div bare tags
   so Streamlit's icon ligature font is not overridden (see font-family rule below). */
.main *,
[data-testid="stMainBlockContainer"] *,
[data-testid="stMarkdownContainer"] *{color:#FFFFFF!important}
/* Then selectively pull back secondary/muted roles */
.screen-header p,[data-testid="stMarkdownContainer"] p{color:#7E9BB8!important}
.kpi-label,.kpi-delta,.section-label,.pending-desc,.layer-detail,
.data-table th,.guard-msg,.chat-agent,.reasoning-block{color:#7E9BB8!important}
/* Apply Inter font to text elements — deliberately exclude bare span/div
   so Streamlit's icon font (used for _arrow_right glyphs etc.) is not overridden.
   Icon components render icon names as text nodes styled with a ligature font;
   forcing Inter on every span/div breaks the font and shows raw strings. */
html,body,[class*="css"],.stMarkdown,.stText,p,label,
.stRadio label,.stSelectbox label,.stButton button,textarea,input{
  font-family:'Inter',-apple-system,BlinkMacSystemFont,sans-serif!important}

.stApp{background:#1A2E4A!important}
.main .block-container{padding:2.25rem 3rem 3rem;max-width:1440px;padding-top:2.25rem!important}

#MainMenu,footer,header[data-testid="stHeader"],[data-testid="stToolbar"],
[data-testid="stDecoration"],[data-testid="stStatusWidget"],
[data-testid="stSidebarCollapseButton"],[data-testid="collapsedControl"],
[data-testid="baseButton-header"],[data-testid="stHeaderActionElements"],
.viewerBadge_container__r5tak,.stDeployButton,
.st-emotion-cache-xtjyj5,.st-emotion-cache-1dp5vir{
  display:none!important;visibility:hidden!important;
  height:0!important;width:0!important;overflow:hidden!important;
  position:absolute!important;pointer-events:none!important}
.stApp>header{display:none!important}

/* Sidebar */
section[data-testid="stSidebar"]{
  background:#090E18!important;border-right:1px solid rgba(255,255,255,0.07)!important;
  min-width:256px!important;max-width:256px!important;box-shadow:none!important}
section[data-testid="stSidebar"] [data-testid="stRadio"] label,
section[data-testid="stSidebar"] [data-testid="stRadio"] label *,
section[data-testid="stSidebar"] .stButton button,
section[data-testid="stSidebar"] .sidebar-section,
section[data-testid="stSidebar"] [class*="sidebar-section"]{color:#FFFFFF!important}
section[data-testid="stSidebar"]>div:first-child{padding:0!important}
section[data-testid="stSidebar"] button[kind="header"],
section[data-testid="stSidebar"] [data-testid="stSidebarCollapseButton"]{display:none!important}

/* Kill the invisible ghost label element above the nav items */
section[data-testid="stSidebar"] [data-testid="stRadio"]>label,
section[data-testid="stSidebar"] [data-testid="stRadio"]>div:first-child>label:first-child:empty,
section[data-testid="stSidebar"] [data-testid="stWidgetLabel"]{
  display:none!important;height:0!important;margin:0!important;padding:0!important;
  pointer-events:none!important;overflow:hidden!important}

section[data-testid="stSidebar"] [data-testid="stRadio"]>div{gap:1px!important}
section[data-testid="stSidebar"] [data-testid="stRadio"] label{
  background:transparent!important;border:none!important;
  border-left:2px solid transparent!important;border-radius:0!important;
  padding:11px 20px!important;font-size:12px!important;font-weight:600!important;
  color:#FFFFFF!important;cursor:pointer!important;transition:all 0.15s!important;
  margin:0!important;letter-spacing:0.07em!important;text-transform:uppercase!important;
  display:flex!important;align-items:center!important}
section[data-testid="stSidebar"] [data-testid="stRadio"] label:hover{
  background:rgba(96,165,250,0.06)!important;
  border-left-color:rgba(96,165,250,0.4)!important;color:#FFFFFF!important}
section[data-testid="stSidebar"] [data-testid="stRadio"] label[data-checked="true"],
section[data-testid="stSidebar"] [data-testid="stRadio"] [aria-checked="true"]{
  background:rgba(96,165,250,0.1)!important;
  border-left-color:#60A5FA!important;color:#FFFFFF!important;font-weight:700!important}
section[data-testid="stSidebar"] [data-testid="stRadio"] label>div:first-child{display:none!important}

section[data-testid="stSidebar"] .stButton button{
  background:transparent!important;border:1px solid rgba(255,255,255,0.09)!important;
  border-radius:3px!important;color:#FFFFFF!important;font-size:11px!important;
  font-weight:600!important;padding:8px 14px!important;
  letter-spacing:0.07em!important;text-transform:uppercase!important;transition:all 0.15s!important}
section[data-testid="stSidebar"] .stButton button *{
  color:#FFFFFF!important;border:none!important;background:transparent!important;padding:0!important}
section[data-testid="stSidebar"] .stButton button:hover{
  border-color:rgba(96,165,250,0.35)!important;color:#FFFFFF!important}

/* Screen header */
.screen-header{margin-bottom:36px;padding-bottom:22px;border-bottom:1px solid rgba(96,165,250,0.12)}
.screen-tag{font-size:10px;font-weight:700;color:#60A5FA;letter-spacing:0.16em;
  text-transform:uppercase;border:1px solid rgba(96,165,250,0.3);padding:4px 10px;
  border-radius:2px;background:rgba(96,165,250,0.08);display:inline-block;margin-bottom:12px}
.screen-header h1,.screen-header h1 *{font-size:30px;font-weight:700;color:#FFFFFF!important;
  margin:0 0 10px;letter-spacing:-0.02em;line-height:1.2}
.screen-header p{font-size:14px;color:#7E9BB8;margin:0;line-height:1.7;max-width:700px}

/* Section divider */
.section-label{font-size:11px;font-weight:700;color:#4A6080;text-transform:uppercase;
  letter-spacing:0.12em;margin:32px 0 16px;display:flex;align-items:center;gap:12px}
.section-label::after{content:'';flex:1;height:1px;background:rgba(96,165,250,0.1)}

/* KPI cards */
.kpi-card{background:#172340;border:1px solid rgba(96,165,250,0.12);border-radius:4px;
  padding:20px 22px;position:relative;overflow:hidden;transition:border-color 0.15s}
.kpi-card:hover{border-color:rgba(96,165,250,0.22)}
.kpi-card::after{content:'';position:absolute;bottom:0;left:0;right:0;
  height:2px;background:rgba(96,165,250,0.5)}
.kpi-card.accent-red::after{background:rgba(248,113,113,0.7)}
.kpi-card.accent-amber::after{background:rgba(251,191,36,0.7)}
.kpi-card.accent-green::after{background:rgba(52,211,153,0.7)}
.kpi-label{font-size:11px;font-weight:700;color:#4A6080;text-transform:uppercase;
  letter-spacing:0.12em;margin-bottom:10px}
.kpi-value{font-family:'JetBrains Mono',monospace!important;font-size:22px;
  font-weight:600;color:#FFFFFF;line-height:1.1;letter-spacing:-0.02em;
  word-break:break-all;overflow-wrap:anywhere}
.kpi-delta{font-size:12px;margin-top:9px;color:#4A6080}

/* Cards */
.fp-card,.guard-msg,.pending-card,.chat-agent{
  background:#172340;border:1px solid rgba(96,165,250,0.10);border-radius:4px}
.fp-card{padding:22px 26px;transition:border-color 0.15s}
.fp-card:hover{border-color:rgba(96,165,250,0.20)}

/* Guard */
.guard-msg{border-left:3px solid #60A5FA;padding:16px 20px;
  color:#7E9BB8;font-size:13.5px;line-height:1.7;margin-bottom:22px}
.guard-msg strong{color:#FFFFFF;font-weight:600}

/* Pending */
.pending-card{padding:56px 44px;text-align:center}
.pending-indicator{width:32px;height:2px;background:#60A5FA;margin:0 auto 22px}
.pending-title{font-size:13px;font-weight:700;color:#FFFFFF!important;margin-bottom:12px;
  text-transform:uppercase;letter-spacing:0.09em}
.pending-desc{font-size:13.5px;color:#4A6080;line-height:1.7;max-width:480px;margin:0 auto}
.feature-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px;
  margin:28px auto 0;text-align:left;max-width:500px}
.feature-item{display:flex;align-items:center;gap:9px;font-size:12.5px;
  color:#FFFFFF!important;padding:9px 12px;background:rgba(96,165,250,0.04);
  border-radius:3px;border:1px solid rgba(96,165,250,0.10)}
.feature-dot{width:4px;height:4px;border-radius:50%;background:#60A5FA;flex-shrink:0}

/* Pipeline rows */
.layer-row{display:flex;align-items:center;justify-content:space-between;
  padding:8px 0;border-bottom:1px solid rgba(96,165,250,0.07)}
.layer-row:last-child{border-bottom:none}
.layer-name{font-size:11px;font-weight:600;color:#FFFFFF!important;
  letter-spacing:0.05em;text-transform:uppercase}
.layer-detail{font-size:11px;color:#4A6080;font-family:'JetBrains Mono',monospace}
.layer-bar{width:28px;height:2px;border-radius:1px;margin:0 10px;flex-shrink:0}
.bar-green{background:#34D399}.bar-amber{background:#FBBF24}
.bar-red{background:#F87171}.bar-grey{background:#1E2E50}

/* Risk banner */
.risk-banner{padding:13px 20px;border-radius:3px;display:flex;align-items:center;
  gap:12px;font-size:12px;font-weight:700;letter-spacing:0.1em;
  text-transform:uppercase;margin-bottom:24px;border-left:3px solid}
.risk-high{background:rgba(248,113,113,0.08);border-color:#F87171;color:#F87171}
.risk-medium{background:rgba(251,191,36,0.08);border-color:#FBBF24;color:#FBBF24}
.risk-low{background:rgba(52,211,153,0.08);border-color:#34D399;color:#34D399}

/* Badges & chips */
.badge{display:inline-block;font-size:10px;font-weight:700;letter-spacing:0.08em;
  text-transform:uppercase;padding:4px 9px;border-radius:2px}
.badge-red{background:rgba(248,113,113,0.12);color:#F87171;border:1px solid rgba(248,113,113,0.25)}
.badge-amber{background:rgba(251,191,36,0.12);color:#FBBF24;border:1px solid rgba(251,191,36,0.25)}
.badge-green{background:rgba(52,211,153,0.12);color:#34D399;border:1px solid rgba(52,211,153,0.25)}
.risk-chip{font-size:10px;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;
  padding:3px 7px;border-radius:2px;display:inline-block}
.chip-red{color:#F87171;background:rgba(248,113,113,0.12)}
.chip-amber{color:#FBBF24;background:rgba(251,191,36,0.12)}
.chip-green{color:#34D399;background:rgba(52,211,153,0.12)}

/* Chat */
.chat-user{background:rgba(59,130,246,0.10);border:1px solid rgba(59,130,246,0.22);
  color:#93C5FD;border-radius:2px 2px 2px 0;padding:12px 16px;margin:10px 0;
  font-size:13.5px;max-width:68%;margin-left:auto;line-height:1.6}
.chat-agent{padding:12px 16px;margin:10px 0;font-size:13.5px;
  max-width:76%;border-radius:0 2px 2px 2px;color:#FFFFFF!important;line-height:1.6}
.chat-agent strong{color:#FFFFFF;font-weight:600}

/* Table */
.data-table{width:100%;border-collapse:separate;border-spacing:0;font-size:13px;margin-top:10px}
.data-table th{padding:12px 16px;color:#4A6080;font-weight:700;text-transform:uppercase;
  font-size:10px;letter-spacing:0.12em;border-bottom:1px solid rgba(96,165,250,0.15);
  text-align:left;background:rgba(96,165,250,0.06)}
.data-table td{padding:14px 16px;color:#FFFFFF!important;
  border-bottom:1px solid rgba(96,165,250,0.08);vertical-align:middle;
  transition:all 0.2s ease}
.data-table tr{transition:background 0.2s ease}
.data-table tr:hover td{background:rgba(96,165,250,0.08);color:#60A5FA!important}
.data-table tr.row-critical{border-left:3px solid #F87171!important}
.data-table tr.row-warn{border-left:3px solid #FBBF24!important}
.data-table tr.row-info{border-left:3px solid #60A5FA!important}

.mono{font-family:'JetBrains Mono',monospace!important;color:#C8D8F0;font-size:12px;letter-spacing:-0.01em}
.amount{font-family:'JetBrains Mono',monospace!important;color:#FFFFFF;font-weight:600;font-size:13px}
.status-indicator{width:8px;height:8px;border-radius:50%;display:inline-block;margin-right:8px}
.bg-red{background:#F87171;box-shadow:0 0 8px rgba(248,113,113,0.4)}
.bg-amber{background:#FBBF24;box-shadow:0 0 8px rgba(251,191,36,0.4)}
.bg-blue{background:#60A5FA;box-shadow:0 0 8px rgba(96,165,250,0.4)}

/* Advanced UI Components */
.badge-band{
  background:rgba(23,35,64,0.6);border:1px solid rgba(96,165,250,0.15);
  border-radius:4px;padding:6px 10px;display:inline-flex;flex-direction:column;gap:3px;
  min-width:120px;box-shadow:0 2px 4px rgba(0,0,0,0.2)}
.band-label{font-size:9px;color:#7E9BB8;text-transform:uppercase;letter-spacing:0.05em;font-weight:700}
.band-value{font-size:11px;color:#FFFFFF;font-weight:800;letter-spacing:0.02em}

.status-pill{
  display:inline-flex;align-items:center;padding:5px 12px;border-radius:20px;
  background:rgba(0,0,0,0.15);border:1px solid rgba(255,255,255,0.08);
  font-size:11px;font-weight:700;letter-spacing:0.03em;color:#FFFFFF}
.status-pill .status-indicator{margin-right:8px;width:6px;height:6px}

.pagination-bar{
  display:flex;align-items:center;justify-content:space-between;
  padding:16px 20px;background:rgba(96,165,250,0.03);border-top:1px solid rgba(96,165,250,0.08);
  border-radius:0 0 4px 4px}
.pagination-info{font-size:12px;color:#4A6080;font-weight:500}
.pagination-info strong{color:#FFFFFF}

.ccy-tag{
  background:rgba(96,165,250,0.12);color:#60A5FA;padding:2px 6px;border-radius:3px;
  font-size:9px;font-weight:800;font-family:'JetBrains Mono',monospace;border:1px solid rgba(96,165,250,0.2)}



/* Reasoning block */
.reasoning-block{background:#0A1020;border:1px solid rgba(96,165,250,0.10);border-radius:3px;
  padding:20px 24px;font-family:'JetBrains Mono',monospace;font-size:12.5px;
  line-height:1.85;color:#FFFFFF!important;max-width:660px}
.reasoning-label{font-size:10px;font-weight:700;color:#60A5FA;
  letter-spacing:0.1em;text-transform:uppercase;margin-bottom:6px}
.reasoning-divider{height:1px;background:rgba(96,165,250,0.08);margin:16px 0}

/* Agent 2 — chat answer markdown polish
   These style the native Streamlit markdown that renders inside the
   blue-bar container we inject with st.markdown HTML around it.
   Target: all stMarkdownContainer blocks that follow an investigator header */
[data-testid="stMarkdownContainer"] ol{
  padding-left:20px!important;margin:6px 0 10px!important}
[data-testid="stMarkdownContainer"] ol li{
  color:#D4E4F7!important;font-size:13.5px!important;
  line-height:1.75!important;margin-bottom:6px!important}
[data-testid="stMarkdownContainer"] ul{
  padding-left:18px!important;margin:6px 0 10px!important}
[data-testid="stMarkdownContainer"] ul li{
  color:#D4E4F7!important;font-size:13.5px!important;
  line-height:1.75!important;margin-bottom:5px!important}
[data-testid="stMarkdownContainer"] strong,
[data-testid="stMarkdownContainer"] b{color:#FFFFFF!important;font-weight:700!important}
/* ── All main-area buttons ───────────────────────────────────────────── */
div[data-testid="stColumn"] .stButton button,
.main .stButton button{
  background:#132040!important;border:1px solid rgba(96,165,250,0.18)!important;
  border-radius:6px!important;color:#C8D8F0!important;font-size:12px!important;
  font-weight:500!important;letter-spacing:0.01em!important;
  text-transform:none!important;padding:8px 14px!important;
  transition:all 0.15s!important;text-align:left!important;
  white-space:normal!important;height:auto!important;line-height:1.5!important}
div[data-testid="stColumn"] .stButton button:hover,
.main .stButton button:hover{background:#1A2E4A!important;border-color:#60A5FA!important;color:#FFFFFF!important}
.main .stButton button:active{background:rgba(96,165,250,0.12)!important}

/* ── Slider ──────────────────────────────────────────────────────────── */
.main [data-testid="stSlider"] label,
.main [data-testid="stSlider"] [data-testid="stWidgetLabel"],
.main [data-testid="stSlider"] div[data-testid="stWidgetLabel"] *{color:#7E9BB8!important;font-size:11px!important}
.main [data-testid="stSlider"] [data-baseweb="slider"] [role="slider"]{background:#60A5FA!important;border-color:#60A5FA!important}
.main [data-testid="stSlider"] [data-baseweb="slider"] div[class*="Track"]{background:rgba(96,165,250,0.15)!important}

/* ── Selectbox ───────────────────────────────────────────────────────── */
.main [data-testid="stSelectbox"] label,
.main [data-testid="stSelectbox"] [data-testid="stWidgetLabel"] *{color:#7E9BB8!important;font-size:11px!important}
div[data-testid="stSelectbox"] [data-baseweb="select"],
div[data-testid="stSelectbox"] [data-baseweb="select"] > div,
div[data-testid="stSelectbox"] [data-baseweb="select"] > div > div,
div[data-testid="stSelectbox"] [data-baseweb="select"] [class*="ValueContainer"],
div[data-testid="stSelectbox"] [data-baseweb="select"] [class*="SingleValue"],
div[data-testid="stSelectbox"] [data-baseweb="select"] [class*="Placeholder"],
div[data-testid="stSelectbox"] [data-baseweb="select"] [class*="control"],
div[data-testid="stSelectbox"] [data-baseweb="select"] [class*="container"]{
  background:#132040!important;border-color:rgba(96,165,250,0.2)!important;color:#FFFFFF!important}
div[data-testid="stSelectbox"] [data-baseweb="select"] *{background:#132040!important;color:#FFFFFF!important}

/* ── Plotly ──────────────────────────────────────────────────────────── */
.main [data-testid="stPlotlyChart"]{border:none!important;background:transparent!important}
.js-plotly-plot .plotly .modebar{background:rgba(23,35,64,0.8)!important}
.js-plotly-plot .plotly .modebar-btn path{fill:#7E9BB8!important}
.js-plotly-plot .plotly .modebar-btn:hover path{fill:#60A5FA!important}

/* ── Kill ALL white Streamlit wrapper backgrounds ────────────────────── */
.main [data-testid="stVerticalBlock"],
.main [data-testid="stVerticalBlockBorderWrapper"],
.main [data-testid="stHorizontalBlock"]{background:transparent!important}

/* ── Expander — dark themed ──────────────────────────────────────────── */
.main [data-testid="stExpander"]{
  background:#0F1E35!important;border:1px solid rgba(96,165,250,0.10)!important;
  border-radius:6px!important}
.main [data-testid="stExpander"] summary{
  color:#7E9BB8!important;font-size:11px!important;font-weight:600!important;
  padding:10px 14px!important;list-style:none!important}
.main [data-testid="stExpander"] summary:hover{color:#60A5FA!important}
.main [data-testid="stExpander"] > div > div{background:transparent!important}

/* ── Chat input — round pill, premium dark ───────────────────────────── */
[data-testid="stBottom"],[data-testid="stBottom"]>div,[data-testid="stBottom"]>div>div{
  background:#1A2E4A!important;border-top:1px solid rgba(96,165,250,0.08)!important}
[data-testid="stChatInput"] textarea,
[data-testid="stChatInputContainer"] textarea{
  background:#0F1A2E!important;color:#C8D8F0!important;
  font-size:13px!important;border:none!important;outline:none!important}
[data-testid="stChatInput"] textarea::placeholder,
[data-testid="stChatInputContainer"] textarea::placeholder{color:#4A6080!important}
[data-testid="stChatInput"] > div,
[data-testid="stChatInputContainer"] > div{
  background:#0F1A2E!important;
  border:1px solid rgba(96,165,250,0.2)!important;
  border-radius:24px!important;
  padding:2px 8px!important}
[data-testid="stChatInput"] > div:focus-within,
[data-testid="stChatInputContainer"] > div:focus-within{
  border-color:rgba(96,165,250,0.45)!important;
  box-shadow:0 0 0 2px rgba(96,165,250,0.08)!important}
[data-testid="stChatInput"] button,
[data-testid="stChatInputContainer"] button{
  background:rgba(96,165,250,0.12)!important;border-radius:50%!important;
  border:none!important;color:#60A5FA!important;
  text-transform:none!important;letter-spacing:0!important}
[data-testid="stChatInput"] button:hover,
[data-testid="stChatInputContainer"] button:hover{background:rgba(96,165,250,0.25)!important}
/* Restore Material Symbols font on the submit button icon span so the arrow
   ligature renders correctly instead of showing raw "_arrow_right" text */
[data-testid="stChatInputSubmitButton"] span,
[data-testid="stChatInputSubmitButton"] *{
  font-family:'Material Symbols Rounded'!important}

/* Sidebar labels */
.sidebar-section,
section[data-testid="stSidebar"] .stMarkdownContainer .sidebar-section,
section[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] span,
section[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] *{
  color:#FFFFFF!important}
.sidebar-footer{font-size:10px;color:#4A6080!important;line-height:1.8;
  padding:12px 20px;border-top:1px solid rgba(96,165,250,0.06);
  margin-top:8px;font-family:'JetBrains Mono',monospace!important}
</style>
""", unsafe_allow_html=True)


# ── Session state ──────────────────────────────────────────────────────────────
for _k, _v in [
    ("agent1_results",    None),
    ("agent2_context",    None),
    ("agent3_assessment", None),
    ("chat_history",      []),
    ("pipeline_status",   {}),
    ("gold_stats",        None),
    ("hunt_page",         1),
    ("hunt_search_query", ""),
    ("hunt_alert_filter", "ALL"),
    ("hunt_bank_filter",  []),
    ("hunt_ccy_filter",   []),
]:


    if _k not in st.session_state:
        st.session_state[_k] = _v


# ── Pipeline status checker ────────────────────────────────────────────────────
def check_pipeline_status() -> dict:
    from databricks import sql as dbsql

    status = {}

    bronze_path = r"C:\FinPulse Project\data\bronze\transactions"
    if os.path.exists(bronze_path) and len(os.listdir(bronze_path)) > 0:
        status["Bronze"] = ("green", "Partitions present")
    else:
        status["Bronze"] = ("red", "No partitions found")

    marker = r"C:\FinPulse Project\data\bronze\.last_processed"
    if os.path.exists(marker):
        with open(marker) as f:
            date = f.read().strip()
        status["Silver"] = ("green", f"Processed {date}")
    else:
        status["Silver"] = ("amber", "Not processed")

    try:
        conn = dbsql.connect(
            server_hostname=os.getenv("DATABRICKS_HOST"),
            http_path=os.getenv("DATABRICKS_HTTP_PATH"),
            access_token=os.getenv("DATABRICKS_TOKEN"),
        )
        cursor = conn.cursor()
        schema = os.getenv("DATABRICKS_SCHEMA", "finpulse")
        cursor.execute(f"SHOW TABLES IN {schema}")
        tables = [row[1] for row in cursor.fetchall()]
        gold_count = len([t for t in tables if t not in ["transactions_silver", "stocks_silver"]])
        status["Gold"] = ("green", f"{gold_count} / 9") if gold_count >= 9 else ("amber", f"{gold_count} / 9")
        status["Soda"] = ("green", "Pass") if gold_count >= 9 else ("grey", "Unknown")
        cursor.execute("SHOW SCHEMAS")
        schemas = [row[0] for row in cursor.fetchall()]
        status["Elementary"] = ("green", "Active") if "finpulse_elementary" in schemas else ("amber", "Not init")
        cursor.close()
        conn.close()
    except Exception:
        for layer in ["Gold", "Soda", "Elementary"]:
            if layer not in status:
                status[layer] = ("grey", "Warehouse sleeping")

    return status


# ── Gold layer data loaders (Databricks SQL Warehouse) ─────────────────────────
def _db_query(sql: str) -> "pd.DataFrame":
    """Run a SQL query against the Databricks SQL Warehouse and return a DataFrame."""
    import pandas as pd
    from config import DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN, DATABRICKS_SCHEMA
    from databricks import sql as dbsql

    conn = dbsql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    )
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
        return pd.DataFrame(rows, columns=cols)
    finally:
        cursor.close()
        conn.close()


@st.cache_data(ttl=300, show_spinner=False)
def load_gold_stats() -> dict:
    """
    Query the Gold layer tables in Databricks and return a unified stats dict.
    All values fall back to None on error — UI degrades gracefully with '—'.

    Keys returned:
      total_transactions, total_fraud, fraud_rate_pct,
      flagged_accounts, total_exposure,
      fraud_by_type (list of dicts),
      top_risk_accounts (list of dicts),
      stock_rankings (list of dicts),
      gold_table_count,
      contamination, error (optional)
    """
    try:
        import pandas as pd
        from config import DATABRICKS_SCHEMA, FRAUD_CONTAMINATION_RATE

        s = DATABRICKS_SCHEMA  # e.g. "finpulse"

        # ── 1. Fraud rate by type (aggregate across all types) ──────────────
        df_fraud = _db_query(f"""
            SELECT
                SUM(total_transactions) AS total_transactions,
                SUM(fraud_count)        AS total_fraud
            FROM {s}.fraud_rate_by_type
        """)
        if not df_fraud.empty:
            total_tx    = int(df_fraud["total_transactions"].iloc[0] or 0)
            total_fraud = int(df_fraud["total_fraud"].iloc[0] or 0)
            fraud_rate  = (total_fraud / total_tx) if total_tx else None
        else:
            total_tx, total_fraud, fraud_rate = None, None, None

        # ── 2. Fraud breakdown by transaction type ──────────────────────────
        df_by_type = _db_query(f"""
            SELECT Payment_Format as type, total_transactions, fraud_count, fraud_rate_pct
            FROM {s}.fraud_rate_by_type
            ORDER BY fraud_count DESC
        """)
        if not df_by_type.empty:
            for col in ["total_transactions", "fraud_count", "fraud_rate_pct"]:
                if col in df_by_type.columns:
                    df_by_type[col] = df_by_type[col].apply(lambda x: float(x) if x is not None else 0.0)
            fraud_by_type = df_by_type.to_dict("records")
        else:
            fraud_by_type = []

        # ── 3. High-risk accounts ────────────────────────────────────────────
        df_risk = _db_query(f"""
            SELECT account_id, total_transactions,
                   indicator_count as risk_indicator_count, total_amount_transacted
            FROM {s}.high_risk_accounts
            ORDER BY indicator_count DESC, total_amount_transacted DESC
            LIMIT 20
        """)
        if not df_risk.empty:
            df_risk["total_amount_transacted"] = df_risk["total_amount_transacted"].apply(
                lambda x: float(x) if x is not None else 0.0
            )
            flagged_accounts = len(df_risk)
            total_exposure   = float(df_risk["total_amount_transacted"].sum())
            top_risk         = df_risk.head(10).to_dict("records")
        else:
            flagged_accounts, total_exposure, top_risk = None, None, []

        # ── 4. Stock performance ranking ─────────────────────────────────────
        df_stocks = _db_query(f"""
            SELECT ticker, start_price, latest_price,
                   total_return_pct, avg_daily_return_pct,
                   trading_days, performance_rank
            FROM {s}.stocks_performance_ranking
            ORDER BY performance_rank ASC
        """)
        if not df_stocks.empty:
            for col in ["start_price", "latest_price", "total_return_pct", "avg_daily_return_pct"]:
                if col in df_stocks.columns:
                    df_stocks[col] = df_stocks[col].apply(lambda x: float(x) if x is not None else 0.0)
            stock_rankings = df_stocks.to_dict("records")
        else:
            stock_rankings = []

        # ── 5. Hourly trends (fraud concentration) ────────────────────────
        df_hourly = _db_query(f"""
            SELECT hour_of_day as hour, transaction_count as total_transactions, fraud_count
            FROM {s}.hourly_pattern_analysis
            ORDER BY hour ASC
        """)

        # ── 6. Gold table count ──────────────────────────────────────────────
        df_tables = _db_query(f"SHOW TABLES IN {s}")
        gold_tables = [
            r for r in df_tables.iloc[:, 1].tolist()
            if r not in ("transactions_silver", "stocks_silver")
        ] if not df_tables.empty else []
        gold_table_count = len(gold_tables)

        return {
            "total_transactions": total_tx,
            "total_fraud":        total_fraud,
            "fraud_rate_pct":     fraud_rate,
            "flagged_accounts":   flagged_accounts,
            "total_exposure":     total_exposure,
            "fraud_by_type":      fraud_by_type,
            "top_risk_accounts":  top_risk,
            "stock_rankings":     stock_rankings,
            "gold_table_count":   gold_table_count,
            "contamination":      FRAUD_CONTAMINATION_RATE,
        }

    except Exception as e:
        try:
            from config import FRAUD_CONTAMINATION_RATE
        except Exception:
            FRAUD_CONTAMINATION_RATE = 0.013
        return {
            "total_transactions": None, "total_fraud": None, "fraud_rate_pct": None,
            "flagged_accounts":   None, "total_exposure": None,
            "fraud_by_type":      [],   "top_risk_accounts": [], "stock_rankings": [],
            "gold_table_count":   None, "contamination": FRAUD_CONTAMINATION_RATE,
            "error": str(e),
        }


# ── Format helpers ─────────────────────────────────────────────────────────────
def fmt_millions(n) -> str:
    if n is None:
        return "—"
    n = int(n)
    if n >= 1_000_000:
        return f"{n/1_000_000:.2f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(n)


def fmt_pct(rate, already_pct=False) -> str:
    if rate is None:
        return "—"
    return f"{rate:.4f}%" if already_pct else f"{rate*100:.4f}%"


def fmt_currency(val) -> str:
    if val is None:
        return "—"
    val = float(val)
    if val >= 1_000_000:
        return f"${val/1_000_000:.2f}M"
    if val >= 1_000:
        return f"${val/1_000:.1f}K"
    return f"${val:,.0f}"


# ── Helpers ────────────────────────────────────────────────────────────────────
def kpi(label: str, value: str, delta: str = "", accent: str = "") -> str:
    accent_cls = f" accent-{accent}" if accent else ""
    delta_html = f'<div class="kpi-delta">{delta}</div>' if delta else ""
    return f"""
    <div class="kpi-card{accent_cls}">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        {delta_html}
    </div>
    """


def pending_placeholder(title: str, desc: str, features: list) -> None:
    items = "".join(
        f'<div class="feature-item"><span class="feature-dot"></span>{f}</div>'
        for f in features
    )
    st.markdown(f"""
    <div class="pending-card">
        <div class="pending-indicator"></div>
        <div class="pending-title">{title}</div>
        <div class="pending-desc">{desc}</div>
        <div class="feature-grid">{items}</div>
    </div>
    """, unsafe_allow_html=True)


def screen_header(tag: str, title: str, subtitle: str) -> None:
    st.markdown(f"""
    <div class="screen-header">
        <span class="screen-tag">{tag}</span>
        <h1>{title}</h1>
        <p>{subtitle}</p>
    </div>
    """, unsafe_allow_html=True)


def guard_message(msg: str) -> None:
    st.markdown(f'<div class="guard-msg">{msg}</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
#                              SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:

    st.markdown("""
    <div style="
        margin: -1rem -1rem 0 -1rem;
        padding: 22px 20px 18px 20px;
        background: #0D1525;
        border-bottom: 1px solid rgba(96,165,250,0.12);
    ">
        <div style="font-size:13px;font-weight:800;color:#FFFFFF!important;
                    letter-spacing:0.22em;text-transform:uppercase;">FINPULSE</div>
        <div style="font-size:9px;color:#4A6080!important;text-transform:uppercase;
                    letter-spacing:0.16em;margin-top:5px;font-weight:600;">
            Compliance Intelligence
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<span class="sidebar-section">Modules</span>', unsafe_allow_html=True)

    screen = st.radio(
        label="​",
        options=["Fraud Detection", "Account Investigation", "Risk Assessment", "Executive Briefing"],
        label_visibility="collapsed",
    )

    st.markdown('<span class="sidebar-section">Pipeline Health</span>', unsafe_allow_html=True)

    if st.button("Refresh Status", use_container_width=True):
        with st.spinner(""):
            st.session_state.pipeline_status = check_pipeline_status()

    if st.session_state.pipeline_status:
        bar_map = {"green": "bar-green", "amber": "bar-amber", "red": "bar-red", "grey": "bar-grey"}
        rows = ""
        for layer, (color, detail) in st.session_state.pipeline_status.items():
            rows += f"""
            <div class="layer-row">
                <span class="layer-name">{layer}</span>
                <div class="layer-bar {bar_map.get(color, 'bar-grey')}"></div>
                <span class="layer-detail">{detail}</span>
            </div>
            """
        st.markdown(f'<div style="padding: 2px 20px 8px 20px;">{rows}</div>', unsafe_allow_html=True)
    else:
        st.markdown(
            '<p style="color:#4A6080; font-size:10px; padding: 0 20px 8px 20px; line-height:1.6; margin:0;">'
            'Click refresh to poll pipeline layers.</p>',
            unsafe_allow_html=True,
        )

    st.markdown(
        f'<div class="sidebar-footer">'
        f'Historical reporting: Power BI<br>'
        f'Session: {datetime.now().strftime("%Y-%m-%d %H:%M")}'
        f'</div>',
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
#                            SCREEN ROUTER
# ══════════════════════════════════════════════════════════════════════════════

# ── Load live stats — lazy, cached in session_state after first fetch ──────────
# Session state caches the dict so reruns (slider moves, button clicks)
# return instantly without re-entering load_gold_stats().
# The @st.cache_data TTL on load_gold_stats() handles the 5-min refresh cycle.
if "gold_stats" not in st.session_state:
    st.session_state.gold_stats = None  # mark as not yet loaded

def _get_stats() -> dict:
    """Return cached stats from session_state, or fetch if first time."""
    if st.session_state.gold_stats is None:
        st.session_state.gold_stats = load_gold_stats()
    return st.session_state.gold_stats

def _unpack_stats(s: dict):
    """Unpack the stats dict into module-level convenience variables."""
    total_tx       = fmt_millions(s.get("total_transactions"))
    total_fraud    = s.get("total_fraud")
    fraud_rate     = fmt_pct(s.get("fraud_rate_pct"))
    flagged_accts  = s.get("flagged_accounts")
    total_exposure = fmt_currency(s.get("total_exposure"))
    contamination  = s.get("contamination", 0.013)
    gold_count     = s.get("gold_table_count")
    fraud_by_type  = s.get("fraud_by_type", [])
    top_risk       = s.get("top_risk_accounts", [])
    stock_rankings = s.get("stock_rankings", [])
    data_error     = s.get("error")
    fraud_labeled  = f"{int(total_fraud):,} confirmed laundering" if total_fraud is not None else "Gold layer"
    tx_delta       = "Gold · IBM AML dataset" if s.get("total_transactions") else "Databricks unavailable"
    gold_coverage  = f"{gold_count} / 9" if gold_count is not None else "—"
    return (total_tx, total_fraud, fraud_rate, flagged_accts, total_exposure,
            contamination, gold_count, fraud_by_type, top_risk, stock_rankings,
            data_error, fraud_labeled, tx_delta, gold_coverage)

# ── HUNT ──────────────────────────────────────────────────────────────────────
if screen == "Fraud Detection":
    screen_header(
        "Fraud Detection",
        "Suspicious Transaction Screening",
        "Automatically analyses transaction behaviour across all accounts and payment channels. "
        "The system looks for unusual patterns — unexpected transfer sizes, currency switches, "
        "activity on known high-risk routes, and combinations of factors that don't fit "
        "normal business behaviour. Results are ranked by severity so your team can "
        "focus on the cases that matter most.",
    )

    # Load Gold stats — shows spinner only on cold cache (first load per session)
    _stats_loaded = st.session_state.gold_stats is not None
    if not _stats_loaded:
        with st.spinner("Connecting to Databricks Gold layer..."):
            _s = _get_stats()
    else:
        _s = _get_stats()

    (_total_tx, _total_fraud, _fraud_rate, _flagged_accts, _total_exposure,
     _contamination, _gold_count, _fraud_by_type, _top_risk, _stock_rankings,
     _data_error, _fraud_labeled, _tx_delta, _gold_coverage) = _unpack_stats(_s)

    if _data_error:
        st.sidebar.markdown(
            f'<p style="color:#F87171;font-size:9px;padding:4px 20px;font-family:monospace;'
            f'line-height:1.5;margin:0;">Warehouse: {_data_error[:80]}</p>',
            unsafe_allow_html=True,
        )

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(kpi("Transactions Scanned", _total_tx, _tx_delta), unsafe_allow_html=True)
    with c2:
        st.markdown(kpi("Confirmed Laundering", fmt_millions(_total_fraud), _fraud_labeled, "red"), unsafe_allow_html=True)
    with c3:
        st.markdown(kpi("Laundering Rate", _fraud_rate, "Across all payment formats"), unsafe_allow_html=True)
    with c4:
        st.markdown(kpi("Contamination Param", f"{_contamination:.3f}", "Domain-calibrated", "amber"), unsafe_allow_html=True)

    # ── Fraud breakdown by type (live from Gold) ──────────────────────────
    if _fraud_by_type:
        st.markdown('<div class="section-label">Laundering Rate by Payment Format</div>', unsafe_allow_html=True)
        st.markdown("""
        <div style="background:rgba(96,165,250,0.06);border-left:3px solid rgba(96,165,250,0.4);
                    border-radius:0 3px 3px 0;padding:11px 16px;margin-bottom:14px;
                    font-size:12.5px;color:#7E9BB8;line-height:1.7;">
            Rates are calculated across the full transaction history.
            <strong style="color:#FFFFFF;">Wire and Reinvestment currently show 0% because no confirmed
            suspicious activity has been recorded on those channels in the source data</strong> —
            this reflects the data as received, not a system limitation.
            Automated Clearing House (ACH) transfers carry the highest confirmed rate at 0.75% and represent the primary area of concern.
        </div>
        """, unsafe_allow_html=True)
        rows_html = ""
        for row in _fraud_by_type:
            rate_val = row.get("fraud_rate_pct", 0) or 0
            badge_cls = "badge-red" if rate_val > 0.1 else "badge-amber" if rate_val > 0 else "badge-green"
            row_cls = "row-critical" if rate_val > 0.1 else "row-warn" if rate_val > 0 else ""
            status_dot = "bg-red" if rate_val > 0.1 else "bg-amber" if rate_val > 0 else "bg-blue"
            
            rows_html += f"""
            <tr class="{row_cls}">
                <td>
                    <span class="status-indicator {status_dot}"></span>
                    <span class="mono">{row.get('type','—')}</span>
                </td>
                <td class="mono">{int(row.get('total_transactions') or 0):,}</td>
                <td class="mono">{int(row.get('fraud_count') or 0):,}</td>
                <td><span class="badge {badge_cls}">{rate_val:.4f}%</span></td>
            </tr>"""
        st.markdown(f"""
        <div class="fp-card" style="padding:0;overflow-x:auto;">
            <table class="data-table">
                <thead><tr>
                    <th>Payment Format</th><th>Total</th>
                    <th>Laundering Count</th><th>Laundering Rate</th>
                </tr></thead>
                <tbody>{rows_html}</tbody>
            </table>
        </div>""", unsafe_allow_html=True)


    st.markdown('<div class="section-label">Run Screening</div>', unsafe_allow_html=True)
    run_hunt()


# ── INVESTIGATE ───────────────────────────────────────────────────────────────
elif screen == "Account Investigation":
    screen_header(
        "Account Investigation",
        "Investigate Suspicious Accounts",
        "Select a flagged account and ask the AI investigator about the transaction behaviour, "
        "money laundering patterns, and market context. Each response brings a new angle — "
        "which banks were involved, what payment formats were used, and how the cross-currency "
        "or high-value wire activity fits known AML typologies.",
    )

    _stats_loaded = st.session_state.gold_stats is not None
    if not _stats_loaded:
        with st.spinner("Connecting to Databricks Gold layer..."):
            _s = _get_stats()
    else:
        _s = _get_stats()
    (_total_tx, _total_fraud, _fraud_rate, _flagged_accts, _total_exposure,
     _contamination, _gold_count, _fraud_by_type, _top_risk, _stock_rankings,
     _data_error, _fraud_labeled, _tx_delta, _gold_coverage) = _unpack_stats(_s)

    if st.session_state.agent1_results is None:
        _flagged_str = str(_flagged_accts) if _flagged_accts is not None else "—"
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown(kpi("Accounts Needing Review", _flagged_str, "Flagged as potentially suspicious"), unsafe_allow_html=True)
        with c2:
            st.markdown(kpi("Total Money at Risk", _total_exposure, "Across all suspicious transactions"), unsafe_allow_html=True)
        with c3:
            st.markdown(kpi("AI Investigator", "Ready", "Ask questions in plain English"), unsafe_allow_html=True)

        guard_message(
            "<strong>Run Fraud Detection first</strong> — "
            "Go to the Fraud Detection page and click 'Run Detection'. "
            "This scores every account with our AI model. Once done, come back here to investigate individual accounts. "
            "The accounts below are already flagged from our database — Fraud Detection adds extra AI scoring on top."
        )

        # ── Top high-risk accounts table (live from Gold) ─────────────────
        if _top_risk:
            st.markdown('<div class="section-label">High-Risk Accounts — Gold Layer</div>', unsafe_allow_html=True)
            rows_html = ""
            for row in _top_risk:
                rows_html += f"""
                <tr class="row-critical">
                    <td>
                        <span class="status-indicator bg-red"></span>
                        <span class="mono">{row.get('account_id','—')}</span>
                    </td>
                    <td class="mono">{int(row.get('total_transactions') or 0):,}</td>
                    <td class="mono">{int(row.get('risk_indicator_count') or 0):,}</td>
                    <td><span class="amount">{fmt_currency(row.get('total_amount_transacted'))}</span></td>
                    <td><span class="risk-chip chip-red">FLAGGED</span></td>
                </tr>"""
            st.markdown(f"""
            <div class="fp-card" style="padding:0;overflow-x:auto;">
                <table class="data-table">
                    <thead><tr>
                        <th>Account ID</th><th>Transactions</th>
                        <th>Risk Indicators</th><th>Total Transacted</th><th>Status</th>
                    </tr></thead>
                    <tbody>{rows_html}</tbody>
                </table>
            </div>""", unsafe_allow_html=True)


    else:
        if run_investigate is not None:
            run_investigate(st.session_state.agent1_results)
        else:
            guard_message("Agent 2 module not found — ensure agent2_investigate.py is in ai_layer/.")


# ── REASON ────────────────────────────────────────────────────────────────────
elif screen == "Risk Assessment":
    screen_header(
        "Regulatory Risk Classification",
        "Risk Assessment",
        "Applies banking compliance rules to each flagged account and recommends "
        "a regulatory action — from filing a Suspicious Activity Report (SAR) to "
        "clearing the account. Every decision is backed by a visible chain of reasoning.",
    )

    if st.session_state.agent2_context is None:
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown(kpi("LLM Engine", "DeepSeek R1", "via OpenRouter"), unsafe_allow_html=True)
        with c2:
            st.markdown(kpi("Regulatory Framework", "BSA / AML", "Bank Secrecy Act", "amber"), unsafe_allow_html=True)
        with c3:
            st.markdown(kpi("Output Actions", "5", "SAR · FREEZE · ESCALATE · MONITOR · CLEAR"), unsafe_allow_html=True)

        guard_message(
            "<strong>Awaiting Investigation Context</strong> — "
            "Complete Fraud Detection and Account Investigation first. "
            "Risk Assessment requires account-level context to perform regulatory classification."
        )

        pending_placeholder(
            title="Chain-of-Thought Reasoner — Pending Build",
            desc="DeepSeek R1 will stream live reasoning steps here once Fraud Detection and Account Investigation "
                 "have produced account-level context.",
            features=[
                "Token-by-token streaming output",
                "BSA/AML regulatory classification",
                "Dynamic risk confidence meter",
                "Structured JSON action output",
                "SAR · FREEZE · ESCALATE · MONITOR · CLEAR",
                "Audit-ready reasoning trace",
            ],
        )

    else:
        pending_placeholder(
            title="Streaming Reasoner — Pending Build",
            desc="Live chain-of-thought reasoning with dynamic risk classification.",
            features=[
                "Token-by-token streaming",
                "Dynamic risk meter",
                "Regulatory classification",
                "Structured JSON output",
            ],
        )


# ── BRIEF ─────────────────────────────────────────────────────────────────────
elif screen == "Executive Briefing":
    screen_header(
        "Board-Ready Compliance Summary",
        "Executive Briefing",
        "Consolidates all findings into a concise report suitable for senior management "
        "and regulatory review. Covers confirmed fraud, flagged accounts, recommended "
        "actions, and market context — exportable as a timestamped PDF.",
    )

    _stats_loaded = st.session_state.gold_stats is not None
    if not _stats_loaded:
        with st.spinner("Connecting to Databricks Gold layer..."):
            _s = _get_stats()
    else:
        _s = _get_stats()
    (_total_tx, _total_fraud, _fraud_rate, _flagged_accts, _total_exposure,
     _contamination, _gold_count, _fraud_by_type, _top_risk, _stock_rankings,
     _data_error, _fraud_labeled, _tx_delta, _gold_coverage) = _unpack_stats(_s)

    if st.session_state.agent3_assessment is None:
        # Risk banner driven by whether fraud exists in Gold data
        if _total_fraud and _total_fraud > 0:
            banner_cls, banner_txt = "risk-high", f"Confirmed Fraud Detected — {fmt_millions(_total_fraud)} transactions flagged"
        elif _flagged_accts and _flagged_accts > 0:
            banner_cls, banner_txt = "risk-medium", f"{_flagged_accts} High-Risk Accounts — Awaiting Agent Classification"
        else:
            banner_cls, banner_txt = "risk-medium", "Risk Status Indeterminate — Complete pipeline to assess"

        st.markdown(f'<div class="risk-banner {banner_cls}">{banner_txt}</div>', unsafe_allow_html=True)

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.markdown(kpi("Transactions Reviewed", _total_tx, _tx_delta), unsafe_allow_html=True)
        with c2:
            st.markdown(kpi("High-Risk Accounts", str(_flagged_accts) if _flagged_accts is not None else "—", "Gold · high_risk_accounts", "red"), unsafe_allow_html=True)
        with c3:
            st.markdown(kpi("Exposure at Risk", _total_exposure, "Discrepancy transactions", "amber"), unsafe_allow_html=True)
        with c4:
            st.markdown(kpi("Gold Models", _gold_coverage, "9 target · dbt materialized", "green"), unsafe_allow_html=True)

        guard_message(
            "<strong>Awaiting Agent Assessment</strong> — "
            "Gold layer data is live below. Run Fraud Detection → Account Investigation → Risk Assessment "
            "to produce the AI-generated executive briefing and SAR recommendations."
        )

        # ── High-risk accounts table (live) ──────────────────────────────
        if _top_risk:
            st.markdown('<div class="section-label">High-Risk Accounts — Priority Action Required</div>', unsafe_allow_html=True)
            rows_html = ""
            for row in _top_risk:
                rows_html += f"""
                <tr class="row-critical">
                    <td>
                        <span class="status-indicator bg-red"></span>
                        <span class="mono">{row.get('account_id','—')}</span>
                    </td>
                    <td class="mono">{int(row.get('total_transactions') or 0):,}</td>
                    <td class="mono">{int(row.get('risk_indicator_count') or 0):,}</td>
                    <td><span class="amount">{fmt_currency(row.get('total_amount_transacted'))}</span></td>
                    <td><span class="risk-chip chip-red">HIGH</span></td>
                    <td><span class="badge badge-amber">Pending Agent 03</span></td>
                </tr>"""
            st.markdown(f"""
            <div class="fp-card" style="padding:0;overflow-x:auto;">
                <table class="data-table">
                    <thead><tr>
                        <th>Account ID</th><th>Transactions</th>
                        <th>Risk Indicators</th><th>Exposure</th>
                        <th>Risk Level</th><th>Recommended Action</th>
                    </tr></thead>
                    <tbody>{rows_html}</tbody>
                </table>
            </div>""", unsafe_allow_html=True)


        # ── Stock performance (live) ──────────────────────────────────────
        if _stock_rankings:
            st.markdown('<div class="section-label">Market Context — Stock Performance Ranking</div>', unsafe_allow_html=True)
            rows_html = ""
            for row in _stock_rankings:
                ret = row.get("total_return_pct") or 0
                ret_cls = "chip-green" if ret >= 0 else "chip-red"
                row_cls = "row-info" if ret >= 0 else "row-warn"
                status_dot = "bg-blue" if ret >= 0 else "bg-amber"
                sign = "+" if ret >= 0 else ""
                rows_html += f"""
                <tr class="{row_cls}">
                    <td>
                        <span class="status-indicator {status_dot}"></span>
                        <span class="mono" style="color:#60A5FA">{row.get('ticker','—')}</span>
                    </td>
                    <td class="mono">${row.get('start_price') or 0:.2f}</td>
                    <td class="mono">${row.get('latest_price') or 0:.2f}</td>
                    <td><span class="risk-chip {ret_cls}">{sign}{ret:.2f}%</span></td>
                    <td class="mono">{int(row.get('trading_days') or 0):,}d</td>
                    <td class="mono">#{int(row.get('performance_rank') or 0)}</td>
                </tr>"""
            st.markdown(f"""
            <div class="fp-card" style="padding:0;overflow-x:auto;">
                <table class="data-table">
                    <thead><tr>
                        <th>Ticker</th><th>Start Price</th><th>Latest Price</th>
                        <th>Total Return</th><th>Trading Days</th><th>Rank</th>
                    </tr></thead>
                    <tbody>{rows_html}</tbody>
                </table>
            </div>""", unsafe_allow_html=True)


    else:
        pending_placeholder(
            title="Executive Report — Pending Build",
            desc="Full compliance briefing with one-click PDF export.",
            features=[
                "Risk status summary",
                "KPI snapshot",
                "Priority action table",
                "PDF export with audit trail",
            ],
        )
