"""
FinPulse — Agent 1: HUNT (IBM AML Edition)
=========================================
File: ai_layer/agent1_hunt.py
Purpose: detect anomalies in IBM AML transactions using Isolation Forest & Rules.

Smart techniques used:
- Stratified sampling: all confirmed laundering + TABLESAMPLE 10% normal rows
- 8-feature IBM AML domain engineering (amount, log_amount, is_cross_currency,
  is_high_value_wire, is_wire, is_ach, amount_x_cross_ccy, amount_x_wire)
- StandardScaler before Isolation Forest — prevents amount dominating binary flags
- Domain contamination calibration: 0.013 (actual IBM AML laundering base rate)
- Hybrid defense-in-depth: Isolation Forest scores ∩ 3-rule domain engine
- 5-bucket percentile banding for analyst-ready anomaly score
- Bank-pair risk: from_bank/to_bank combo enriched from cross_bank_flow Gold table
- Entity-type context from accounts_silver (Corporation / Partnership / Sole Prop)
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import os
import sys
from pathlib import Path
from datetime import datetime
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

_PROJECT_ROOT = Path(__file__).parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

@st.cache_data(ttl=600, show_spinner=False)
def _run_full_pipeline(contamination: float, type_filter: str) -> pd.DataFrame:
    df = load_aml_data()
    if type_filter != "ALL":
        df = df[df["Payment_Format"] == type_filter].copy()
    df = engineer_features(df)
    df = run_isolation_forest(df, contamination)
    df = apply_rules(df)
    df = assign_percentile_bands(df)
    return df


@st.cache_data(ttl=600, show_spinner=False)
def load_aml_data() -> pd.DataFrame:
    """
    Stratified sample from aml_risk_indicators:
      - ALL confirmed laundering rows (is_fraud = 1)
      - 10% TABLESAMPLE of normal rows, capped at 50K

    Also pulls bank-pair risk from cross_bank_flow and entity context from
    investigation_queue — both pre-aggregated at Gold layer so no Silver scan.
    """
    from config import DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN, DATABRICKS_SCHEMA
    from databricks import sql as dbsql

    conn = dbsql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    )
    try:
        cursor = conn.cursor()

        # ── Stratified sample: ALL laundering + 10% normal ─────────────────
        # aml_risk_indicators is now the FULL Silver table (no risk_flag filter).
        # Isolation Forest needs both classes to learn a baseline — without normal
        # rows it can't distinguish anomalies from just "everything looks weird".
        cursor.execute(f"""
            SELECT account_id, from_bank, to_bank, payment_format AS Payment_Format, amount,
                   receiving_currency, payment_currency,
                   is_cross_currency, is_high_value_wire, is_fraud
            FROM {DATABRICKS_SCHEMA}.aml_risk_indicators
            WHERE is_fraud = 1

            UNION ALL

            SELECT account_id, from_bank, to_bank, payment_format AS Payment_Format, amount,
                   receiving_currency, payment_currency,
                   is_cross_currency, is_high_value_wire, is_fraud
            FROM {DATABRICKS_SCHEMA}.aml_risk_indicators TABLESAMPLE (10 PERCENT)
            WHERE is_fraud = 0
            LIMIT 50000
        """)
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
        df = pd.DataFrame(rows, columns=cols)

        # ── Bank-pair laundering rate from cross_bank_flow Gold table ───────
        try:
            cursor.execute(f"""
                SELECT from_bank, to_bank,
                       ROUND(laundering_rate_pct, 4) AS bank_pair_launder_rate,
                       laundering_count AS bank_pair_launder_count
                FROM {DATABRICKS_SCHEMA}.cross_bank_flow
            """)
            bp_rows = cursor.fetchall()
            bp_cols = [d[0] for d in cursor.description]
            bank_pair_df = pd.DataFrame(bp_rows, columns=bp_cols)
            for c in ["bank_pair_launder_rate", "bank_pair_launder_count"]:
                bank_pair_df[c] = pd.to_numeric(bank_pair_df[c], errors="coerce").fillna(0.0)
            df = df.merge(bank_pair_df, on=["from_bank", "to_bank"], how="left")
            df["bank_pair_launder_rate"] = df["bank_pair_launder_rate"].fillna(0.0)
            df["bank_pair_launder_count"] = df["bank_pair_launder_count"].fillna(0.0)
        except Exception:
            df["bank_pair_launder_rate"] = 0.0
            df["bank_pair_launder_count"] = 0.0

        # ── Investigation queue priority score overlay ───────────────────────
        try:
            cursor.execute(f"""
                SELECT account_id, priority_score, indicator_count,
                       cross_currency_count, high_value_wire_count,
                       entity_name, bank_name
                FROM {DATABRICKS_SCHEMA}.investigation_queue
            """)
            q_rows = cursor.fetchall()
            q_cols = [d[0] for d in cursor.description]
            queue_df = pd.DataFrame(q_rows, columns=q_cols)
            for c in ["priority_score", "indicator_count", "cross_currency_count", "high_value_wire_count"]:
                queue_df[c] = pd.to_numeric(queue_df[c], errors="coerce").fillna(0.0)
            df = df.merge(
                queue_df[["account_id", "priority_score", "indicator_count",
                           "cross_currency_count", "high_value_wire_count",
                           "entity_name", "bank_name"]],
                on="account_id", how="left"
            )
            df["priority_score"] = df["priority_score"].fillna(0.0)
            df["indicator_count"] = df["indicator_count"].fillna(0.0)
            df["entity_name"] = df["entity_name"].fillna("Unknown")
            df["bank_name"] = df["bank_name"].fillna("Unknown")
        except Exception:
            df["priority_score"] = 0.0
            df["indicator_count"] = 0.0
            df["entity_name"] = "Unknown"
            df["bank_name"] = "Unknown"

        # Type coercions
        for col in ["is_fraud", "is_cross_currency", "is_high_value_wire"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)

        return df
    finally:
        cursor.close()
        conn.close()


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    8-feature IBM AML domain engineering:

    1. amount           — raw transaction size (high = integration risk)
    2. log_amount       — log1p(amount) compresses right-skewed distribution so
                          Isolation Forest doesn't fixate purely on outlier amounts
    3. is_cross_currency — paying_currency != receiving_currency = layering signal
    4. is_high_value_wire — Wire Transfer > $100K = integration-phase signal
    5. is_wire          — binary: Wire Transfer (highest laundering rate format)
    6. is_ach           — binary: ACH (structuring/smurfing risk at high volumes)
    7. amount_x_cross_ccy — interaction: large cross-currency transfers are
                          disproportionately risky (layering + size combined)
    8. bank_pair_launder_rate — from cross_bank_flow Gold: known-hot bank corridors
                          inject network-level intelligence into per-row scoring
    """
    df = df.copy()
    # IBM AML dataset uses "Wire" not "Wire Transfer"
    df["is_wire"]    = (df["Payment_Format"].str.lower().str.strip() == "wire").astype(int)
    df["is_ach"]     = (df["Payment_Format"].str.lower().str.strip() == "ach").astype(int)
    df["log_amount"] = np.log1p(df["amount"])

    # Interaction feature: cross-currency AND large amount = strongest layering signal
    df["amount_x_cross_ccy"] = df["amount"] * df["is_cross_currency"]

    # Interaction feature: high-value wire with known-hot bank corridor
    df["amount_x_wire"] = df["amount"] * df["is_wire"]

    return df


def run_isolation_forest(df: pd.DataFrame, contamination: float) -> pd.DataFrame:
    """
    Isolation Forest on 8-feature scaled matrix.
    StandardScaler is non-negotiable: amount ($0–$92M) would completely
    dominate binary flags (0 or 1) without normalization.
    """
    feature_cols = [
        "amount", "log_amount",
        "is_cross_currency", "is_high_value_wire",
        "is_wire", "is_ach",
        "amount_x_cross_ccy", "amount_x_wire",
    ]
    X = df[feature_cols].values
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    iso_forest = IsolationForest(
        contamination=contamination,
        random_state=42,    # fixed seed → audit-reproducible scores
        n_estimators=100,   # 100 trees → stable score estimates
        max_samples="auto", # subsample for efficiency on 58K rows
    )
    predictions = iso_forest.fit_predict(X_scaled)
    raw_scores  = iso_forest.score_samples(X_scaled) * -1  # flip: higher = more anomalous

    s_min, s_max = raw_scores.min(), raw_scores.max()
    if s_max > s_min:
        scores_norm = (raw_scores - s_min) / (s_max - s_min) * 100
    else:
        scores_norm = np.zeros_like(raw_scores)

    df["anomaly_score"] = scores_norm
    df["ml_flag"]       = (predictions == -1).astype(int)
    return df


def apply_rules(df: pd.DataFrame) -> pd.DataFrame:
    """
    3-rule domain engine targeting IBM AML typologies:
      Rule 1: Integration — high-value wire transfer (>$100K + Wire format)
      Rule 2: Layering   — cross-currency + above-dataset-average amount
      Rule 3: Hot corridor — bank pair with >0.5% historical laundering rate

    Tier logic (defense-in-depth):
      HIGH:   Isolation Forest AND (any rule) → two independent systems agree
      MEDIUM: Either Isolation Forest OR any rule → investigate further
      LOW:    Neither system raised a flag
    """
    df = df.copy()
    avg_amt = df["amount"].mean()

    # IBM AML domain rules
    rule1 = (df["amount"] > 100_000) & (df["is_wire"] == 1)
    rule2 = (df["is_cross_currency"] == 1) & (df["amount"] > avg_amt)
    rule3 = df["bank_pair_launder_rate"] > 0.5  # hot bank corridor

    df["rule_flag"]  = (rule1 | rule2 | rule3).astype(int)
    df["rule1_flag"] = rule1.astype(int)
    df["rule2_flag"] = rule2.astype(int)
    df["rule3_flag"] = rule3.astype(int)

    both   = (df["ml_flag"] == 1) & (df["rule_flag"] == 1)
    either = (df["ml_flag"] == 1) | (df["rule_flag"] == 1)
    df["confidence_tier"] = np.where(both, "HIGH", np.where(either, "MEDIUM", "LOW"))
    return df


def assign_percentile_bands(df: pd.DataFrame) -> pd.DataFrame:
    """5-bucket percentile banding for analyst-friendly score interpretation."""
    if df.empty:
        df["score_band"] = "—"
        return df
    scores = df["anomaly_score"]
    p20, p40, p60, p80 = np.percentile(scores, [20, 40, 60, 80])
    def band(s):
        if s >= p80:   return "Top 20% — Critical"
        if s >= p60:   return "Top 40% — High"
        if s >= p40:   return "Top 60% — Elevated"
        if s >= p20:   return "Top 80% — Moderate"
        return "Bottom 20% — Low"
    df["score_band"] = scores.apply(band)
    return df


def build_scatter(df: pd.DataFrame) -> go.Figure:
    """
    X = log(amount): log-scale compresses the $0–$92M range so mid-range
    transactions don't all cluster at x≈0. More informative than raw amount.
    Y = anomaly_score (0–100): higher = more unusual.
    Top-right quadrant = highest risk: large amounts + high anomaly scores.
    """
    low    = df[df["confidence_tier"] == "LOW"]
    medium = df[df["confidence_tier"] == "MEDIUM"]
    high   = df[df["confidence_tier"] == "HIGH"]

    _hover = (
        "<b>Account: %{customdata[0]}</b><br>"
        "Payment Method: %{customdata[1]}<br>"
        "Transaction Value: $%{customdata[4]:,.0f}<br>"
        "Risk Score: %{y:.1f}/100 · %{customdata[5]}<br>"
        "Route: %{customdata[2]} to %{customdata[3]}<br>"
        "Entity: %{customdata[6]}"
        "<extra></extra>"
    )

    def _custom(subset):
        return subset[["account_id", "Payment_Format", "from_bank", "to_bank",
                        "amount", "score_band", "entity_name"]].values

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=np.log1p(low["amount"]), y=low["anomaly_score"],
        mode="markers", name="Normal",
        marker=dict(color="rgba(74,96,128,0.35)", size=4),
        customdata=_custom(low), hovertemplate=_hover
    ))
    fig.add_trace(go.Scatter(
        x=np.log1p(medium["amount"]), y=medium["anomaly_score"],
        mode="markers", name="Medium Risk",
        marker=dict(color="rgba(251,191,36,0.75)", size=8),
        customdata=_custom(medium), hovertemplate=_hover
    ))
    fig.add_trace(go.Scatter(
        x=np.log1p(high["amount"]), y=high["anomaly_score"],
        mode="markers", name="High Risk",
        marker=dict(color="rgba(248,113,113,0.9)", size=12),
        customdata=_custom(high), hovertemplate=_hover
    ))

    # Annotation: top-right risk zone
    fig.add_annotation(
        x=0.97, y=0.97, xref="paper", yref="paper",
        text="HIGHEST CONCERN", showarrow=False,
        font=dict(color="rgba(248,113,113,0.45)", size=10),
        align="right"
    )

    fig.update_layout(
        paper_bgcolor="#1A2E4A", plot_bgcolor="#172340",
        font=dict(color="#7E9BB8"),
        xaxis=dict(title="Transaction Size (compressed scale)", gridcolor="rgba(96,165,250,0.07)"),
        yaxis=dict(title="Suspicion Score (0 = Normal, 100 = Highly Suspicious)", gridcolor="rgba(96,165,250,0.07)"),
        margin=dict(l=60, r=20, t=28, b=60), height=480,
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color="#7E9BB8")),
    )
    return fig


@st.cache_data(ttl=600, show_spinner=False)
def load_corridor_stats() -> pd.DataFrame:
    """Top corridors joined to bank names from accounts_silver."""
    from config import DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN, DATABRICKS_SCHEMA
    from databricks import sql as dbsql
    conn = dbsql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    )
    try:
        cursor = conn.cursor()
        # Join to accounts_silver twice to resolve bank IDs → readable names
        # cross_bank_flow now includes from_bank_name / to_bank_name resolved via accounts_silver
        cursor.execute(f"""
            SELECT
                from_bank, to_bank,
                from_bank_name, to_bank_name,
                transaction_count, laundering_count,
                ROUND(laundering_rate_pct, 4) AS laundering_rate_pct,
                total_volume_usd, cross_currency_count
            FROM {DATABRICKS_SCHEMA}.cross_bank_flow
            ORDER BY laundering_rate_pct DESC, laundering_count DESC
        """)
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
        return pd.DataFrame(rows, columns=cols)
    finally:
        cursor.close()
        conn.close()


def build_bank_pair_chart(_df: pd.DataFrame) -> go.Figure:
    try:
        cbf = load_corridor_stats()
    except Exception:
        return go.Figure()

    if cbf.empty:
        return go.Figure()

    for c in ["laundering_rate_pct", "laundering_count", "transaction_count", "total_volume_usd"]:
        cbf[c] = pd.to_numeric(cbf[c], errors="coerce").fillna(0.0)

    top = cbf.head(10).copy()
    top["corridor"] = top["from_bank_name"] + " to " + top["to_bank_name"]
    top["rate_label"] = top["laundering_rate_pct"].map(lambda x: f"{x:.1f}%")

    max_rate = top["laundering_rate_pct"].max() or 1
    opacities = (top["laundering_rate_pct"] / max_rate * 0.55 + 0.35).clip(0.3, 0.9)
    bar_colors = [f"rgba(248,113,113,{o:.2f})" for o in opacities]

    fig = go.Figure(go.Bar(
        x=top["laundering_rate_pct"],
        y=top["corridor"],
        orientation="h",
        marker=dict(color=bar_colors, line=dict(color="rgba(255,255,255,0.06)", width=1)),
        customdata=top[["laundering_count", "transaction_count", "total_volume_usd", "cross_currency_count"]].values,
        hovertemplate=(
            "<b>%{y}</b><br>"
            "Suspicious Rate: <b>%{x:.2f}%</b> of all transfers<br>"
            "Confirmed Suspicious: %{customdata[0]:,} transactions<br>"
            "Total Transfers: %{customdata[1]:,}<br>"
            "Total Value: $%{customdata[2]:,.0f}<br>"
            "Cross-Border: %{customdata[3]:,}"
            "<extra></extra>"
        ),
        text=top["rate_label"],
        textposition="outside",
        textfont=dict(color="#F87171", size=10),
    ))
    fig.update_layout(
        title=dict(
            text="Highest-Risk Transfer Routes — % of Transfers Confirmed Suspicious",
            font=dict(color="#7E9BB8", size=12)
        ),
        paper_bgcolor="#1A2E4A", plot_bgcolor="#172340",
        font=dict(color="#7E9BB8"),
        xaxis=dict(title="Suspicious Transfer Rate (%)", gridcolor="rgba(96,165,250,0.07)"),
        yaxis=dict(autorange="reversed"),
        margin=dict(l=20, r=80, t=44, b=40), height=360,
    )
    return fig


def _kpi(label: str, value: str, delta: str = "", accent: str = "") -> str:
    """Local KPI card — mirrors finpulse_app.py design system."""
    accent_cls = f" accent-{accent}" if accent else ""
    delta_html = f'<div class="kpi-delta">{delta}</div>' if delta else ""
    return f"""
    <div class="kpi-card{accent_cls}">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        {delta_html}
    </div>"""


def _insight_box(text: str, level: str = "info") -> str:
    """Inline insight callout — level: info | warn | critical."""
    colors = {
        "info":     ("rgba(96,165,250,0.08)",  "rgba(96,165,250,0.3)",  "#60A5FA"),
        "warn":     ("rgba(251,191,36,0.08)",   "rgba(251,191,36,0.3)",  "#FBBF24"),
        "critical": ("rgba(248,113,113,0.08)",  "rgba(248,113,113,0.3)", "#F87171"),
    }
    bg, border, color = colors.get(level, colors["info"])
    return f"""
    <div style="background:{bg};border-left:3px solid {color};border-radius:0 3px 3px 0;
                padding:12px 18px;margin:14px 0 4px;font-size:13px;line-height:1.7;color:{color};">
        {text}
    </div>"""


def run_hunt():
    col_btn, col_slider, col_filter = st.columns([1, 2, 1])

    with col_btn:
        st.markdown('<div style="padding-top:20px"></div>', unsafe_allow_html=True)
        run_button = st.button("Run Detection", use_container_width=True)

    with col_slider:
        contamination = st.slider(
            "Contamination (AML base rate)", 0.005, 0.10, 0.013, 0.001,
            help="0.013 = domain-calibrated to the IBM AML dataset's confirmed laundering base rate. "
                 "Increasing this flags more accounts as anomalous; decreasing it tightens the filter."
        )

    with col_filter:
        type_filter = st.selectbox(
            "Filter by Payment Format",
            options=["ALL", "Wire", "ACH", "Cheque", "Credit Card", "Cash", "Bitcoin", "Reinvestment"],
            help="Restrict Isolation Forest scoring to a single payment channel. "
                 "Use this to investigate format-specific laundering patterns in isolation."
        )

    if run_button:
        st.session_state.hunt_page = 1
        st.session_state.hunt_search_query = ""
        st.session_state.hunt_alert_filter = "ALL"

    if run_button or st.session_state.get("agent1_results") is not None:
        with st.spinner("Running analysis..."):
            try:
                if run_button:
                    df = _run_full_pipeline(contamination, type_filter)
                    st.session_state.agent1_results = df[df["confidence_tier"].isin(["HIGH", "MEDIUM"])].to_dict("records")
                else:
                    df = pd.DataFrame(st.session_state.agent1_results)
            except Exception as e:
                st.error(f"Detection pipeline error: {e}")
                return

            if df.empty:
                st.warning("No suspicious accounts detected with current parameters.")
                return

        # ── Filter Bar (Single Line Layout) ──────────────────────────────────
        st.markdown('<div class="section-label" style="margin-top:28px;">Audit Review Queue</div>', unsafe_allow_html=True)
        
        f1, f2, f3, f4 = st.columns([1.5, 1, 1, 1])
        with f1:
            raw_search = st.text_input("Search Account ID or Entity", value=st.session_state.get("hunt_search_query", ""), placeholder="ID or Name...")
            if raw_search != st.session_state.get("hunt_search_query"):
                st.session_state.hunt_search_query = raw_search
                st.session_state.hunt_page = 1
                st.rerun()
        
        with f2:
            unique_banks = sorted(df["bank_name"].unique().tolist())
            sel_banks = st.multiselect("Bank Location", options=unique_banks, default=st.session_state.get("hunt_bank_filter", []))
            if sel_banks != st.session_state.get("hunt_bank_filter"):
                st.session_state.hunt_bank_filter = sel_banks
                st.session_state.hunt_page = 1
                st.rerun()

        with f3:
            alert_options = ["ALL", "Critical", "Investigate"]
            current_alert = st.session_state.get("hunt_alert_filter", "ALL")
            default_alert = alert_options.index(current_alert.title()) if current_alert.title() in alert_options else 0
            sel_alert = st.selectbox("Alert Level", options=alert_options, index=default_alert)
            if sel_alert.upper() != current_alert:
                st.session_state.hunt_alert_filter = sel_alert.upper()
                st.session_state.hunt_page = 1
                st.rerun()

        with f4:
            ccys = sorted(df["receiving_currency"].unique().tolist()) if "receiving_currency" in df.columns else []
            sel_ccys = st.multiselect("Currency Route", options=ccys, default=st.session_state.get("hunt_ccy_filter", []))
            if sel_ccys != st.session_state.get("hunt_ccy_filter"):
                st.session_state.hunt_ccy_filter = sel_ccys
                st.session_state.hunt_page = 1
                st.rerun()

        # ── Filtering Logic ──────────────────────────────────────────────────
        filtered_df = df.copy()
        if st.session_state.get("hunt_search_query"):
            q = st.session_state.hunt_search_query.lower()
            filtered_df = filtered_df[
                filtered_df["account_id"].str.lower().str.contains(q) | 
                filtered_df["entity_name"].str.lower().str.contains(q)
            ]
        
        if st.session_state.get("hunt_alert_filter", "ALL") != "ALL":
            filter_map = {"CRITICAL": "HIGH", "INVESTIGATE": "MEDIUM"}
            target_tier = filter_map.get(st.session_state.hunt_alert_filter)
            filtered_df = filtered_df[filtered_df["confidence_tier"] == target_tier]

        if st.session_state.get("hunt_bank_filter"):
            filtered_df = filtered_df[filtered_df["bank_name"].isin(st.session_state.hunt_bank_filter)]

        if st.session_state.get("hunt_ccy_filter"):
            filtered_df = filtered_df[filtered_df["receiving_currency"].isin(st.session_state.hunt_ccy_filter)]

        # ── Pagination Logic ─────────────────────────────────────────────────
        if "hunt_page" not in st.session_state: st.session_state.hunt_page = 1
        PAGE_SIZE = 10
        total_records = len(filtered_df)
        total_pages = (total_records + PAGE_SIZE - 1) // PAGE_SIZE if total_records > 0 else 1
        
        if st.session_state.hunt_page > total_pages: st.session_state.hunt_page = total_pages
        if st.session_state.hunt_page < 1: st.session_state.hunt_page = 1
        
        start_idx = (st.session_state.hunt_page - 1) * PAGE_SIZE
        end_idx = start_idx + PAGE_SIZE
        page_df = filtered_df.iloc[start_idx:end_idx]

        # ── Visualizing the Review Queue (Premium HTML) ─────────────────────
        if not page_df.empty:
            rows_html = ""
            for _, row in page_df.iterrows():
                tier = row.get("confidence_tier", "LOW")
                row_cls, dot_cls, level_txt = {
                    "HIGH":   ("row-critical", "bg-red", "Critical"),
                    "MEDIUM": ("row-warn", "bg-amber", "Investigate"),
                }.get(tier, ("row-info", "bg-blue", "Normal"))
                
                band = row.get("score_band", "—").split(" — ")
                band_main, band_sub = (band[0], band[1]) if len(band) > 1 else (band[0], "")
                amt_str = f"${row.get('amount', 0):,.0f}" if row.get('amount', 0) < 1_000_000 else f"${row.get('amount', 0)/1_000_000:.2f}M"
                
                ccy_badge = f'<span class="ccy-tag">{row.get("receiving_currency","")}</span>' if row.get("is_cross_currency") == 1 else ""
                
                rows_html += f"""
                <tr class="{row_cls}">
                    <td><div style="display:flex;align-items:center;"><span class="status-indicator {dot_cls}"></span><span class="mono">{row.get('account_id','—')}</span></div></td>
                    <td><div style="font-size:12.5px; font-weight:600;">{row.get('entity_name','—')}</div></td>
                    <td>
                        <div style="font-size:11px; font-weight:700; color:#FFFFFF;">{row.get('bank_name','—')}</div>
                        <div style="font-size:10px; color:#4A6080; margin-top:2px;">Route: {row.get('from_bank','—')} to {row.get('to_bank','—')}</div>
                    </td>
                    <td><div style="font-size:11.5px;">{row.get('Payment_Format','—')} {ccy_badge}</div></td>
                    <td><span class="amount">{amt_str}</span></td>
                    <td class="mono">{row.get('anomaly_score', 0):.1f}</td>
                    <td><div class="badge-band"><span class="band-label">{band_sub}</span><span class="band-value">{band_main}</span></div></td>
                    <td><div class="status-pill"><span class="status-indicator {dot_cls}"></span>{level_txt}</div></td>
                    <td><span style="font-family:monospace; font-size:11px; color:#F87171;">{row.get('bank_pair_launder_rate', 0):.2f}%</span></td>
                </tr>"""

            st.markdown(f"""
            <div class="fp-card" style="padding:0;overflow:auto;">
                <table class="data-table">
                    <thead><tr>
                        <th>Account ID</th><th>Entity</th><th>Bank & Route</th>
                        <th>Method</th><th>Value</th><th>Score</th>
                        <th>Band</th><th>Status</th><th>Route Risk</th>
                    </tr></thead>
                    <tbody>{rows_html}</tbody>
                </table>
                <div class="pagination-bar">
                    <div class="pagination-info">
                        Showing <strong>{start_idx+1}-{min(end_idx, total_records)}</strong> of <strong>{total_records}</strong> transactions detected
                    </div>
                </div>
            </div>""", unsafe_allow_html=True)

            # Pagination HUD
            p_c1, p_c2, p_c3, p_c4, p_c5 = st.columns([7, 1, 1, 1, 1])
            with p_c4:
                if st.button("Previous", disabled=(st.session_state.hunt_page <= 1), use_container_width=True):
                    st.session_state.hunt_page -= 1
                    st.rerun()
            with p_c5:
                if st.button("Next", disabled=(st.session_state.hunt_page >= total_pages), use_container_width=True):
                    st.session_state.hunt_page += 1
                    st.rerun()

            st.download_button(
                label="Export Audit Selection (CSV)",
                data=filtered_df.to_csv(index=False).encode('utf-8'),
                file_name=f"finpulse_audit_{int(datetime.now().timestamp())}.csv",
                mime="text/csv",
                use_container_width=True
            )
        else:
            st.info("No records match your criteria. Adjust filters to broaden search.")


        # ── Rule engine breakdown ────────────────────────────────────────────
        if not filtered_df.empty:
            r1 = int(filtered_df["rule1_flag"].sum()) if "rule1_flag" in filtered_df.columns else 0
            r2 = int(filtered_df["rule2_flag"].sum()) if "rule2_flag" in filtered_df.columns else 0
            r3 = int(filtered_df["rule3_flag"].sum()) if "rule3_flag" in filtered_df.columns else 0
            ml_only   = int(((filtered_df["ml_flag"] == 1) & (filtered_df["rule_flag"] == 0)).sum())
            rule_only = int(((filtered_df["ml_flag"] == 0) & (filtered_df["rule_flag"] == 1)).sum())


            st.markdown('<div class="section-label" style="margin-top:28px;">Why Were These Accounts Flagged?</div>',
                        unsafe_allow_html=True)
            st.markdown(_insight_box(
                "Every flagged account triggered one or more of the checks below. "
                "Accounts that triggered <strong>multiple checks</strong> are considered highest priority — "
                "the more independent checks agree, the stronger the case for further action."
            ), unsafe_allow_html=True)

            st.markdown(f"""
            <div class="fp-card" style="margin-top:16px;padding:26px 28px;">
                <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:24px;">
                    <div>
                        <div class="kpi-label">Large Wire Transfers</div>
                        <div class="kpi-value" style="font-size:22px;color:#F87171;">{r1:,}</div>
                        <div class="kpi-delta" style="margin-top:8px;">High-value transfers over $100,000 — a common method to move laundered funds back into the system</div>
                    </div>
                    <div>
                        <div class="kpi-label">Currency Switching</div>
                        <div class="kpi-value" style="font-size:22px;color:#FBBF24;">{r2:,}</div>
                        <div class="kpi-delta" style="margin-top:8px;">Payments sent and received in different currencies — used to disguise the origin of funds across borders</div>
                    </div>
                    <div>
                        <div class="kpi-label">High-Risk Routes</div>
                        <div class="kpi-value" style="font-size:22px;color:#F87171;">{r3:,}</div>
                        <div class="kpi-delta" style="margin-top:8px;">Transfers on bank-to-bank routes with a history of confirmed suspicious activity</div>
                    </div>
                    <div>
                        <div class="kpi-label">Pattern-Based Only</div>
                        <div class="kpi-value" style="font-size:22px;color:#60A5FA;">{ml_only:,}</div>
                        <div class="kpi-delta" style="margin-top:8px;">Unusual transaction patterns detected by behavioural analysis — no specific rule triggered</div>
                    </div>
                    <div>
                        <div class="kpi-label">Policy-Based Only</div>
                        <div class="kpi-value" style="font-size:22px;color:#60A5FA;">{rule_only:,}</div>
                        <div class="kpi-delta" style="margin-top:8px;">Triggered a compliance policy check — pattern analysis did not independently flag these</div>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
