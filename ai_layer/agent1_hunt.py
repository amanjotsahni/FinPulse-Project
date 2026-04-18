"""
FinPulse — Agent 1: HUNT
=========================
File: ai_layer/agent1_hunt.py
Called from: finpulse_app.py when screen == "HUNT"

What this agent does:
  1. Fetches a stratified sample (~50k rows) from balance_discrepancy_summary
  2. Engineers 5 row-level features for Isolation Forest
  3. Runs Isolation Forest (contamination=0.013 — matches actual 0.13% fraud rate)
  4. Applies domain-aware hard rules on top of ML scores
  5. Assigns final confidence tiers: HIGH / MEDIUM / LOW
  6. Renders interactive Plotly scatter plot
  7. Shows flagged accounts table
  8. Saves results to st.session_state.agent1_results for Agent 2

Data source: finpulse.balance_discrepancy_summary (Gold layer, stratified sample ~50k rows)
No raw transaction data is touched. No pipeline is triggered.
"""

# ── Imports ───────────────────────────────────────────────────────────────────
import streamlit as st          # UI framework
import pandas as pd             # data manipulation
import numpy as np              # numerical operations
import plotly.graph_objects as go  # interactive charts
import os
import sys
from pathlib import Path

# Allow importing config from project root
_PROJECT_ROOT = Path(__file__).parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# Isolation Forest — scikit-learn's unsupervised anomaly detection algorithm
from sklearn.ensemble import IsolationForest

# StandardScaler — normalises features so no single feature dominates
# Example: amount ranges in millions, isFraud is 0/1 — without scaling
# the model would ignore isFraud completely
from sklearn.preprocessing import StandardScaler


# ── Data loader ───────────────────────────────────────────────────────────────
# @st.cache_data means: run this function once, cache the result for ttl seconds.
# If the user clicks anything on screen, Streamlit reruns the script but
# this function does NOT re-query Databricks — it returns the cached DataFrame.
# ttl=600 = 10 minutes. After 10 min, the next call fetches fresh data.
# This is critical — without caching, every slider move would hit Databricks.

@st.cache_data(ttl=600, show_spinner=False)
def load_discrepancy_data() -> pd.DataFrame:
    """
    Loads a stratified sample from balance_discrepancy_summary.

    Why stratified sampling:
    - Full table has ~1.2M rows — too large for in-memory ML on 8GB RAM.
    - We need fraud cases represented proportionally (0.13% rate).
    - Stratified sample preserves the fraud/normal ratio.

    Sample strategy:
    - All fraud transactions (is_fraud = 1) — always include, there aren't many.
    - Random 50,000 of non-fraud transactions.
    - Together: representative of the full distribution.

    This is the production pattern — train on representative sample,
    score on representative sample. Full table stays in Databricks for audit.
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

        # Fetch ALL fraud transactions first — there are very few of them.
        # We never want to lose fraud signals in sampling.
        cursor.execute(f"""
            SELECT account_id, type, amount, oldbalanceOrg, newbalanceOrig,
                   oldbalanceDest, newbalanceDest, balance_gap, is_fraud
            FROM {DATABRICKS_SCHEMA}.balance_discrepancy_summary
            WHERE is_fraud = 1
        """)
        fraud_rows = cursor.fetchall()
        fraud_cols = [d[0] for d in cursor.description]
        df_fraud = pd.DataFrame(fraud_rows, columns=fraud_cols)

        # Fetch a random 50,000 sample of non-fraud transactions.
        # ORDER BY RAND() + LIMIT is Databricks-native — does not require full scan.
        cursor.execute(f"""
            SELECT account_id, type, amount, oldbalanceOrg, newbalanceOrig,
                   oldbalanceDest, newbalanceDest, balance_gap, is_fraud
            FROM {DATABRICKS_SCHEMA}.balance_discrepancy_summary
            WHERE is_fraud = 0
            ORDER BY RAND()
            LIMIT 50000
        """)
        normal_rows = cursor.fetchall()
        normal_cols = [d[0] for d in cursor.description]
        df_normal = pd.DataFrame(normal_rows, columns=normal_cols)

        df = pd.concat([df_fraud, df_normal], ignore_index=True)

        # Databricks returns Decimal types — convert to Python float/int
        int_cols  = ["is_fraud"]
        float_cols = ["amount", "oldbalanceOrg", "newbalanceOrig",
                      "oldbalanceDest", "newbalanceDest", "balance_gap"]
        for col in int_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        for col in float_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        return df

    finally:
        cursor.close()
        conn.close()


# ── Feature engineering ───────────────────────────────────────────────────────
def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Derives ML features from row-level transaction data.

    Features:
      - amount:        transaction size — large amounts are higher risk
      - balance_gap:   oldbalanceOrg - amount - newbalanceOrig — the core
                       discrepancy signal; money that cannot be accounted for
      - gap_ratio:     balance_gap / amount — normalises the gap relative to
                       transaction size; 1.0 means 100% of the amount vanished
      - dest_drain:    newbalanceDest - oldbalanceDest — how much the destination
                       account grew; in fraud, this is often 0 (money laundered out)
      - is_transfer:   1 if type=TRANSFER, 0 if CASH_OUT — TRANSFER has higher fraud rate
    """
    df = df.copy()

    # gap_ratio: how large the discrepancy is relative to the transaction amount
    # clip amount at 1 to avoid division by zero on zero-amount rows
    df["gap_ratio"] = df["balance_gap"] / df["amount"].clip(lower=1)

    # dest_drain: destination account balance change
    # in confirmed fraud the destination often stays at 0 (funds immediately moved out)
    df["dest_drain"] = df["newbalanceDest"] - df["oldbalanceDest"]

    # is_transfer: binary flag — TRANSFER type has measurably higher fraud rate
    df["is_transfer"] = (df["type"] == "TRANSFER").astype(int)

    return df


# ── Isolation Forest ──────────────────────────────────────────────────────────
def run_isolation_forest(df: pd.DataFrame, contamination: float) -> pd.DataFrame:
    """
    Runs Isolation Forest on the 4 engineered features.

    Isolation Forest works by randomly partitioning the feature space
    into isolation trees. Anomalies are data points that get isolated
    in fewer splits — they are "easier to isolate" because they sit far
    from the cluster of normal transactions.

    contamination: the proportion of data points the model expects to be
    anomalous. We use 0.013 because our dataset has 0.13% fraud.
    Using the default 0.1 would flag 100x too many transactions.

    Adds two columns to the DataFrame:
      anomaly_score:  raw score from -1 (most anomalous) to +1 (most normal)
                      we flip the sign so higher = more suspicious
      ml_flag:        1 if Isolation Forest classifies as anomaly, 0 if normal
    """
    # 5 row-level features
    feature_cols = ["amount", "balance_gap", "gap_ratio", "dest_drain", "is_transfer"]

    # Extract the feature matrix — shape: (1000, 4)
    X = df[feature_cols].values

    # StandardScaler: transforms each feature to mean=0, std=1
    # Without this, "amount" (values in hundreds of thousands) would completely
    # dominate "is_transfer" (values 0 or 1) and the model would effectively
    # only look at amount
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Initialise Isolation Forest
    # contamination=0.013 — tells the model 1.3% of data is expected to be anomalous
    # random_state=42     — ensures the same result every run (reproducibility)
    # n_estimators=100    — number of isolation trees (default, good balance of speed/accuracy)
    iso_forest = IsolationForest(
        contamination=contamination,
        random_state=42,
        n_estimators=100,
    )

    # fit_predict: trains the model AND predicts in one call
    # Returns: -1 for anomaly, +1 for normal
    predictions = iso_forest.fit_predict(X_scaled)

    # score_samples: returns the raw anomaly score
    # More negative = more anomalous
    # We multiply by -1 so higher score = more suspicious (more intuitive)
    scores = iso_forest.score_samples(X_scaled) * -1

    # Normalise scores to 0-100 range for display
    # min-max normalisation: (x - min) / (max - min) * 100
    score_min, score_max = scores.min(), scores.max()
    if score_max > score_min:
        scores_normalised = (scores - score_min) / (score_max - score_min) * 100
    else:
        scores_normalised = np.zeros_like(scores)

    # Add results back to DataFrame
    df = df.copy()
    df["anomaly_score"] = scores_normalised   # 0-100, higher = more suspicious
    df["ml_flag"] = (predictions == -1).astype(int)  # 1=anomaly, 0=normal

    return df


# ── Hard rules engine ─────────────────────────────────────────────────────────
def apply_rules(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies domain-aware hard rules on top of ML scores.

    Why hybrid ML + rules:
      Pure ML fails on extreme class imbalance (0.13% fraud).
      The model may miss obvious cases because they're statistically rare.
      Hard rules capture domain knowledge that the data alone can't express:
      - We KNOW fraud only exists in TRANSFER and CASH_OUT
      - We KNOW large balance discrepancies are the strongest indicator
      - We KNOW isFraud ground truth from the dataset

    Rule logic:
      If ALL three conditions are met → rule_flag = 1 (definite concern)
        1. Type is TRANSFER or CASH_OUT (fraud only here)
        2. balance_diff is very negative (money disappeared)
        3. amount is above the 75th percentile (large transaction)

    Final confidence tier:
      HIGH   — both ML flag AND rule flag = 1 (both systems agree)
      MEDIUM — either ML flag OR rule flag = 1 (one system flagged it)
      LOW    — neither flag = 1 (normal transaction)
    """
    df = df.copy()

    # Rule thresholds derived from distribution of row-level transaction features
    amount_75th = df["amount"].quantile(0.75)
    gap_75th    = df["balance_gap"].quantile(0.75)    # large positive gap = suspicious
    ratio_75th  = df["gap_ratio"].quantile(0.75)      # high ratio = most of amount vanished

    # A transaction is rule-flagged if it meets ANY TWO of three conditions:
    #   1. Amount is in the top 25%  — large transaction
    #   2. Balance gap is in the top 25% — large unexplained discrepancy
    #   3. Gap ratio is in the top 25% — most of the amount cannot be accounted for
    cond1 = (df["amount"]      >= amount_75th).astype(int)
    cond2 = (df["balance_gap"] >= gap_75th).astype(int)
    cond3 = (df["gap_ratio"]   >= ratio_75th).astype(int)

    df["rule_flag"] = ((cond1 + cond2 + cond3) >= 2).astype(int)

    # Assign confidence tier based on combination of ML and rule flags
    def assign_tier(row):
        if row["ml_flag"] == 1 and row["rule_flag"] == 1:
            return "HIGH"      # both systems flagged it — strongest signal
        elif row["ml_flag"] == 1 or row["rule_flag"] == 1:
            return "MEDIUM"    # one system flagged it — worth investigating
        else:
            return "LOW"       # neither — normal transaction

    # Apply the function row by row
    # axis=1 means apply across columns (row-wise), not down columns (column-wise)
    df["confidence_tier"] = df.apply(assign_tier, axis=1)

    return df


# ── Plotly scatter plot ───────────────────────────────────────────────────────
def build_scatter(df: pd.DataFrame) -> go.Figure:
    """
    Builds an interactive Plotly scatter plot.

    X axis: amount (transaction size)
    Y axis: balance_gap (unexplained discrepancy — how positive = how suspicious)
    Color:  confidence tier (LOW=grey, MEDIUM=amber, HIGH=red)
    Size:   anomaly_score (larger dot = higher ML suspicion score)
    Hover:  shows account_id, type, amount, balance_diff, tier, score

    Why this layout:
      Fraud pattern in this dataset concentrates in the bottom-right quadrant:
      - High amount (right) + Large negative balance_diff (bottom)
      - This visual makes the fraud cluster immediately visible
    """
    # Separate into three groups for individual trace control
    # Each group gets its own color, opacity, and marker size
    low    = df[df["confidence_tier"] == "LOW"]
    medium = df[df["confidence_tier"] == "MEDIUM"]
    high   = df[df["confidence_tier"] == "HIGH"]

    # X = transaction amount, Y = balance_gap (unexplained discrepancy)
    # Fraud cluster sits top-right: high amount + large positive balance gap
    _hover = (
        "<b>%{customdata[0]}</b><br>"
        "Type: %{customdata[1]}<br>"
        "Amount: $%{x:,.0f}<br>"
        "Balance Gap: $%{y:,.0f}<br>"
        "Score: %{customdata[2]:.1f}<br>"
        "Tier: %{customdata[3]}"
        "<extra></extra>"
    )

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=low["amount"], y=low["balance_gap"],
        mode="markers", name="Normal",
        marker=dict(color="rgba(74,96,128,0.4)", size=5, line=dict(width=0)),
        customdata=low[["account_id", "type", "anomaly_score", "confidence_tier"]].values,
        hovertemplate=_hover,
    ))

    fig.add_trace(go.Scatter(
        x=medium["amount"], y=medium["balance_gap"],
        mode="markers", name="Medium Risk",
        marker=dict(
            color="rgba(251,191,36,0.75)",
            size=medium["anomaly_score"].clip(lower=6, upper=16),
            line=dict(width=0.5, color="rgba(251,191,36,0.3)"),
        ),
        customdata=medium[["account_id", "type", "anomaly_score", "confidence_tier"]].values,
        hovertemplate=_hover,
    ))

    fig.add_trace(go.Scatter(
        x=high["amount"], y=high["balance_gap"],
        mode="markers", name="High Risk",
        marker=dict(
            color="rgba(248,113,113,0.9)",
            size=high["anomaly_score"].clip(lower=10, upper=20),
            line=dict(width=1, color="rgba(248,113,113,0.5)"),
        ),
        customdata=high[["account_id", "type", "anomaly_score", "confidence_tier"]].values,
        hovertemplate=_hover,
    ))

    # ── High Risk Zone annotation ─────────────────────────────────────────────
    # Draw a dashed rectangle in the top-right quadrant where fraud clusters.
    # Thresholds: top 25% of both axes — same logic as apply_rules().
    if len(df) > 4:
        _x_thresh = float(df["amount"].quantile(0.75))
        _y_thresh = float(df["balance_gap"].quantile(0.75))
        _x_max    = float(df["amount"].max()) * 1.05
        _y_max    = float(df["balance_gap"].max()) * 1.05

        fig.add_shape(
            type="rect",
            x0=_x_thresh, x1=_x_max,
            y0=_y_thresh, y1=_y_max,
            line=dict(color="rgba(248,113,113,0.35)", width=1, dash="dot"),
            fillcolor="rgba(248,113,113,0.04)",
            layer="below",
        )
        fig.add_annotation(
            x=_x_max, y=_y_max,
            text="HIGH RISK ZONE",
            showarrow=False,
            xanchor="right", yanchor="top",
            font=dict(size=9, color="rgba(248,113,113,0.55)",
                      family="JetBrains Mono, monospace"),
            bgcolor="rgba(9,14,24,0.0)",
        )

    # ── Layout — match app dark theme exactly ──
    # Palette mirror: body=#1A2E4A  card=#172340  T2=#7E9BB8  accent=#60A5FA
    fig.update_layout(
        paper_bgcolor="#1A2E4A",   # matches .stApp body background
        plot_bgcolor="#172340",    # matches .kpi-card / .fp-card background
        font=dict(
            family="Inter, -apple-system, BlinkMacSystemFont, sans-serif",
            color="#7E9BB8",       # T2 secondary — axis labels, legend text
            size=12,
        ),
        xaxis=dict(
            title=dict(text="Transaction Amount (USD)", font=dict(size=11, color="#4A6080")),
            gridcolor="rgba(96,165,250,0.07)",
            zeroline=False,
            tickformat="$,.0f",
            tickfont=dict(color="#7E9BB8", size=11),
            linecolor="rgba(96,165,250,0.12)",
        ),
        yaxis=dict(
            title=dict(text="Balance Gap per Transaction (USD)", font=dict(size=11, color="#4A6080")),
            gridcolor="rgba(96,165,250,0.07)",
            zeroline=True,
            zerolinecolor="rgba(248,113,113,0.25)",
            zerolinewidth=1,
            tickformat="$,.0f",
            tickfont=dict(color="#7E9BB8", size=11),
            linecolor="rgba(96,165,250,0.12)",
        ),
        legend=dict(
            bgcolor="rgba(9,14,24,0.85)",      # sidebar ink — darkest surface
            bordercolor="rgba(96,165,250,0.15)",
            borderwidth=1,
            font=dict(size=11, color="#7E9BB8"),
        ),
        hoverlabel=dict(
            bgcolor="#090E18",                 # sidebar ink for tooltip bg
            bordercolor="rgba(96,165,250,0.3)",
            font=dict(family="JetBrains Mono, monospace", size=12, color="#FFFFFF"),
        ),
        margin=dict(l=60, r=20, t=16, b=60),
        height=480,
        hovermode="closest",
    )

    return fig


# ── Main run function — called from finpulse_app.py ───────────────────────────
def run_hunt():
    """
    Entry point called by finpulse_app.py on the HUNT screen.
    Orchestrates the full Agent 1 flow:
      load data → engineer features → run model → apply rules → render UI
    """

    # ── Controls row ──────────────────────────────────────────────────────────
    # Two columns: left for the button, right for the threshold slider
    col_btn, col_slider, col_filter = st.columns([1, 2, 1])

    with col_btn:
        run_button = st.button("Run Detection", use_container_width=True)

    with col_slider:
        # Contamination slider — lets user adjust how aggressive detection is
        # Default 0.013 matches the actual fraud rate in the dataset
        # Moving right flags more transactions, moving left flags fewer
        contamination = st.slider(
            "Contamination threshold",
            min_value=0.005,
            max_value=0.10,
            value=0.013,
            step=0.001,
            format="%.3f",
            help="Proportion of transactions expected to be anomalous. "
                 "0.013 matches the dataset's actual 0.13% fraud rate.",
        )

    with col_filter:
        # Filter by transaction type
        type_filter = st.selectbox(
            "Transaction type",
            options=["ALL", "TRANSFER", "CASH_OUT"],
            index=0,
        )

    # ── Detection logic ───────────────────────────────────────────────────────
    # Only runs when the button is clicked OR results are already in session state
    # This prevents re-running the model on every Streamlit rerun

    if run_button or st.session_state.agent1_results is not None:

        # Show a spinner while data loads and model runs
        with st.spinner("Loading Gold layer data and running detection..."):

            # Step 1: Load data (cached — won't re-query if already fetched)
            try:
                df = load_discrepancy_data()
            except Exception as e:
                st.error(f"Failed to load data from Databricks: {e}")
                return

            # Step 2: Apply type filter
            if type_filter != "ALL":
                df = df[df["type"] == type_filter].copy()

            # Guard: too few accounts to run meaningful Isolation Forest
            if len(df) < 10:
                st.warning(
                    f"Only {len(df)} accounts in the discrepancy summary. "
                    "Run `dbt run` to refresh the Gold layer first."
                )
                return

            # Step 3: Engineer features
            df = engineer_features(df)

            # Step 4: Run Isolation Forest with current contamination value
            df = run_isolation_forest(df, contamination)

            # Step 5: Apply hard rules
            df = apply_rules(df)

            # Step 6: Save flagged transactions (MEDIUM + HIGH) to session state for Agent 2
            flagged = df[df["confidence_tier"].isin(["HIGH", "MEDIUM"])].copy()
            st.session_state.agent1_results = flagged[[
                "account_id", "type", "amount", "balance_gap", "gap_ratio",
                "dest_drain", "is_fraud",
                "anomaly_score", "confidence_tier", "rule_flag", "ml_flag",
            ]].to_dict("records")

        # ── Summary KPIs ──────────────────────────────────────────────────────
        total      = len(df)
        high_count = len(df[df["confidence_tier"] == "HIGH"])
        med_count  = len(df[df["confidence_tier"] == "MEDIUM"])
        low_count  = len(df[df["confidence_tier"] == "LOW"])
        fraud_confirmed = int(df["is_fraud"].sum()) if "is_fraud" in df.columns else 0

        # Render KPI cards using the CSS classes defined in finpulse_app.py
        k1, k2, k3, k4, k5 = st.columns(5)
        with k1:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-label">Transactions Sampled</div>
                <div class="kpi-value">{total:,}</div>
                <div class="kpi-delta">Stratified sample · Gold layer</div>
            </div>""", unsafe_allow_html=True)
        _high_pct_kpi = f"{high_count/total*100:.1f}% of sample" if total > 0 else "—"
        _med_pct_kpi  = f"{med_count/total*100:.1f}% of sample"  if total > 0 else "—"
        _low_pct_kpi  = f"{low_count/total*100:.1f}% cleared"    if total > 0 else "—"
        with k2:
            st.markdown(f"""
            <div class="kpi-card accent-red">
                <div class="kpi-label">High Risk</div>
                <div class="kpi-value">{high_count:,}</div>
                <div class="kpi-delta">AI + rules both agree · {_high_pct_kpi}</div>
            </div>""", unsafe_allow_html=True)
        with k3:
            st.markdown(f"""
            <div class="kpi-card accent-amber">
                <div class="kpi-label">Medium Risk</div>
                <div class="kpi-value">{med_count:,}</div>
                <div class="kpi-delta">One signal flagged · {_med_pct_kpi}</div>
            </div>""", unsafe_allow_html=True)
        with k4:
            st.markdown(f"""
            <div class="kpi-card accent-green">
                <div class="kpi-label">Normal</div>
                <div class="kpi-value">{low_count:,}</div>
                <div class="kpi-delta">{_low_pct_kpi} · no suspicious pattern</div>
            </div>""", unsafe_allow_html=True)
        with k5:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-label">Confirmed Fraud</div>
                <div class="kpi-value">{fraud_confirmed:,}</div>
                <div class="kpi-delta">Ground truth · dataset label</div>
            </div>""", unsafe_allow_html=True)

        st.markdown('<div class="section-label">Anomaly Scatter Plot</div>', unsafe_allow_html=True)

        # ── "How to read this chart" guide strip ─────────────────────────────
        # Three plain-English cards so any new user immediately understands
        # the axes, colour coding, and what action to take.
        _high_pct = f"{high_count / total * 100:.1f}%" if total > 0 else "—"
        st.markdown(f"""
        <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;margin-bottom:14px;">

          <div style="background:#111D30;border:1px solid rgba(96,165,250,0.12);
                      border-left:3px solid #60A5FA;border-radius:3px;padding:12px 14px;">
            <div style="font-size:9px;letter-spacing:0.14em;text-transform:uppercase;
                        color:#4A6080;margin-bottom:6px;font-weight:600;">X AXIS — Transaction Size</div>
            <div style="font-size:12px;color:#C8D8F0;line-height:1.55;">
              How much money moved in a single transaction.
              Fraud tends to involve <strong style="color:#60A5FA">large amounts</strong> —
              criminals move as much as possible before being caught.
              Dots further right = bigger transactions.
            </div>
          </div>

          <div style="background:#111D30;border:1px solid rgba(96,165,250,0.12);
                      border-left:3px solid #F87171;border-radius:3px;padding:12px 14px;">
            <div style="font-size:9px;letter-spacing:0.14em;text-transform:uppercase;
                        color:#4A6080;margin-bottom:6px;font-weight:600;">Y AXIS — Balance Gap</div>
            <div style="font-size:12px;color:#C8D8F0;line-height:1.55;">
              Money that went missing after the transaction.
              If you sent $10,000 but your balance only dropped $2,000,
              the <strong style="color:#F87171">$8,000 gap</strong> is unexplained —
              a strong fraud signal. Higher Y = more suspicious.
            </div>
          </div>

          <div style="background:#111D30;border:1px solid rgba(96,165,250,0.12);
                      border-left:3px solid #FBBF24;border-radius:3px;padding:12px 14px;">
            <div style="font-size:9px;letter-spacing:0.14em;text-transform:uppercase;
                        color:#4A6080;margin-bottom:6px;font-weight:600;">DOT COLOUR — Risk Tier</div>
            <div style="font-size:12px;color:#C8D8F0;line-height:1.55;">
              <span style="color:#F87171;font-weight:600;">Red</span> = both the AI model
              and domain rules agree it's suspicious.<br>
              <span style="color:#FBBF24;font-weight:600;">Amber</span> = one signal flagged it — warrants a look.<br>
              <span style="color:#4A6080;">Grey</span> = normal, no flags raised.
              <strong style="color:#FFFFFF">{_high_pct}</strong> of this sample is red.
            </div>
          </div>

        </div>
        """, unsafe_allow_html=True)

        # ── Scatter plot ──────────────────────────────────────────────────────
        fig = build_scatter(df)

        # use_container_width=True: chart fills the full column width
        # config disables the Plotly toolbar logo but keeps zoom/pan
        st.plotly_chart(
            fig,
            use_container_width=True,
            config={
                "displaylogo": False,
                "modeBarButtonsToRemove": ["select2d"],  # keep lasso, remove box select
            },
        )

        # ── Flagged accounts table ─────────────────────────────────────────
        st.markdown(
            '<div class="section-label">Flagged Accounts — Passed to INVESTIGATE</div>',
            unsafe_allow_html=True,
        )

        # ── Glossary card — plain-English column definitions ──────────────
        st.markdown("""
        <details style="margin-bottom:14px;cursor:pointer;">
          <summary style="font-size:10px;color:#4A6080;letter-spacing:0.12em;
                          text-transform:uppercase;font-weight:600;
                          list-style:none;display:flex;align-items:center;gap:6px;">
            <span style="color:#60A5FA;">+</span> What do these columns mean?
          </summary>
          <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;
                      margin-top:10px;padding:12px;
                      background:#111D30;border-radius:3px;
                      border:1px solid rgba(96,165,250,0.10);">
            <div>
              <div style="font-size:9px;color:#60A5FA;font-weight:700;
                          text-transform:uppercase;letter-spacing:0.1em;margin-bottom:4px;">Amount</div>
              <div style="font-size:11px;color:#7E9BB8;line-height:1.5;">
                The total USD moved in this transaction. Large amounts are a primary fraud indicator.
              </div>
            </div>
            <div>
              <div style="font-size:9px;color:#F87171;font-weight:700;
                          text-transform:uppercase;letter-spacing:0.1em;margin-bottom:4px;">Balance Gap</div>
              <div style="font-size:11px;color:#7E9BB8;line-height:1.5;">
                Money unaccounted for after the transaction. If $10K was sent but the balance
                only dropped $3K, the $7K gap is the red flag.
              </div>
            </div>
            <div>
              <div style="font-size:9px;color:#FBBF24;font-weight:700;
                          text-transform:uppercase;letter-spacing:0.1em;margin-bottom:4px;">Gap Ratio</div>
              <div style="font-size:11px;color:#7E9BB8;line-height:1.5;">
                Balance Gap ÷ Amount. A ratio of 1.0 means 100% of the transaction amount
                vanished — the most extreme case. Above 0.5 is highly suspicious.
              </div>
            </div>
            <div>
              <div style="font-size:9px;color:#60A5FA;font-weight:700;
                          text-transform:uppercase;letter-spacing:0.1em;margin-bottom:4px;">Anomaly Score</div>
              <div style="font-size:11px;color:#7E9BB8;line-height:1.5;">
                0–100 scale from the AI model. Higher = more unusual compared to the rest
                of the dataset. Scores above 75 are flagged HIGH risk.
              </div>
            </div>
          </div>
        </details>
        """, unsafe_allow_html=True)

        if flagged.empty:
            st.markdown(
                '<div class="guard-msg">No accounts flagged at current threshold. '
                'Lower the contamination value to increase sensitivity.</div>',
                unsafe_allow_html=True,
            )
        else:
            # ── Search + filter controls ──────────────────────────────────
            _PAGE_SIZE = 25
            sf_col, tf_col, _ = st.columns([2, 1, 2])
            with sf_col:
                _search = st.text_input(
                    "Search account ID",
                    placeholder="e.g. C114526666",
                    label_visibility="collapsed",
                ).strip()
            with tf_col:
                _tier_filter = st.selectbox(
                    "Tier",
                    options=["ALL", "HIGH", "MEDIUM"],
                    index=0,
                    label_visibility="collapsed",
                )

            # Sort descending by score, apply search + tier filter
            _sorted = flagged.sort_values("anomaly_score", ascending=False)
            if _search:
                _sorted = _sorted[_sorted["account_id"].str.contains(_search, case=False, na=False)]
            if _tier_filter != "ALL":
                _sorted = _sorted[_sorted["confidence_tier"] == _tier_filter]

            _total_filtered = len(_sorted)
            _total_pages    = max(1, (_total_filtered + _PAGE_SIZE - 1) // _PAGE_SIZE)

            # Persist current page in session_state; reset to 1 when filters change
            _filter_key = f"{_search}|{_tier_filter}"
            if st.session_state.get("_hunt_filter_key") != _filter_key:
                st.session_state["_hunt_filter_key"] = _filter_key
                st.session_state["_hunt_page"]       = 1
            _current_page = st.session_state.get("_hunt_page", 1)
            _current_page = max(1, min(_current_page, _total_pages))

            _start = (_current_page - 1) * _PAGE_SIZE
            _page_df = _sorted.iloc[_start : _start + _PAGE_SIZE]

            # ── Table ─────────────────────────────────────────────────────
            rows_html = ""
            for _, row in _page_df.iterrows():
                _is_high  = row["confidence_tier"] == "HIGH"
                _row_style = ' style="background:rgba(248,113,113,0.06);"' if _is_high else ""
                tier_html  = (
                    '<span class="badge badge-red">HIGH</span>'
                    if _is_high
                    else '<span class="badge badge-amber">MEDIUM</span>'
                )
                fraud_html = (
                    '<span class="badge badge-red">CONFIRMED</span>'
                    if row.get("is_fraud", 0) == 1
                    else '<span class="badge badge-green">UNCONFIRMED</span>'
                )

                # Score bar — coloured progress bar + numeric label
                _score     = float(row['anomaly_score'])
                _bar_color = (
                    "#F87171" if _score >= 75
                    else "#FBBF24" if _score >= 50
                    else "#4A6080"
                )
                _score_html = f"""
                <div style="display:flex;align-items:center;gap:6px;min-width:90px;">
                  <div style="flex:1;height:5px;background:rgba(255,255,255,0.07);
                              border-radius:2px;overflow:hidden;">
                    <div style="width:{_score:.0f}%;height:100%;
                                background:{_bar_color};border-radius:2px;"></div>
                  </div>
                  <span style="font-size:11px;font-family:'JetBrains Mono',monospace;
                               color:{_bar_color};min-width:28px;text-align:right;">
                    {_score:.0f}
                  </span>
                </div>"""

                rows_html += f"""
                <tr{_row_style}>
                    <td><span class="mono">{row['account_id']}</span></td>
                    <td class="mono">{row.get('type','—')}</td>
                    <td><span class="amount">${row['amount']:,.0f}</span></td>
                    <td class="mono">${row['balance_gap']:,.0f}</td>
                    <td class="mono">{row['gap_ratio']:.2f}</td>
                    <td>{_score_html}</td>
                    <td>{tier_html}</td>
                    <td>{fraud_html}</td>
                </tr>"""

            st.markdown(f"""
            <div class="fp-card" style="padding:0;overflow-x:auto;">
                <table class="data-table">
                    <thead><tr>
                        <th>Account ID</th>
                        <th>Type</th>
                        <th>Amount</th>
                        <th>Balance Gap</th>
                        <th>Gap Ratio</th>
                        <th>Anomaly Score</th>
                        <th>Risk Tier</th>
                        <th>Fraud Status</th>
                    </tr></thead>
                    <tbody>{rows_html}</tbody>
                </table>
            </div>""", unsafe_allow_html=True)

            # ── Pagination controls ────────────────────────────────────────
            st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

            _p_cols = st.columns([2, 5, 2])
            with _p_cols[0]:
                st.markdown(
                    f'<div style="font-size:11px;color:#7E9BB8;padding-top:6px;">'
                    f'Showing {_start+1}–{min(_start+_PAGE_SIZE, _total_filtered)}'
                    f' of <strong style="color:#FFFFFF">{_total_filtered:,}</strong> flagged'
                    f'</div>',
                    unsafe_allow_html=True,
                )
            with _p_cols[1]:
                # Page number buttons — show at most 7 page buttons centred on current page
                _window_start = max(1, _current_page - 3)
                _window_end   = min(_total_pages, _window_start + 6)
                _window_start = max(1, _window_end - 6)

                btn_cols = st.columns(
                    [1] + [0.6] * (_window_end - _window_start + 1) + [1]
                )
                # Prev arrow
                with btn_cols[0]:
                    if st.button("←", disabled=(_current_page == 1), key="_hunt_prev"):
                        st.session_state["_hunt_page"] = _current_page - 1
                        st.rerun()
                # Page number buttons
                for _idx, _pn in enumerate(range(_window_start, _window_end + 1), start=1):
                    with btn_cols[_idx]:
                        _label = f"**{_pn}**" if _pn == _current_page else str(_pn)
                        if st.button(_label, key=f"_hunt_pg_{_pn}"):
                            st.session_state["_hunt_page"] = _pn
                            st.rerun()
                # Next arrow
                with btn_cols[-1]:
                    if st.button("→", disabled=(_current_page == _total_pages), key="_hunt_next"):
                        st.session_state["_hunt_page"] = _current_page + 1
                        st.rerun()
            with _p_cols[2]:
                st.markdown(
                    f'<div style="font-size:11px;color:#7E9BB8;padding-top:6px;text-align:right;">'
                    f'Page {_current_page} / {_total_pages}'
                    f'</div>',
                    unsafe_allow_html=True,
                )

    else:
        # No results yet — show instructions
        st.markdown(
            '<div class="guard-msg">'
            'Click <strong>Run Detection</strong> to start Isolation Forest scoring '
            'across the Gold layer balance discrepancy table. '
            'Adjust the contamination threshold to control detection sensitivity.'
            '</div>',
            unsafe_allow_html=True,
        )