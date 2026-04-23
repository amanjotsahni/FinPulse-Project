"""
FinPulse — Agent 2: ACCOUNT INVESTIGATION
==========================================
Clean, professional AML investigation screen.
All HTML is self-contained per st.markdown() call — no split divs.
Chat input styled like a premium AI (dark pill).
Suggestion chips are small, left-aligned, natural case — no caps.
"""

import streamlit as st
import pandas as pd
import numpy as np
import os
import sys
import html as html_lib
from pathlib import Path
from datetime import datetime

_PROJECT_ROOT = Path(__file__).parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


# ── Groq client ───────────────────────────────────────────────────────────────
def _get_client():
    if "groq_client" not in st.session_state:
        import dotenv
        from groq import Groq
        dotenv.load_dotenv(r"C:\FinPulse Project\.env")
        api_key = os.getenv("GROQ_API_KEY")
        if not api_key:
            raise ValueError("GROQ_API_KEY not found in .env")
        st.session_state.groq_client = Groq(api_key=api_key)
    return st.session_state.groq_client


# ── Databricks helper ─────────────────────────────────────────────────────────
def _query(sql: str) -> pd.DataFrame:
    from config import DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN
    from databricks import sql as dbsql
    conn = dbsql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    )
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(rows, columns=cols)
    finally:
        cur.close()
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# DATA RETRIEVAL
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=600, show_spinner=False)
def _fetch_account_txns(account_id: str) -> pd.DataFrame:
    """
    Fetch full transaction history for a flagged account with bank names and corridor risk.
    """
    from config import DATABRICKS_SCHEMA
    df = _query(f"""
        SELECT 
            t.from_bank, t.to_bank, 
            COALESCE(a1.bank_name, CONCAT('Institution ', CAST(t.from_bank AS STRING))) as from_bank_name,
            COALESCE(a2.bank_name, CONCAT('Institution ', CAST(t.to_bank AS STRING))) as to_bank_name,
            t.payment_format as type, t.amount,
            t.payment_currency as Payment_Currency,
            t.receiving_currency as Receiving_Currency,
            t.is_cross_currency, t.is_high_value_wire, t.is_fraud,
            t.txn_time as transaction_timestamp,
            -- Extra enrichment: Corridor Risk % from cross_bank_flow
            COALESCE(cbf.laundering_rate_pct, 0.0) as corridor_risk_pct
        FROM {DATABRICKS_SCHEMA}.flagged_account_transactions t
        LEFT JOIN (
            SELECT bank_id, FIRST(bank_name) as bank_name 
            FROM {DATABRICKS_SCHEMA}.accounts_silver GROUP BY bank_id
        ) a1 ON t.from_bank = a1.bank_id
        LEFT JOIN (
            SELECT bank_id, FIRST(bank_name) as bank_name 
            FROM {DATABRICKS_SCHEMA}.accounts_silver GROUP BY bank_id
        ) a2 ON t.to_bank = a2.bank_id
        LEFT JOIN {DATABRICKS_SCHEMA}.cross_bank_flow cbf
            ON t.from_bank = cbf.from_bank AND t.to_bank = cbf.to_bank
        WHERE t.account_id = '{account_id}'
        ORDER BY t.txn_time DESC
        LIMIT 100
    """)
    if df.empty:
        return df
    
    # Process numeric columns
    for col in ["amount", "corridor_risk_pct"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    for col in ["is_fraud", "is_cross_currency", "is_high_value_wire"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    
    # Extract Date and Time for the forensic table
    try:
        ts = pd.to_datetime(df["transaction_timestamp"])
        df["txn_date"] = ts.dt.strftime("%Y-%m-%d")
        df["txn_time_only"] = ts.dt.strftime("%H:%M:%S")
        df["hour"] = ts.dt.hour
    except Exception:
        df["txn_date"] = "—"
        df["txn_time_only"] = "—"
        df["hour"] = -1
        
    return df


@st.cache_data(ttl=600, show_spinner=False)
def _fetch_fraud_rates() -> pd.DataFrame:
    from config import DATABRICKS_SCHEMA
    try:
        return _query(f"""
            SELECT Payment_Format as type, total_transactions, fraud_count, fraud_rate_pct
            FROM {DATABRICKS_SCHEMA}.fraud_rate_by_type
            ORDER BY fraud_rate_pct DESC
        """)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=600, show_spinner=False)
def _fetch_investigation_queue() -> pd.DataFrame:
    """
    Load the full investigation_queue Gold table — every account that warrants
    investigation regardless of Agent 1's sample:
      - All confirmed laundering accounts (is_laundering=1)
      - All multi-indicator accounts (2+ AML risk-flagged transactions)
    Sorted by priority_score desc.
    """
    from config import DATABRICKS_SCHEMA
    try:
        df = _query(f"""
            SELECT account_id, indicator_count as risk_indicator_count, total_amount,
                   cross_currency_count,
                   is_laundering as is_fraud, flag_confirmed_fraud, flag_multi_risk,
                   priority_score
            FROM {DATABRICKS_SCHEMA}.investigation_queue
            ORDER BY priority_score DESC
        """)
        for col in ["total_amount", "cross_currency_count", "priority_score"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        df["risk_indicator_count"] = pd.to_numeric(df["risk_indicator_count"], errors="coerce").fillna(0).astype(int)
        df["is_fraud"] = pd.to_numeric(df["is_fraud"], errors="coerce").fillna(0).astype(int)
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=600, show_spinner=False)
def _fetch_peer_accounts() -> pd.DataFrame:
    """Top 25 accounts by total exposure — peer ranking for LLM context."""
    from config import DATABRICKS_SCHEMA
    try:
        df = _query(f"""
            SELECT account_id, entity_name, bank_name,
                   indicator_count as risk_indicator_count,
                   total_amount as total_amount_transacted,
                   cross_currency_count, high_value_wire_count,
                   priority_score
            FROM {DATABRICKS_SCHEMA}.investigation_queue
            ORDER BY total_amount DESC
            LIMIT 25
        """)
        for c in ["total_amount_transacted", "cross_currency_count",
                  "high_value_wire_count", "priority_score"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=600, show_spinner=False)
def _fetch_entity_profile(account_id: str) -> pd.DataFrame:
    """
    Entity laundering profile from entity_laundering_profile Gold table.
    Surfaces entity-type level risk (Corporation vs Partnership vs Sole Prop)
    and the account's bank network footprint.
    """
    from config import DATABRICKS_SCHEMA
    try:
        return _query(f"""
            SELECT entity_type_encoded, entity_name,
                   transaction_count, total_volume_usd,
                   laundering_count, laundering_rate_pct, banks_involved
            FROM {DATABRICKS_SCHEMA}.entity_laundering_profile
            WHERE entity_name = (
                SELECT entity_name FROM {DATABRICKS_SCHEMA}.investigation_queue
                WHERE account_id = '{account_id}' LIMIT 1
            )
        """)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=600, show_spinner=False)
def _fetch_bank_corridor(from_bank, to_bank) -> pd.DataFrame:
    """
    Cross-bank flow stats for a specific bank pair from cross_bank_flow Gold table.
    Tells the LLM whether this account's routing is a known-hot corridor.
    """
    from config import DATABRICKS_SCHEMA
    try:
        if from_bank is None or to_bank is None:
            return pd.DataFrame()
        return _query(f"""
            SELECT from_bank, to_bank, transaction_count, total_volume_usd,
                   laundering_count, laundering_rate_pct, cross_currency_count
            FROM {DATABRICKS_SCHEMA}.cross_bank_flow
            WHERE from_bank = {int(from_bank)} AND to_bank = {int(to_bank)}
        """)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=600, show_spinner=False)
def _fetch_hourly_pattern() -> pd.DataFrame:
    """
    Hourly laundering pattern from hourly_pattern_analysis Gold table.
    Used to flag whether this account's transaction hours match known
    off-hours laundering windows (e.g., 2–5 AM structuring activity).
    """
    from config import DATABRICKS_SCHEMA
    try:
        df = _query(f"""
            SELECT hour_of_day, payment_format,
                   transaction_count, avg_amount, fraud_count
            FROM {DATABRICKS_SCHEMA}.hourly_pattern_analysis
            ORDER BY fraud_count DESC
        """)
        for c in ["transaction_count", "avg_amount", "fraud_count"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=600, show_spinner=False)
def _fetch_market_context() -> pd.DataFrame:
    """
    Transaction-market correlation from transaction_market_context Gold table.
    Surfaces laundering_intensity_vs_10d_avg and is_market_stress_day —
    tells the LLM whether laundering activity spikes during banking stress.
    """
    from config import DATABRICKS_SCHEMA
    try:
        df = _query(f"""
            SELECT transaction_date, laundering_count, laundering_rate_pct,
                   cross_currency_count, high_value_wire_count,
                   jpm_daily_return_pct, gs_daily_return_pct,
                   is_market_stress_day, laundering_intensity_vs_10d_avg
            FROM {DATABRICKS_SCHEMA}.transaction_market_context
            ORDER BY transaction_date DESC
            LIMIT 10
        """)
        for c in ["laundering_count", "laundering_rate_pct", "cross_currency_count",
                  "high_value_wire_count", "jpm_daily_return_pct", "gs_daily_return_pct",
                  "laundering_intensity_vs_10d_avg"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=600, show_spinner=False)
def _fetch_market() -> pd.DataFrame:
    from config import DATABRICKS_SCHEMA
    try:
        return _query(f"""
            SELECT ticker, start_price, latest_price, total_return_pct, performance_rank
            FROM {DATABRICKS_SCHEMA}.stocks_performance_ranking
            ORDER BY performance_rank ASC
        """)
    except Exception:
        return pd.DataFrame()


# ══════════════════════════════════════════════════════════════════════════════
# CONTEXT ASSEMBLY (A in RAG)
# ══════════════════════════════════════════════════════════════════════════════

def _account_context_str(account_id: str) -> str:
    """
    Full IBM AML account context block for the LLM.
    Includes: transaction history, bank routing, currency flow, entity profile,
    bank-corridor risk, and off-hours pattern detection.
    """
    df = _fetch_account_txns(account_id)
    if df.empty:
        return f"No flagged transaction records found for {account_id}."

    n              = len(df)
    total_amt      = df["amount"].sum()
    cross_currency = int(df["is_cross_currency"].sum())
    high_wire      = int(df["is_high_value_wire"].sum())
    fraud_count    = int(df["is_fraud"].sum())
    types          = df["type"].value_counts().to_dict()
    unique_banks_from = df["from_bank"].nunique() if "from_bank" in df.columns else "?"
    unique_banks_to   = df["to_bank"].nunique()   if "to_bank"   in df.columns else "?"
    currencies_paid   = df["Payment_Currency"].unique().tolist()   if "Payment_Currency"   in df.columns else []
    currencies_rcvd   = df["Receiving_Currency"].unique().tolist() if "Receiving_Currency" in df.columns else []
    max_single_txn    = df["amount"].max()

    # Off-hours detection: flag transactions outside 09:00–17:00 as suspicious
    off_hours_count = 0
    if "transaction_timestamp" in df.columns:
        try:
            ts = pd.to_datetime(df["transaction_timestamp"], errors="coerce")
            off_hours_count = int(((ts.dt.hour < 9) | (ts.dt.hour >= 17)).sum())
        except Exception:
            pass

    # Dominant bank corridor
    primary_from = df["from_bank"].mode()[0] if "from_bank" in df.columns and not df.empty else None
    primary_to   = df["to_bank"].mode()[0]   if "to_bank"   in df.columns and not df.empty else None

    # Bank corridor risk from cross_bank_flow
    corridor_str = ""
    if primary_from is not None and primary_to is not None:
        bdf = _fetch_bank_corridor(primary_from, primary_to)
        if not bdf.empty:
            r = bdf.iloc[0]
            corridor_str = (
                f"\nBANK CORRIDOR (primary routing {primary_from} → {primary_to}):\n"
                f"  dataset-wide txns on this route: {int(r.get('transaction_count',0)):,}"
                f"  laundering_rate: {float(r.get('laundering_rate_pct',0)):.4f}%"
                f"  cross_currency_count: {int(r.get('cross_currency_count',0)):,}"
            )

    # Entity profile
    entity_str = ""
    edf = _fetch_entity_profile(account_id)
    if not edf.empty:
        r = edf.iloc[0]
        enc = int(r.get("entity_type_encoded", -1))
        etype = {0: "Corporation", 1: "Partnership", 2: "Sole Proprietorship"}.get(enc, "Unknown")
        entity_str = (
            f"\nENTITY PROFILE ({r.get('entity_name','?')}):\n"
            f"  entity_type={etype}"
            f"  dataset_laundering_rate={float(r.get('laundering_rate_pct',0)):.4f}%"
            f"  banks_involved={int(r.get('banks_involved',0))}"
            f"  total_volume={float(r.get('total_volume_usd',0)):,.0f} USD"
        )

    rows = []
    for i, (_, r) in enumerate(df.iterrows(), 1):
        cc_flag  = " | CROSS_CURRENCY" if r.get("is_cross_currency") else ""
        hwv_flag = " | HVW>100K"       if r.get("is_high_value_wire") else ""
        rows.append(
            f"[{i:02d}] {r['transaction_timestamp']} | {r['type']:14s}"
            f" | from={r.get('from_bank','?')} to={r.get('to_bank','?')}"
            f" | paid={r['amount']:>13,.0f} | {r.get('Payment_Currency','?')}→{r.get('Receiving_Currency','?')}"
            f" | launder={'YES' if r['is_fraud'] else 'no'}{cc_flag}{hwv_flag}"
        )

    return (
        f"ACCOUNT {account_id}  ({n} transactions in investigation table)\n"
        f"  total_amount_paid={total_amt:>15,.0f} USD  max_single_txn={max_single_txn:,.0f} USD\n"
        f"  cross_currency={cross_currency}/{n}  high_value_wire={high_wire}/{n}"
        f"  off_hours_txns={off_hours_count}/{n}\n"
        f"  laundering_confirmed={fraud_count}/{n}  payment_formats={types}\n"
        f"  routing: {unique_banks_from} sending bank(s), {unique_banks_to} receiving bank(s)\n"
        f"  currencies_paid={currencies_paid}  currencies_received={currencies_rcvd}"
        + corridor_str + entity_str +
        f"\n\nROW DETAIL (most recent first, up to 50):\n"
        + "\n".join(rows)
    )


def _peer_context_str(account_id: str) -> str:
    """Peer ranking with entity name, bank name, and cross-currency/wire counts."""
    df = _fetch_peer_accounts()
    if df.empty:
        return "Peer data unavailable."
    lines = ["PEER RANKING — top 25 accounts by total amount transacted:"]
    rank = None
    for i, (_, r) in enumerate(df.iterrows(), 1):
        marker = "  <== THIS ACCOUNT" if r["account_id"] == account_id else ""
        entity = r.get("entity_name", "?") or "?"
        bank   = r.get("bank_name", "?") or "?"
        lines.append(
            f"  #{i:02d}  {r['account_id']}  {float(r['total_amount_transacted']):>14,.0f} USD"
            f"  indicators={int(r['risk_indicator_count'])}"
            f"  cross_ccy={int(r.get('cross_currency_count',0))}"
            f"  hvw={int(r.get('high_value_wire_count',0))}"
            f"  [{entity} @ {bank}]{marker}"
        )
        if r["account_id"] == account_id:
            rank = i
    lines.append(
        f"\nRANK: {account_id} is {'#'+str(rank) if rank else 'outside top 25'}"
        f" of 25 highest-exposure accounts."
    )
    return "\n".join(lines)


def _fraud_rate_str() -> str:
    df = _fetch_fraud_rates()
    if df.empty:
        return "Fraud rate data unavailable."
    lines = ["IBM AML DATASET — LAUNDERING RATES BY PAYMENT FORMAT:"]
    for _, r in df.iterrows():
        lines.append(
            f"  {r['type']:10s}: {int(r['total_transactions']):>8,} txns"
            f"  {int(r['fraud_count']):>6,} fraud  {float(r['fraud_rate_pct']):.4f}%"
        )
    return "\n".join(lines)


def _market_str() -> str:
    """
    Combined market context: 2-year stock performance + daily laundering-vs-market
    correlation from transaction_market_context Gold table.
    Surfaces laundering_intensity_vs_10d_avg and is_market_stress_day.
    """
    perf_df  = _fetch_market()
    ctx_df   = _fetch_market_context()

    lines = ["MARKET CONTEXT — 2-Year Stock Performance (financial sector):"]
    if not perf_df.empty:
        for _, r in perf_df.iterrows():
            ret   = float(r["total_return_pct"])
            sign  = "+" if ret >= 0 else ""
            trend = "STRONG GAIN" if ret > 30 else ("MODERATE GAIN" if ret > 0 else "LOSS")
            lines.append(
                f"  #{int(r['performance_rank'])} {r['ticker']}: "
                f"${float(r['start_price']):.0f} → ${float(r['latest_price']):.0f}"
                f"  ({sign}{ret:.1f}%)  [{trend}]"
            )

    if not ctx_df.empty:
        lines.append("\nLAUNDERING-MARKET CORRELATION (recent 10 days):")
        stress_days = int(ctx_df["is_market_stress_day"].sum()) if "is_market_stress_day" in ctx_df else 0
        avg_intensity = float(ctx_df["laundering_intensity_vs_10d_avg"].mean()) if "laundering_intensity_vs_10d_avg" in ctx_df else 0
        lines.append(
            f"  market_stress_days={stress_days}/10"
            f"  avg_laundering_intensity_vs_10d={avg_intensity:.3f}x"
        )
        for _, r in ctx_df.head(5).iterrows():
            stress = " [STRESS DAY]" if r.get("is_market_stress_day") else ""
            lines.append(
                f"  {r.get('transaction_date','?')}:"
                f" laundering={int(r.get('laundering_count',0))}"
                f"  cross_ccy={int(r.get('cross_currency_count',0))}"
                f"  jpm={float(r.get('jpm_daily_return_pct',0)):+.2f}%"
                f"  gs={float(r.get('gs_daily_return_pct',0)):+.2f}%"
                f"  intensity={float(r.get('laundering_intensity_vs_10d_avg',0)):.2f}x{stress}"
            )

    lines.append(
        "\n  Consider: laundering_intensity > 1.5x on market stress days may signal"
        " coordinated capital flight using the banking sector downturn as cover."
    )
    return "\n".join(lines)


def _agent1_forensic_str(account_id: str, agent1_results: list, all_scores: list) -> str:
    """
    Full Agent 1 forensic block: Isolation Forest scores, 3-rule breakdown,
    bank-corridor risk, entity metadata, score band percentile.
    """
    recs = [r for r in agent1_results if r.get("account_id") == account_id]
    if not recs:
        return f"No Agent 1 ML records for {account_id} (account may be from Gold queue only)."

    scores      = [r.get("anomaly_score", 0) for r in recs]
    max_score   = max(scores)
    avg_score   = sum(scores) / len(scores)
    tier        = recs[0].get("confidence_tier", "—")
    score_band  = recs[0].get("score_band", "—")
    entity_name = recs[0].get("entity_name", "Unknown")
    bank_name   = recs[0].get("bank_name", "Unknown")
    ml_flags    = sum(r.get("ml_flag", 0) for r in recs)
    rule_flags  = sum(r.get("rule_flag", 0) for r in recs)
    both_flags  = sum(1 for r in recs if r.get("ml_flag",0) and r.get("rule_flag",0))
    cross_ccy   = sum(r.get("is_cross_currency", 0) for r in recs)
    high_wire   = sum(r.get("is_high_value_wire", 0) for r in recs)
    rule1       = sum(r.get("rule1_flag", 0) for r in recs)   # integration: HVW >100K
    rule2       = sum(r.get("rule2_flag", 0) for r in recs)   # layering: cross-ccy + above avg
    rule3       = sum(r.get("rule3_flag", 0) for r in recs)   # hot corridor: bank pair >0.5%
    bp_rate     = max((r.get("bank_pair_launder_rate", 0) for r in recs), default=0)
    pct         = int(np.searchsorted(sorted(all_scores), max_score) / max(len(all_scores), 1) * 100)

    lines = [
        f"AGENT 1 DETECTION ENGINE — {account_id}:",
        f"  entity={entity_name}  bank={bank_name}",
        f"  TIER={tier}  score_band={score_band}",
        f"  anomaly_score={max_score:.1f}/100  (top {100-pct}th percentile of all sampled)",
        f"  avg_score={avg_score:.1f}  n_scored_txns={len(recs)}",
        f"  ml_flags={ml_flags}/{len(recs)}  rule_flags={rule_flags}/{len(recs)}"
        f"  dual_flags={both_flags}/{len(recs)}",
        f"  cross_currency={cross_ccy}/{len(recs)}  high_value_wire={high_wire}/{len(recs)}",
        f"  RULE BREAKDOWN:",
        f"    Rule1 (Integration — HVW >$100K):      {rule1}/{len(recs)}",
        f"    Rule2 (Layering — cross-ccy + above avg): {rule2}/{len(recs)}",
        f"    Rule3 (Hot corridor — bank pair >0.5%):  {rule3}/{len(recs)}",
        f"  bank_pair_laundering_rate={bp_rate:.4f}%",
        f"  DUAL_FLAG={both_flags} = Isolation Forest AND rules both agree = highest certainty",
    ]
    return "\n".join(lines)


def _hourly_pattern_str(account_id: str) -> str:
    """
    Hourly laundering pattern context. Tells the LLM which hours have the
    highest laundering counts in the IBM AML dataset — used to flag whether
    this account's off-hours transactions match known laundering windows.
    """
    df = _fetch_hourly_pattern()
    if df.empty:
        return ""
    top = df.groupby("hour_of_day")["fraud_count"].sum().nlargest(5).reset_index()
    lines = ["HOURLY LAUNDERING PATTERN (IBM AML dataset, top 5 hours by laundering count):"]
    for _, r in top.iterrows():
        h = int(r["hour_of_day"])
        label = "OFF-HOURS" if h < 9 or h >= 17 else "business hours"
        lines.append(f"  Hour {h:02d}:00  laundering_count={int(r['fraud_count']):,}  [{label}]")
    lines.append(
        "  Off-hours transactions (outside 09:00–17:00) in high-laundering hours"
        " are a structuring/smurfing temporal signal."
    )
    return "\n".join(lines)


def route_context(question: str, account_id: str, agent1_results: list, all_scores: list) -> str:
    """
    Assemble full RAG context block for the LLM. Uses all 6 Gold tables:
      1. Agent 1 forensic scores (Isolation Forest + 3-rule breakdown)
      2. Account transaction history (flagged_account_transactions)
      3. Bank corridor risk (cross_bank_flow)
      4. Entity laundering profile (entity_laundering_profile)
      5. Peer ranking (investigation_queue top 25)
      6. Hourly laundering pattern (hourly_pattern_analysis)
      7. Laundering rate by payment format (fraud_rate_by_type)
      8. Market + laundering correlation (transaction_market_context + stocks_performance_ranking)
    """
    parts = [_agent1_forensic_str(account_id, agent1_results, all_scores)]
    if account_id and account_id != "— choose an account to begin —":
        parts.append(_account_context_str(account_id))
        parts.append(_peer_context_str(account_id))
        hourly = _hourly_pattern_str(account_id)
        if hourly:
            parts.append(hourly)
    parts.append(_fraud_rate_str())
    parts.append(_market_str())
    return "\n\n" + ("\n\n" + "─"*72 + "\n\n").join(parts)


# ══════════════════════════════════════════════════════════════════════════════
# LLM
# ══════════════════════════════════════════════════════════════════════════════

_SYSTEM_PROMPT = """You are a senior AML investigator at a global financial institution's FIU.
You are answering a compliance analyst's specific question about a flagged account.

DATASET: IBM AML HI-Small_Trans.csv — 5.07M synthetic transactions, 9 days (Sept 2022).
Payment formats: Wire Transfer, ACH, Cheque, Credit Card, Debit Card.
Signals are structural — no balances. Key red flags: cross-currency flow, high-value wires >100K, off-hours timing, and hot bank corridors.

AML TYPOLOGIES (name them when evidence supports):
- Layering: paying_currency ≠ receiving_currency — obscures trail through conversion
- Integration: wire transfers >100K — moves funds into legitimate financial system 
- Structuring: multiple transactions just below thresholds, off-hours clustering
- Hot corridor: bank pair with elevated historical laundering rate
- Round-tripping: funds exit and return via different routes or currencies

CONTEXT FOR EVERY ANSWER (pull from all of it — do not ignore any section):
- Exact amounts, payment formats, and bank IDs from transaction rows
- Corridor laundering rate: from_bank → to_bank historical % from cross_bank_flow
- Anomaly score: Isolation Forest percentile rank
- Entity type laundering rate from entity_laundering_profile
- Peer ranking vs top 25 accounts by exposure
- Hourly pattern: does this account's timing match known laundering windows?
- Market context: laundering intensity vs 10-day average, market stress day flag
- Payment format baseline laundering rate from fraud_rate_by_type

RESPONSE FORMATTING — YOU MUST ONLY OUTPUT IN THIS STRICT VISUAL FORMAT:

Do not simply write paragraphs of text. Every response must look like a beautiful UI.
Use these HTML components:

1. KEY VALUE / METRIC:
<div style="background:#1E293B; border-radius:6px; padding:12px; margin:8px 0; border:1px solid rgba(255,255,255,0.1);">
  <span style="color:#94A3B8; font-size:11px; text-transform:uppercase; font-weight:700;">{LABEL}</span><br>
  <span style="color:#FFFFFF; font-size:18px; font-weight:700;">{VALUE}</span>
</div>

2. SEVERE WARNING / FRAUD FLAG:
<div style="background:rgba(239,68,68,0.1); border-left:4px solid #EF4444; border-radius:4px; padding:12px; margin:8px 0;">
  <strong style="color:#EF4444;">🚨 {TITLE}:</strong> <span style="color:#F87171;">{TEXT}</span>
</div>

3. GENERAL INSIGHT / OBSERVATION:
<div style="background:rgba(96,165,250,0.1); border-left:4px solid #3B82F6; border-radius:4px; padding:12px; margin:8px 0;">
  <strong style="color:#60A5FA;">💡 {TITLE}:</strong> <span style="color:#E0E8F0;">{TEXT}</span>
</div>

4. TRANSACTION FLOW DIAGRAM:
<div style="background:rgba(15,30,53,0.5); border-radius:6px; padding:16px; margin:12px 0; text-align:center; border:1px dashed #3B82F6;">
  <span style="background:#2563EB; color:white; padding:4px 10px; border-radius:12px; font-size:12px; font-weight:bold;">{SOURCE}</span> 
  ➔ <span style="color:#94A3B8; font-size:12px;">{ACTION}</span> ➔ 
  <span style="background:#2563EB; color:white; padding:4px 10px; border-radius:12px; font-size:12px; font-weight:bold;">{DEST}</span>
</div>

RULES:
1. Break everything down into the visual HTML blocks provided above. Limit plain text to 1 short introductory sentence.
2. If explaining a calculation or logic, put it inside an Insight block.
3. Every single response MUST contain at least one of these visual elements.
4. Keep the text concise and punchy. You are an enterprise UI, not a chatbot.
5. NEVER repeat facts from prior turns.
6. Max 250 words total."""


def _build_messages(question: str, context: str, history: list, global_summary: str) -> list:
    # Pass prior Q&A as actual conversation turns so the model has full memory
    # and cannot claim it doesn't know what was already said
    messages = [{"role": "system", "content": _SYSTEM_PROMPT}]

    # Inject prior turns as real assistant messages (up to last 6)
    for turn in history[-6:]:
        messages.append({"role": "user",      "content": turn["question"]})
        messages.append({"role": "assistant", "content": turn["answer"]})

    # Current question with full context
    user_content = (
        f"=== GLOBAL DETECTION SUMMARY ===\n{global_summary}\n\n"
        f"=== TRANSACTION EVIDENCE + MARKET DATA ===\n{context}\n\n"
        f"=== NEW QUESTION (answer this — do not repeat what you already said above) ===\n{question}"
    )
    messages.append({"role": "user", "content": user_content})
    return messages


# ══════════════════════════════════════════════════════════════════════════════
# UI HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _risk_gauge_html(score: float, tier: str) -> str:
    pct   = min(score / 100, 1.0)
    r     = 44
    circ  = 3.14159 * r
    dash  = pct * circ
    color = "#F87171" if tier == "HIGH" else "#FBBF24" if tier == "MEDIUM" else "#34D399"
    return (
        f'<div style="display:flex;flex-direction:column;align-items:center;">'
        f'<svg width="110" height="62" viewBox="0 0 110 62">'
        f'<path d="M 11 52 A 44 44 0 0 1 99 52" fill="none" stroke="rgba(255,255,255,0.06)" stroke-width="8" stroke-linecap="round"/>'
        f'<path d="M 11 52 A 44 44 0 0 1 99 52" fill="none" stroke="{color}" stroke-width="8" stroke-linecap="round"'
        f' stroke-dasharray="{dash:.1f} {circ:.1f}" opacity="0.9"/>'
        f'<text x="55" y="46" text-anchor="middle" font-family="JetBrains Mono,monospace" font-size="18" font-weight="800" fill="{color}">{score:.0f}</text>'
        f'</svg>'
        f'</div>'
    )


def _signal_bar(label: str, value: int, total: int, color: str, note: str = "") -> str:
    pct = (value / max(total, 1)) * 100
    note_html = f'<span style="font-size:9px;color:#4A6080;margin-left:4px;">{note}</span>' if note else ""
    return (
        f'<div style="margin-bottom:10px;">'
        f'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:3px;">'
        f'<span style="font-size:10px;font-weight:600;color:#7E9BB8;text-transform:uppercase;letter-spacing:0.08em;">{label}</span>'
        f'<span style="font-family:\'JetBrains Mono\',monospace;font-size:11px;color:{color};font-weight:700;">{value}/{total}{note_html}</span>'
        f'</div>'
        f'<div style="height:4px;background:rgba(255,255,255,0.05);border-radius:2px;overflow:hidden;">'
        f'<div style="width:{pct:.0f}%;height:100%;background:{color};border-radius:2px;opacity:0.85;"></div>'
        f'</div>'
        f'</div>'
    )


# ══════════════════════════════════════════════════════════════════════════════
# EVIDENCE CARD — all HTML in one st.markdown call
# ══════════════════════════════════════════════════════════════════════════════

def _render_evidence_card(account_id: str, agent1_results: list, all_scores: list):
    recs = [r for r in agent1_results if r.get("account_id") == account_id]
    if not recs:
        st.markdown(
            '<div style="padding:16px;color:#7E9BB8;font-size:13px;">'
            f'No Agent 1 records for <code>{account_id}</code>. Run Fraud Detection first.</div>',
            unsafe_allow_html=True)
        return

    max_score  = max(r.get("anomaly_score", 0) for r in recs)
    tier       = recs[0].get("confidence_tier", "LOW")
    is_fraud   = any(r.get("is_fraud", 0) == 1 for r in recs)
    n          = len(recs)
    ml_flags   = sum(r.get("ml_flag", 0) for r in recs)
    rule_flags = sum(r.get("rule_flag", 0) for r in recs)
    both_flags = sum(1 for r in recs if r.get("ml_flag",0) and r.get("rule_flag",0))
    cross_ccy  = sum(r.get("is_cross_currency", 0) for r in recs)
    high_wire  = sum(r.get("is_high_value_wire", 0) for r in recs)
    total_amt  = sum(r.get("amount", 0) for r in recs)
    pct_rank   = int(np.searchsorted(sorted(all_scores), max_score) / max(len(all_scores),1) * 100)

    types: dict = {}
    for r in recs:
        t = r.get("type", "—")
        types[t] = types.get(t, 0) + 1

    tier_color  = "#F87171" if tier == "HIGH" else "#FBBF24" if tier == "MEDIUM" else "#34D399"
    tier_border = tier_color + "33"   # pre-compute — never interpolate hex+literal inside f-string
    tc_bg       = ("rgba(248,113,113,0.08)" if tier == "HIGH" else
                   "rgba(251,191,36,0.08)"  if tier == "MEDIUM" else
                   "rgba(52,211,153,0.08)")

    fraud_badge = (
        '<span style="background:rgba(248,113,113,0.15);color:#F87171;padding:3px 10px;'
        'border-radius:3px;font-size:10px;font-weight:700;letter-spacing:0.08em;">⚠ Confirmed Fraud</span>'
        if is_fraud else
        '<span style="background:rgba(96,165,250,0.08);color:#7E9BB8;padding:3px 10px;'
        'border-radius:3px;font-size:10px;font-weight:500;">No ground-truth label</span>'
    )
    type_chips = " ".join(
        f'<span style="background:rgba(96,165,250,0.1);color:#93C5FD;padding:2px 8px;'
        f'border-radius:3px;font-size:10px;font-weight:600;">{t} ×{c}</span>'
        for t, c in types.items()
    )
    percentile_text = f"Top {100-pct_rank}th percentile" if pct_rank < 100 else "99th+ percentile"
    gauge_html = _risk_gauge_html(max_score, tier)
    # Fetch transaction data early — used in both top card and table below
    txn_df = _fetch_account_txns(account_id)

    # --- CSS FOR THE SINGLE LARGE CARD (SMART LOOK) ---
    card_style = """
    <style>
    .forensic-shell {
        background: #111D32;
        border: none;
        border-radius: 8px;
        padding: 30px;
        margin-bottom: 24px;
        box-shadow: 0 4px 30px rgba(0,0,0,0.4);
    }
    .shell-tag { font-size: 10px; color: #4A6080; text-transform: uppercase; letter-spacing: 0.16em; font-weight: 800; margin-bottom: 8px; }
    .shell-title { font-size: 26px; font-weight: 800; color: #FFFFFF; font-family: monospace; margin-top: 5px; }
    .metrics-grid-compact {
        display: grid;
        grid-template-columns: repeat(4, 1fr);
        gap: 24px;
        margin: 25px 0;
    }
    .m-item .m-label { font-size: 9px; color: #4A6080; text-transform: uppercase; letter-spacing: 0.1em; font-weight: 700; margin-bottom: 4px; }
    .m-item .m-value { font-size: 18px; font-weight: 700; color: #FFFFFF; font-family: 'JetBrains Mono', monospace; }
    
    /* Plotly Polish */
    .stPlotlyChart { background: transparent !important; }
    </style>
    """
    st.markdown(card_style, unsafe_allow_html=True)

    # Wrap the entire top section in the "Forensic Shell" div style using a SINGLE HTML blob
    # This prevents Streamlit's auto-tag closing from breaking the unified CSS container.
    
    signal_bars = [
        _signal_bar("ML Flagged",      ml_flags,   n, "#60A5FA", "Isolation Forest"),
        _signal_bar("Rule Flagged",    rule_flags,  n, "#A78BFA", "Domain rules"),
        _signal_bar("Dual Flagged",    both_flags,  n, "#F87171", "Highest certainty"),
        _signal_bar("Cross-Currency",  cross_ccy,   n, "#FBBF24", "Layering signal"),
    ]

    left_col = (
        '<div class="shell-tag">Account Under Investigation</div>'
        f'<div class="shell-title">{account_id}</div>'
        '<div style="display:flex; gap:10px; align-items:center; margin-top:14px;">'
        f'{fraud_badge} {type_chips}'
        f'<span style="font-size:10px; color:#4A6080; font-weight:600;">{percentile_text}</span>'
        '</div>'
        '<div class="metrics-grid-compact" style="margin-top:28px;">'
        f'<div class="m-item"><div class="m-label">Total Amount Paid</div><div class="m-value" style="color:#FFFFFF;">${total_amt:,.0f}</div></div>'
        f'<div class="m-item"><div class="m-label">High-Value Wires</div><div class="m-value" style="color:#F87171;">{high_wire}/{n}</div></div>'
        f'<div class="m-item"><div class="m-label">Cross-Currency</div><div class="m-value" style="color:#FBBF24;">{cross_ccy}/{n}<span style="font-size:10px;font-weight:600;margin-left:8px;color:#4A6080;">layering</span></div></div>'
        f'<div class="m-item"><div class="m-label">Flagged Txns</div><div class="m-value" style="color:#FFFFFF;">{n}</div></div>'
        '</div>'
    )
    
    mid_col = (
        '<div class="shell-tag" style="margin-bottom:12px;">Signal Breakdown</div>'
        f'<div style="display:flex; flex-direction:column; gap:11px;">'
        f'{"".join(signal_bars)}'
        f'</div>'
    )
    
    right_col = (
        '<div style="background:rgba(0,0,0,0.12); border-radius:8px; padding:16px 12px; display:flex; flex-direction:column; align-items:center; height:100%; justify-content:center;">'
        f'{gauge_html}'
        '<div style="font-size:9px; color:#4A6080; text-transform:uppercase; font-weight:800; letter-spacing:0.1em; margin-top:4px;">Anomaly Score</div>'
        f'<div style="font-size:16px; font-weight:900; color:{tier_color}; letter-spacing:0.05em; margin-top:4px;">{tier}</div>'
        '</div>'
    )

    top_html = (
        '<div class="forensic-shell">'
        '<div style="display:grid; grid-template-columns: 1.8fr 1.3fr 0.9fr; gap:30px; margin-bottom:28px;">'
        f'<div>{left_col}</div>'
        f'<div>{mid_col}</div>'
        f'<div>{right_col}</div>'
        '</div>'
    )

    if not txn_df.empty:
        rows_html = ""
        for i, (_, row) in enumerate(txn_df.head(20).iterrows(), 1):
            cc_flag  = bool(row.get("is_cross_currency", 0))
            hwv_flag = bool(row.get("is_high_value_wire", 0))
            fraud    = bool(row.get("is_fraud", 0))

            # --- Currency flow ---
            pay_ccy = row.get("Payment_Currency", "")
            rcv_ccy = row.get("Receiving_Currency", "")
            if cc_flag:
                ccy_cell = (f'<span style="color:#60A5FA;font-size:10px;font-weight:600;">{pay_ccy}</span>'
                            f'<span style="color:#2A4060;font-size:10px;"> → </span>'
                            f'<span style="color:#FBBF24;font-size:10px;font-weight:600;">{rcv_ccy}</span>')
            else:
                ccy_cell = (f'<span style="color:#5A7A9A;font-size:10px;">{pay_ccy} → {rcv_ccy}</span>'
                            if pay_ccy else '<span style="color:#1E3050;font-size:10px;">—</span>')

            # --- Date / time ---
            raw_ts = row.get("transaction_timestamp", "")
            try:
                import pandas as _pd
                dt = _pd.to_datetime(raw_ts)
                date_cell = (
                    f'<span style="color:#7E9BB8;font-size:10px;">{dt.strftime("%Y-%m-%d")}</span>'
                    f'<br><span style="color:#3A5070;font-size:9px;font-family:monospace;">{dt.strftime("%H:%M")}</span>'
                )
            except Exception:
                date_cell = f'<span style="color:#7E9BB8;font-size:10px;">{str(raw_ts)[:10] if raw_ts else "—"}</span>'

            # --- Bank route ---
            from_b = str(row.get("from_bank_name") or row.get("from_bank", "—"))
            to_b   = str(row.get("to_bank_name")   or row.get("to_bank",   "—"))
            route_cell = (
                f'<span style="color:#93C5FD;font-size:10px;">{from_b}</span>'
                f'<span style="color:#2A4060;"> → </span>'
                f'<span style="color:#93C5FD;font-size:10px;">{to_b}</span>'
            )

            # --- Corridor risk % ---
            corr_risk = float(row.get("corridor_risk_pct", 0) or 0)
            if corr_risk >= 5.0:
                risk_cell = f'<span style="color:#F87171;font-weight:700;font-size:11px;">{corr_risk:.1f}%</span><br><span style="color:#4A2020;font-size:8px;">HIGH RISK</span>'
            elif corr_risk >= 2.0:
                risk_cell = f'<span style="color:#FBBF24;font-weight:700;font-size:11px;">{corr_risk:.1f}%</span><br><span style="color:#4A3A10;font-size:8px;">ELEVATED</span>'
            elif corr_risk > 0:
                risk_cell = f'<span style="color:#34D399;font-weight:600;font-size:11px;">{corr_risk:.1f}%</span><br><span style="color:#103A25;font-size:8px;">NORMAL</span>'
            else:
                risk_cell = '<span style="color:#2A3A50;font-size:10px;">N/A</span>'

            # --- Status tags ---
            tags = []
            if fraud:    tags.append('<span style="background:rgba(248,113,113,0.15);color:#F87171;font-size:9px;font-weight:700;padding:2px 6px;border-radius:3px;">SUSPECT</span>')
            if hwv_flag: tags.append('<span style="background:rgba(251,191,36,0.12);color:#FBBF24;font-size:9px;font-weight:700;padding:2px 6px;border-radius:3px;">HI‑WIRE</span>')
            if cc_flag:  tags.append('<span style="background:rgba(96,165,250,0.12);color:#60A5FA;font-size:9px;font-weight:700;padding:2px 6px;border-radius:3px;">LAYER</span>')
            status_cell = " ".join(tags) if tags else '<span style="color:#1E3A28;font-size:9px;font-weight:600;">CLEAR</span>'

            row_color = "rgba(248,113,113,0.05)" if fraud else ("rgba(255,255,255,0.013)" if i % 2 == 0 else "transparent")

            rows_html += (
                f'<tr style="background:{row_color};">'
                f'<td style="color:#2A4060;font-family:monospace;font-size:10px;padding:8px 10px;white-space:nowrap;">[{i:02d}]</td>'
                f'<td style="color:#93C5FD;font-size:11px;font-weight:600;padding:8px 10px;white-space:nowrap;">{row["type"]}</td>'
                f'<td style="color:#FFFFFF;font-family:monospace;font-size:11px;font-weight:700;padding:8px 10px;white-space:nowrap;">${row["amount"]:,.0f}</td>'
                f'<td style="padding:8px 10px;white-space:nowrap;">{ccy_cell}</td>'
                f'<td style="padding:8px 10px;">{date_cell}</td>'
                f'<td style="padding:8px 10px;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{route_cell}</td>'
                f'<td style="padding:8px 10px;text-align:center;">{risk_cell}</td>'
                f'<td style="padding:8px 10px;">{status_cell}</td>'
                f'</tr>'
            )

        headers = ["#", "Format", "Amount Paid", "Currency Flow", "Date / Time", "Bank Route", "Corridor Risk", "Status"]
        table_html = (
            '<div class="shell-tag" style="margin-bottom:10px; padding-top:24px;">Recent Risk-Flagged Transactions</div>'
            '<table style="width:100%;border-collapse:collapse;text-align:left;">'
            '<thead><tr style="border-bottom:1px solid rgba(96,165,250,0.08);">'
            + "".join(f'<th style="padding:8px 10px;color:#2A4060;font-size:9px;text-transform:uppercase;letter-spacing:0.12em;font-weight:700;white-space:nowrap;">{h}</th>' for h in headers)
            + f'</tr></thead><tbody>{rows_html}</tbody></table>'
        )
        full_html = top_html + table_html + '</div>'
    else:
        full_html = top_html + '</div>'

    st.markdown(full_html, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# FORENSIC CHARTS — Temporal + Volume breakdown per account
# ══════════════════════════════════════════════════════════════════════════════

def _render_forensic_charts(account_id: str):
    import plotly.graph_objects as go

    txn_df = _fetch_account_txns(account_id)
    if txn_df.empty:
        return

    txn_df["transaction_timestamp"] = pd.to_datetime(txn_df["transaction_timestamp"], errors="coerce")
    txn_df = txn_df.dropna(subset=["transaction_timestamp"])
    if txn_df.empty:
        return

    txn_df["hour"]     = txn_df["transaction_timestamp"].dt.hour
    txn_df["is_fraud"] = txn_df["is_fraud"].astype(int)

    # ── Shared style constants ────────────────────────────────────────────────
    _BG       = "rgba(0,0,0,0)"
    _FONT     = dict(family="Inter, sans-serif", color="#7E9BB8", size=11)
    _GRID     = "rgba(255,255,255,0.05)"
    _LINE     = "rgba(255,255,255,0.08)"
    _TICK     = dict(color="#4A6080", size=10)
    _TITLE_F  = dict(color="#4A6080", size=10)

    def _base_layout(title: str) -> dict:
        # NO MARGIN OR HEIGHT HERE - they are passed explicitly to avoid collisions
        return dict(
            title=dict(text=title, font=dict(color="#C8D8F0", size=13, family="Inter, sans-serif"), x=0),
            plot_bgcolor=_BG, paper_bgcolor=_BG,
            font=_FONT,
            showlegend=False,
        )

    # ── Chart 1: Hourly Transaction Intensity ─────────────────────────────────
    hourly = (
        txn_df.groupby("hour")
        .agg(count=("amount", "count"), fraud=("is_fraud", "sum"))
        .reset_index()
    )
    bar_colors = ["#F87171" if r["fraud"] > 0 else "#3B6FAD" for _, r in hourly.iterrows()]

    fig1 = go.Figure(go.Bar(
        x=hourly["hour"], y=hourly["count"],
        marker_color=bar_colors, marker_line_width=0,
        hovertemplate="<b>%{x}:00h</b><br>Transactions: %{y}<extra></extra>",
    ))
    fig1.update_layout(
        **_base_layout("Hourly Activity — When Transactions Were Made"),
        margin=dict(l=50, r=30, t=44, b=36),
        height=230
    )
    fig1.update_xaxes(title_text="Hour of Day", dtick=2,
                      gridcolor=_GRID, linecolor=_LINE, tickfont=_TICK, title_font=_TITLE_F)
    fig1.update_yaxes(title_text="Count",
                      gridcolor=_GRID, linecolor=_LINE, tickfont=_TICK, title_font=_TITLE_F)
    fig1.add_vrect(x0=-0.5, x1=5.5,
                   fillcolor="rgba(248,113,113,0.05)", line_width=0,
                   annotation_text="Off-Hours", annotation_position="top left",
                   annotation_font_color="#F87171", annotation_font_size=9)

    # ── Chart 2: Total Amount by Payment Format ───────────────────────────────
    by_fmt = (
        txn_df.groupby("type")["amount"].sum()
        .sort_values(ascending=True)
        .reset_index()
        .rename(columns={"type": "fmt", "amount": "total"})
    )
    by_fmt["label"] = by_fmt["total"].apply(
        lambda v: f"${v/1e6:.1f}M" if v >= 1e6 else f"${v/1e3:.0f}K"
    )

    fig2 = go.Figure(go.Bar(
        x=by_fmt["total"], y=by_fmt["fmt"],
        orientation="h",
        marker=dict(
            color=list(range(len(by_fmt))),
            colorscale=[[0, "#1A3357"], [1, "#60A5FA"]],
            showscale=False, line_width=0,
        ),
        text=by_fmt["label"], textposition="outside",
        textfont=dict(color="#7E9BB8", size=10),
        hovertemplate="<b>%{y}</b><br>Total: $%{x:,.0f}<extra></extra>",
    ))
    fig2.update_layout(
        **_base_layout("Total Amount by Payment Format"),
        margin=dict(l=110, r=70, t=44, b=36),
        height=230
    )
    fig2.update_xaxes(title_text="Total USD", tickformat="$,.0f",
                      gridcolor=_GRID, linecolor=_LINE, tickfont=_TICK, title_font=_TITLE_F)
    fig2.update_yaxes(title_text="",
                      gridcolor=_GRID, linecolor=_LINE,
                      tickfont=dict(color="#C8D8F0", size=10), title_font=_TITLE_F)

    # ── Render using st.components.v1.html (preserves Plotly JS) ───────────────
    import streamlit.components.v1 as components

    html_fig1 = fig1.to_html(full_html=False, include_plotlyjs='cdn', config={'displayModeBar': False})
    html_fig2 = fig2.to_html(full_html=False, include_plotlyjs='cdn', config={'displayModeBar': False})

    full_vis_html = f"""
    <html><head>
    <style>
      body {{ margin:0; padding:0; background: #111D32; font-family: Inter, sans-serif; overflow: hidden; }}
      .shell-tag {{ font-size:10px; color:#4A6080; text-transform:uppercase; letter-spacing:0.16em; font-weight:800; padding: 20px 24px 0 24px; }}
      .chart-row {{ display:flex; gap:15px; padding: 12px 24px 24px 24px; box-sizing: border-box; width: 100%; }}
      .chart-col {{ flex:1; width: 50%; max-width: 50%; min-width: 0; overflow: hidden; }}
      .plotly-graph-div {{ width: 100% !important; max-width: 100% !important; margin: 0 auto; }}
    </style>
    </head><body>
      <div class="shell-tag">Account-Level Forensic Visuals</div>
      <div class="chart-row">
        <div class="chart-col">{html_fig1}</div>
        <div class="chart-col">{html_fig2}</div>
      </div>
    </body></html>
    """
    components.html(full_vis_html, height=310, scrolling=False)




# ══════════════════════════════════════════════════════════════════════════════
# EVIDENCE CHAIN (collapsible)
# ══════════════════════════════════════════════════════════════════════════════

def _render_evidence_chain(account_id: str, agent1_results: list, all_scores: list):
    recs = [r for r in agent1_results if r.get("account_id") == account_id]
    if not recs:
        return

    max_score  = max(r.get("anomaly_score", 0) for r in recs)
    n          = len(recs)
    ml_flags   = sum(r.get("ml_flag", 0) for r in recs)
    rule_flags = sum(r.get("rule_flag", 0) for r in recs)
    both_flags = sum(1 for r in recs if r.get("ml_flag",0) and r.get("rule_flag",0))
    cross_ccy  = sum(r.get("is_cross_currency", 0) for r in recs)
    high_wire  = sum(r.get("is_high_value_wire", 0) for r in recs)
    pct_rank   = int(np.searchsorted(sorted(all_scores), max_score) / max(len(all_scores),1) * 100)

    facts = []
    if both_flags > 0:
        facts.append(("Strong signal", "Two independent systems both raised the alarm",
            f"This account was flagged by <strong>both</strong> our AI model and our rule-based checks — "
            f"on {both_flags} out of {n} transaction(s). Think of it like two separate smoke detectors going off "
            f"at the same time: much harder to ignore than just one. When two completely different methods agree, "
            f"we have much higher confidence something is genuinely wrong.",
            "#F87171"))
    if cross_ccy > 0:
        facts.append(("Strong signal", "Currency was converted mid-transfer — a layering signal",
            f"On {cross_ccy} out of {n} transaction(s), the currency paid was <strong>different</strong> from "
            f"the currency received. This is a core layering technique in money laundering: converting funds "
            f"between currencies mid-flow creates additional complexity for investigators and helps obscure "
            f"the money trail. In legitimate business transfers, the same currency is typically used end-to-end.",
            "#FBBF24"))
    if high_wire > 0:
        facts.append(("Moderate signal", "High-value wire transfers detected",
            f"{high_wire} out of {n} transaction(s) involved wire transfers exceeding <strong>100,000 USD</strong>. "
            f"Wire transfers at this scale move large amounts quickly between banks and jurisdictions. "
            f"The IBM AML dataset flags these specifically because they represent the kind of large, fast "
            f"capital movement used in the integration phase of money laundering — converting illicit funds "
            f"into what appears to be legitimate large-scale business activity.",
            "#A78BFA"))
    if ml_flags > 0:
        facts.append(("AI detection", "The AI model flagged this as highly unusual",
            f"Our AI scanned over 5 million IBM AML transactions and ranked this account in the "
            f"<strong>top {100-pct_rank}%</strong> most unusual (score: {max_score:.0f}/100). "
            f"The model learned what normal payment patterns look like — and this account's combination "
            f"of amount, payment format, cross-currency activity, and high-value wires stood out significantly. "
            f"It doesn't mean laundering is confirmed, but it does mean this account behaves very differently "
            f"from the vast majority of accounts in the dataset.",
            "#60A5FA"))
    if rule_flags > 0:
        facts.append(("Rule-based check", "Crossed multiple AML threshold checks",
            f"{rule_flags} of {n} transaction(s) tripped our hard-coded AML rules — "
            f"specifically: wire transfers exceeding $100K, or cross-currency transfers above the average "
            f"transaction amount. These rules don't use AI — they're straightforward checks based on "
            f"known money laundering red flags in the IBM AML research dataset.",
            "#34D399"))
    if not facts:
        return

    # Build all fact cards as one HTML block
    cards_html = ""
    for severity, title, description, color in facts:
        cards_html += (
            f'<div style="display:flex;gap:12px;padding:14px 0;border-bottom:1px solid rgba(96,165,250,0.06);">'
            f'<div style="flex-shrink:0;width:3px;border-radius:2px;background:{color};margin:2px 0;"></div>'
            f'<div>'
            f'<div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">'
            f'<span style="font-size:9px;font-weight:800;color:{color};text-transform:uppercase;letter-spacing:0.12em;background:rgba(255,255,255,0.05);padding:2px 7px;border-radius:3px;">{severity}</span>'
            f'<span style="font-size:13px;font-weight:700;color:#FFFFFF;">{title}</span>'
            f'</div>'
            f'<div style="font-size:12.5px;color:#A0B8D0;line-height:1.75;">{description}</div>'
            f'</div></div>'
        )

    with st.expander("Why was this account flagged?", expanded=True):
        st.markdown(
            f'<div style="background:#0F1E35;border-radius:4px;padding:4px 0;">{cards_html}</div>',
            unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# CHAT TURN RENDERER — premium chatbot style
# ══════════════════════════════════════════════════════════════════════════════

def _render_chat_turn(turn: dict):
    ts   = turn.get("timestamp", "")
    acct = turn.get("account", "")
    meta = f"{acct} · {ts}" if acct and acct != "— choose an account to begin —" else ts

    # User bubble — right-aligned with avatar
    st.markdown(
        f'<div style="display:flex;justify-content:flex-end;gap:10px;margin:16px 0 8px;">'
        f'<div style="background:linear-gradient(135deg,rgba(59,130,246,0.15),rgba(59,130,246,0.08));'
        f'border:1px solid rgba(59,130,246,0.18);'
        f'border-radius:16px 16px 4px 16px;padding:12px 16px;max-width:72%;'
        f'font-size:13px;color:#E0E8F0;line-height:1.65;">'
        f'{html_lib.escape(turn["question"])}'
        f'<div style="font-size:9px;color:#5A7A9A;margin-top:6px;text-align:right;">{meta}</div>'
        f'</div>'
        f'<div style="flex-shrink:0;width:30px;height:30px;border-radius:50%;'
        f'background:linear-gradient(135deg,#3B82F6,#1D4ED8);display:flex;align-items:center;'
        f'justify-content:center;font-size:12px;color:#fff;font-weight:700;margin-top:2px;">Y</div>'
        f'</div>',
        unsafe_allow_html=True)

    # AI Investigator bubble — left-aligned with avatar
    st.markdown(
        '<div style="display:flex;gap:10px;margin:8px 0 4px;">'
        '<div style="flex-shrink:0;width:30px;height:30px;border-radius:50%;'
        'background:linear-gradient(135deg,#60A5FA,#3B82F6);display:flex;align-items:center;'
        'justify-content:center;font-size:11px;color:#fff;font-weight:800;margin-top:2px;">AI</div>'
        '<div style="font-size:9px;font-weight:700;color:#60A5FA;letter-spacing:0.12em;'
        'text-transform:uppercase;padding-top:8px;">Investigator</div>'
        '</div>',
        unsafe_allow_html=True)

    # Answer — native st.markdown for proper bold/list/table rendering. Allow HTML for rich cards.
    st.markdown(turn["answer"], unsafe_allow_html=True)
    st.markdown('<div style="height:1px;background:linear-gradient(90deg,rgba(96,165,250,0.12),transparent);margin:12px 0;"></div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# SUGGESTION CHIPS — small, clean, natural case
# ══════════════════════════════════════════════════════════════════════════════

def _render_suggestions(account_id: str, agent1_results: list):
    recs = [r for r in agent1_results if r.get("account_id") == account_id]
    
    if not recs:
        suggestions = [
            ("Detection Summary", "What makes this account suspicious and what stands out most?"),
            ("Typology Match", "Which financial crime pattern does this behavior most closely resemble?"),
            ("Peer Analysis", "How does this account's risk level compare to others in the queue?"),
            ("Market Influence", "Could these transactions be linked to broader market movements or stress?"),
        ]
    else:
        cross_ccy  = sum(r.get("is_cross_currency", 0) for r in recs)
        high_wire  = sum(r.get("is_high_value_wire", 0) for r in recs)
        
        suggestions = [
            ("Forensic Walkthrough", f"Explain the transaction behavior for {account_id} and why it's unusual."),
        ]
        if cross_ccy > 0:
            suggestions.append(("Layering Analysis", "Explain the suspicious cross-currency activity and layering signal."))
        if high_wire > 0:
            suggestions.append(("Integration Phase", "What is the significance of these high-value wire transfers?"))
        
        suggestions.append(("Temporal Patterns", "Do the transaction hours match known off-hours laundering windows?"))
        suggestions.append(("Strategic Ranking", "Where does this account sit in the global priority list and why?"))

    st.markdown("""
    <style>
    .suggestion-card-container {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 12px;
        margin-top: 10px;
    }
    .stButton > button {
        height: auto !important;
        padding: 12px 16px !important;
        background: rgba(255, 255, 255, 0.03) !important;
        border: 1px solid rgba(96, 165, 250, 0.1) !important;
        border-radius: 8px !important;
        text-align: left !important;
        transition: all 0.2s ease !important;
    }
    .stButton > button:hover {
        background: rgba(96, 165, 250, 0.08) !important;
        border-color: rgba(96, 165, 250, 0.3) !important;
        transform: translateY(-1px);
    }
    .sugg-title { font-size: 13px; font-weight: 700; color: #93C5FD; margin-bottom: 4px; display: block; }
    .sugg-desc { font-size: 11px; color: #7E9BB8; line-height: 1.4; display: block; filter: none !important; }
    </style>
    """, unsafe_allow_html=True)

    st.markdown('<div style="font-size:11px;color:#4A6080;margin:12px 0 12px;font-weight:700;text-transform:uppercase;letter-spacing:0.1em;">Investigative Leads</div>', unsafe_allow_html=True)
    
    cols = st.columns(2)
    for i, (title, desc) in enumerate(suggestions[:6]):
        with cols[i % 2]:
            if st.button(title, key=f"sugg_new_{i}", help=desc, use_container_width=True):
                question_map = {
                    "Detection Summary": "Summarise the key signals that caused this account to be flagged. What stands out most in the data?",
                    "Typology Match": "Looking at the transaction behaviour, which known financial crime typology does this most closely match — and why?",
                    "Peer Analysis": "Is this account more or less concerning than the other accounts in our investigation queue? What puts it above or below them?",
                    "Market Influence": "Could the timing or scale of this account's transactions be linked to market conditions or sector stress?",
                    "Forensic Walkthrough": f"Explain the transaction behaviour for account {account_id}. What payment formats were used, which banks were involved, and what makes the pattern unusual?",
                    "Layering Analysis": f"Account {account_id} has suspicious cross-currency transaction(s). Explain the layering typology — what does this tell us about the funds movement?",
                    "Integration Phase": f"Account {account_id} has wire transfer(s) exceeding 100,000 USD. What AML typology and laundering stage does this represent?",
                    "Temporal Patterns": f"Do the transaction hours for {account_id} match known structuring windows in the IBM AML dataset?",
                    "Strategic Ranking": f"Where does account {account_id} rank against the top 25 accounts by exposure? Is this outlier or systemic?"
                }
                st.session_state["_prefill_question"] = question_map.get(title, title)
                st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def run_investigate(agent1_results: list):
    try:
        client = _get_client()
    except Exception as e:
        st.error(f"Groq initialisation failed: {e}")
        return

    # Global metrics
    high_rows  = [r for r in agent1_results if r.get("confidence_tier") == "HIGH"]
    med_rows   = [r for r in agent1_results if r.get("confidence_tier") == "MEDIUM"]
    fraud_rows = [r for r in agent1_results if r.get("is_fraud") == 1]
    all_scores = [r.get("anomaly_score", 0) for r in agent1_results]

    top5 = sorted(agent1_results, key=lambda r: r.get("anomaly_score", 0), reverse=True)[:5]
    global_summary = (
        f"Total flagged: {len(high_rows)} HIGH + {len(med_rows)} MEDIUM"
        f" ({len(fraud_rows)} confirmed fraud labels)\n"
        f"Score range: {min(all_scores):.0f} – {max(all_scores):.0f}\n"
        f"Top 5: " + " | ".join(
            f"{r['account_id']} score={r['anomaly_score']:.0f} {r['confidence_tier']}"
            for r in top5)
    )

    # ── Account list: investigation_queue (Gold) + Agent 1 scores overlaid ──────
    # investigation_queue is the authoritative source — every confirmed fraud and
    # every multi-discrepancy account, regardless of Agent 1's sample.
    # Agent 1's anomaly_score overlays on top where available (higher = more urgent).
    acct_scores: dict = {}

    # Step 1: seed from investigation_queue — all 354 priority accounts
    queue_df = _fetch_investigation_queue()
    if not queue_df.empty:
        for _, row in queue_df.iterrows():
            acct_scores[row["account_id"]] = float(row["priority_score"])

    # Step 2: overlay Agent 1 scores — if Agent 1 scored an account, use that
    # (Agent 1 scores are 0-100 from IF; priority_score is also 0-100)
    for r in agent1_results:
        aid = r.get("account_id", "")
        if aid:
            agent1_score = r.get("anomaly_score", 0)
            # Take the higher of Agent 1 score and Gold priority score
            acct_scores[aid] = max(acct_scores.get(aid, 0), agent1_score)

    flagged_accounts = sorted(acct_scores, key=lambda a: acct_scores[a], reverse=True)

    # Label map: show reason tag for gold-queued accounts
    queue_lookup = {}
    if not queue_df.empty:
        for _, row in queue_df.iterrows():
            tags = []
            if row["is_fraud"] == 1:
                tags.append("LAUNDERING")
            if row["flag_multi_risk"] == 1:
                tags.append(f"{int(row['risk_indicator_count'])}x indicators")
            queue_lookup[row["account_id"]] = " · ".join(tags) if tags else ""

    # ── HEADER ROW ────────────────────────────────────────────────────────────
    sel_col, k1_col, k2_col, k3_col = st.columns([3, 1, 1, 1])

    with sel_col:
        st.markdown(
            '<div style="font-size:11px;color:#7E9BB8;margin-bottom:6px;font-weight:600;">'
            'Choose a suspicious account to investigate — most concerning ones are at the top:</div>',
            unsafe_allow_html=True)
        options   = ["— choose an account to begin —"] + flagged_accounts
        label_map = {"— choose an account to begin —": "— choose an account to begin —"}
        for a in flagged_accounts:
            tag = queue_lookup.get(a, "")
            score_part = f"score {acct_scores[a]:.0f}"
            label_map[a] = f"{a}  ·  {score_part}  {('· ' + tag) if tag else ''}".strip()
        prev = st.session_state.get("_selected_account", "— choose an account to begin —")
        selected_account = st.selectbox(
            "account", options=options,
            format_func=lambda x: label_map.get(x, x),
            label_visibility="collapsed",
        )
        if selected_account != prev:
            st.session_state["_selected_account"] = selected_account
            st.session_state.chat_history = []
            st.session_state.agent2_context = None
        # Always expose selected account for Agent 3 to read
        st.session_state["investigate_account"] = selected_account
        st.session_state["investigate_history"]  = st.session_state.chat_history

    with k1_col:
        st.markdown(f"""
        <div class="kpi-card accent-red" style="padding:12px 16px;">
          <div class="kpi-label">Serious Concern</div>
          <div class="kpi-value" style="font-size:20px;">{len(high_rows)}</div>
          <div class="kpi-delta">Dual flagged</div>
        </div>""", unsafe_allow_html=True)

    with k2_col:
        st.markdown(f"""
        <div class="kpi-card accent-amber" style="padding:12px 16px;">
          <div class="kpi-label">Review Queue</div>
          <div class="kpi-value" style="font-size:20px;">{len(med_rows)}</div>
          <div class="kpi-delta">Investigation urged</div>
        </div>""", unsafe_allow_html=True)

    with k3_col:
        st.markdown(f"""
        <div class="kpi-card" style="padding:12px 16px;">
          <div class="kpi-label">AI Logic</div>
          <div class="kpi-value" style="font-size:14px;line-height:1.2;font-weight:800;color:#93C5FD;">LLAMA 3.3</div>
          <div class="kpi-delta">Real-time Forensics</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    # ── EVIDENCE CARD + CHAIN ─────────────────────────────────────────────────
    if selected_account != "— choose an account to begin —":
        st.markdown(
            '<div style="font-size:9px;letter-spacing:0.14em;text-transform:uppercase;'
            'color:#4A6080;margin-bottom:10px;font-weight:700;">Account Summary</div>',
            unsafe_allow_html=True)
        _render_evidence_card(selected_account, agent1_results, all_scores)
        _render_forensic_charts(selected_account)
        _render_evidence_chain(selected_account, agent1_results, all_scores)
        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    # ── INVESTIGATION CHAT ────────────────────────────────────────────────────
    # Chat container card — lighter, centered, premium chatbot feel
    st.markdown("""
    <style>
    .chat-container {
        background: linear-gradient(180deg, #15253E 0%, #0F1E35 100%);
        border: 1px solid rgba(96,165,250,0.12);
        border-radius: 12px;
        padding: 20px 24px 8px;
        margin: 8px auto 0;
        max-width: 100%;
        box-shadow: 0 8px 32px rgba(0,0,0,0.3);
    }
    .chat-header {
        display: flex; align-items: center; gap: 10px;
        padding-bottom: 14px;
        border-bottom: 1px solid rgba(96,165,250,0.08);
        margin-bottom: 14px;
    }
    .chat-header-icon {
        width: 32px; height: 32px; border-radius: 50%;
        background: linear-gradient(135deg, #3B82F6, #2563EB);
        display: flex; align-items: center; justify-content: center;
        font-size: 13px; color: #fff; font-weight: 800;
    }
    .chat-header-text { font-size: 14px; font-weight: 700; color: #E0E8F0; }
    .chat-header-sub { font-size: 10px; color: #5A7A9A; }
    </style>
    """, unsafe_allow_html=True)

    n_turns = len(st.session_state.chat_history)
    header_sub = f"{n_turns} conversation{'s' if n_turns != 1 else ''} · last 6 in memory" if n_turns else "Ready to investigate"

    st.markdown(f"""
    <div class="chat-container">
      <div class="chat-header">
        <div class="chat-header-icon">AI</div>
        <div>
          <div class="chat-header-text">AI Investigator</div>
          <div class="chat-header-sub">{header_sub}</div>
        </div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    if st.session_state.chat_history:
        for turn in st.session_state.chat_history:
            _render_chat_turn(turn)

        with st.expander("💡 More questions to try", expanded=False):
            _render_suggestions(selected_account, agent1_results)

        # Clear chat simple text link on right
        st.markdown("""
        <style>
        button[title="Clear chat"] {
            background: transparent !important;
            border: none !important;
            box-shadow: none !important;
            color: #FFFFFF !important;
            font-size: 12px !important;
            text-decoration: underline !important;
            padding: 0 !important;
            float: right;
            margin-top: 10px;
            margin-right: 5px;
            min-height: 0 !important;
            height: auto !important;
            line-height: normal !important;
        }
        button[title="Clear chat"] > div, button[title="Clear chat"] p {
            padding: 0 !important; margin: 0 !important; font-size: 12px !important; color: #FFFFFF !important;
        }
        button[title="Clear chat"]:hover, button[title="Clear chat"]:hover p {
            color: #A0B8D0 !important;
            background: transparent !important;
        }
        </style>
        """, unsafe_allow_html=True)
        
        c1, c2 = st.columns([5, 1])
        with c2:
            if st.button("Clear chat", help="Clear chat"):
                st.session_state.chat_history   = []
                st.session_state.agent2_context = None
                st.rerun()
    else:
        if selected_account != "— choose an account to begin —":
            _render_suggestions(selected_account, agent1_results)
        else:
            # ── Onboarding guide ──────────────────────────────────────────────
            st.markdown("""
<div style="background:#15253E;border:1px solid rgba(96,165,250,0.12);border-radius:10px;padding:24px 28px;margin-top:4px;">
  <div style="font-size:15px;font-weight:700;color:#FFFFFF;margin-bottom:6px;">Account Investigation</div>
  <div style="font-size:13px;color:#A0B8D0;line-height:1.8;margin-bottom:20px;">
    Select a flagged account from the dropdown above to begin. The AI will analyse the transaction data
    and answer your questions about what happened, why it was flagged, and what action to take.
  </div>

  <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:24px;">
    <div style="background:rgba(96,165,250,0.06);border:1px solid rgba(96,165,250,0.12);border-radius:6px;padding:14px 16px;">
      <div style="font-size:11px;font-weight:700;color:#60A5FA;margin-bottom:6px;">Step 1 — Select an account</div>
      <div style="font-size:12px;color:#A0B8D0;line-height:1.6;">Accounts are ranked by suspicion score — the highest risk appears first.</div>
    </div>
    <div style="background:rgba(96,165,250,0.06);border:1px solid rgba(96,165,250,0.12);border-radius:6px;padding:14px 16px;">
      <div style="font-size:11px;font-weight:700;color:#60A5FA;margin-bottom:6px;">Step 2 — Review evidence</div>
      <div style="font-size:12px;color:#A0B8D0;line-height:1.6;">Charts and tables show exactly what triggered the flags.</div>
    </div>
    <div style="background:rgba(96,165,250,0.06);border:1px solid rgba(96,165,250,0.12);border-radius:6px;padding:14px 16px;">
      <div style="font-size:11px;font-weight:700;color:#60A5FA;margin-bottom:6px;">Step 3 — Ask the AI</div>
      <div style="font-size:12px;color:#A0B8D0;line-height:1.6;">Use the chat box or click a suggested question below.</div>
    </div>
    <div style="background:rgba(96,165,250,0.06);border:1px solid rgba(96,165,250,0.12);border-radius:6px;padding:14px 16px;">
      <div style="font-size:11px;font-weight:700;color:#60A5FA;margin-bottom:6px;">Step 4 — Get a recommendation</div>
      <div style="font-size:12px;color:#A0B8D0;line-height:1.6;">Ask whether to freeze, file a SAR, escalate, or clear.</div>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

    # Prefill handling
    prefill = st.session_state.get("_prefill_question")
    if prefill:
        del st.session_state["_prefill_question"]

    # Global CSS injection for Chat Input Box
    st.markdown("""
    <style>
    /* Make the chat input container smaller and centered */
    [data-testid="stChatInput"] {
        max-width: 680px !important;
        margin: 0 auto !important;
        padding-bottom: 24px !important;
        z-index: 99999 !important;
    }
    /* Style the outer input container to be cream/light white */
    [data-testid="stChatInput"] > div {
        background-color: #F8F9FB !important;
        border: 1px solid #E2E8F0 !important;
        border-radius: 24px !important;
        box-shadow: 0 4px 20px rgba(0,0,0,0.15) !important;
        padding: 4px 8px !important;
    }
    /* Safely inject the background color into the deep streamlit react wrappers */
    [data-testid="stChatInput"] div[data-baseweb="textarea"],
    [data-testid="stChatInput"] div[data-baseweb="base-input"] {
        background-color: #F8F9FB !important;
        border: none !important;
    }
    [data-testid="stChatInput"] textarea {
        background-color: #F8F9FB !important;
        color: #0F1E35 !important;
        caret-color: #0F1E35 !important;
        font-weight: 600 !important;
        pointer-events: auto !important;
    }
    /* Placeholder in a visible dark blueish black */
    [data-testid="stChatInput"] textarea::placeholder {
        color: #4A6080 !important;
        opacity: 0.8 !important;
    }
    /* Fix buttons inside */
    [data-testid="stChatInput"] button {
        background-color: transparent !important;
        pointer-events: auto !important;
    }
    [data-testid="stChatInput"] button:hover {
        background-color: rgba(15, 30, 53, 0.05) !important;
    }
    /* Send icon properly matches the dark blueish theme */
    [data-testid="stChatInput"] svg {
        fill: #0F1E35 !important;
        color: #0F1E35 !important;
    }
    </style>
    """, unsafe_allow_html=True)

    # Chat input
    user_question = st.chat_input(
        "Ask about this account, fraud patterns, or regulatory recommendations..."
    ) or prefill

    if user_question:
        if selected_account == "— choose an account to begin —":
            st.warning("Select an account first.")
            return

        # Show user bubble immediately
        st.markdown(
            f'<div style="display:flex;justify-content:flex-end;gap:10px;margin:16px 0 8px;">'
            f'<div style="background:linear-gradient(135deg,rgba(59,130,246,0.15),rgba(59,130,246,0.08));'
            f'border:1px solid rgba(59,130,246,0.18);'
            f'border-radius:16px 16px 4px 16px;padding:12px 16px;max-width:72%;'
            f'font-size:13px;color:#E0E8F0;line-height:1.65;">'
            f'{html_lib.escape(user_question)}'
            f'<div style="font-size:9px;color:#5A7A9A;margin-top:6px;text-align:right;">'
            f'{datetime.now().strftime("%H:%M:%S")}</div>'
            f'</div>'
            f'<div style="flex-shrink:0;width:30px;height:30px;border-radius:50%;'
            f'background:linear-gradient(135deg,#3B82F6,#1D4ED8);display:flex;align-items:center;'
            f'justify-content:center;font-size:12px;color:#fff;font-weight:700;margin-top:2px;">Y</div>'
            f'</div>',
            unsafe_allow_html=True)

        # Retrieve
        with st.spinner("Querying Gold layer…"):
            context = route_context(user_question, selected_account, agent1_results, all_scores)

        # Build messages
        full_global = global_summary + f"\nCurrently investigating: {selected_account}"
        messages = _build_messages(
            question=user_question,
            context=context,
            history=st.session_state.chat_history,
            global_summary=full_global,
        )

        # Stream
        try:
            # AI avatar + label
            st.markdown(
                '<div style="display:flex;gap:10px;margin:8px 0 4px;">'
                '<div style="flex-shrink:0;width:30px;height:30px;border-radius:50%;'
                'background:linear-gradient(135deg,#60A5FA,#3B82F6);display:flex;align-items:center;'
                'justify-content:center;font-size:11px;color:#fff;font-weight:800;margin-top:2px;">AI</div>'
                '<div style="font-size:9px;font-weight:700;color:#60A5FA;letter-spacing:0.12em;'
                'text-transform:uppercase;padding-top:8px;">Investigator</div>'
                '</div>',
                unsafe_allow_html=True)

            stream_box    = st.empty()
            full_response = ""

            stream = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=messages,
                temperature=0.15,
                max_tokens=1200,
                stream=True,
            )
            for chunk in stream:
                delta = chunk.choices[0].delta.content or ""
                if delta:
                    full_response += delta
                    stream_box.markdown(full_response, unsafe_allow_html=True)

            # Save turn
            st.session_state.chat_history.append({
                "question":  user_question,
                "answer":    full_response,
                "account":   selected_account,
                "timestamp": datetime.now().strftime("%H:%M:%S"),
            })
            st.session_state.agent2_context = {
                "selected_account": selected_account,
                "last_question":    user_question,
                "last_answer":      full_response,
                "full_history":     st.session_state.chat_history,
                "flagged_count":    len(agent1_results),
                "agent1_summary":   global_summary,
            }
            st.rerun()

        except Exception as e:
            st.error(f"Groq error: {e}")
