"""Dashboard pages. Each ``page_*`` function renders one page; app.py wires navigation.

Kept separate from app.py so every page can be rendered headlessly in tests
(``streamlit.testing.v1.AppTest``).
"""

from __future__ import annotations

import json

import pandas as pd
import streamlit as st

from cachemind.dashboard import data as D


def badge(kind: str) -> None:
    label, help_text = D.PROVENANCE[kind]
    color = {
        "live": "green",
        "saved": "blue",
        "synthetic": "violet",
        "offline": "orange",
        "stale": "gray",
    }[kind]
    st.markdown(f":{color}-badge[{label}]  <small>{help_text}</small>", unsafe_allow_html=True)


# --- 1 overview ----------------------------------------------------------------------


def page_overview() -> None:
    st.title("CacheMind")
    st.caption("Learning-augmented cache replacement with ML and real-time Big Data analytics")
    st.markdown("""
A cache holds a small subset of items. When it is full and a new item arrives, a
**replacement policy** decides what to evict. Classical policies use simple rules
(FIFO, LRU, LFU). CacheMind learns, from the request history only, the probability
that an item is requested again within the next *H* requests, and evicts the item
least likely to be reused (**ML-guided**), or blends that with recency (**hybrid**).
Belady's offline optimum (which sees the future) is shown as an upper bound.
""")
    st.graphviz_chart("""
digraph G { rankdir=LR; node [shape=box, style="rounded,filled", fillcolor="#eef3fb",
  fontname="Helvetica", fontsize=11]; edge [fontsize=9];
  csv [label="Historical CSV\\n(legacy logs)"]; json [label="Historical JSON\\n(API logs)"];
  meta [label="Item / user\\nmetadata"]; gen [label="Real-time\\ngenerator"];
  kafka [label="Apache Kafka\\ntopic", fillcolor="#fde9d9"];
  sss [label="Spark Structured\\nStreaming", fillcolor="#fff2cc"];
  batch [label="Spark batch ETL", fillcolor="#fff2cc"];
  bronze [label="Bronze\\n(raw + lineage)"]; silver [label="Silver\\n(canonical)"];
  gold [label="Gold\\n(aggregates)"]; feats [label="ML features\\n(Spark windows)"];
  mllib [label="Spark MLlib\\nLR / RF", fillcolor="#fff2cc"];
  hbase [label="Apache HBase\\n(operational)", fillcolor="#e2f0d9"];
  parquet [label="Parquet\\n(analytical)", fillcolor="#e2f0d9"];
  sim [label="Cache experiment\\nengine"]; ui [label="Streamlit UI"];
  gen -> kafka -> sss -> bronze; csv -> batch; json -> batch; batch -> bronze;
  bronze -> silver; meta -> silver; silver -> gold; silver -> feats -> mllib;
  gold -> hbase; gold -> parquet; sss -> hbase; mllib -> sim; silver -> sim;
  sim -> parquet; sim -> hbase; parquet -> ui; hbase -> ui; }
""")
    c1, c2, c3 = st.columns(3)
    c1.metric("Saved experiments", len(D.list_experiments()))
    c2.metric("Model artifacts", len(D.list_models()))
    c3.metric("Silver datasets", len(D.list_datasets()))
    st.info(
        "Profiles: **A** lightweight (simulator + scikit-learn, no JVM), **B** component "
        "integration (Spark / Kafka / HBase one at a time), **C** full pipeline. "
        "All data is synthetic; see docs/ARCHITECTURE.md."
    )


# --- 2 live monitoring --------------------------------------------------------------


def page_live() -> None:
    st.title("Live request monitoring")
    snap = D.streaming_snapshot()
    if snap.provenance == "none":
        st.warning(
            "No streaming output exists yet. Start Kafka + HBase (`make infra-up`), the "
            "streaming job (`make streaming`) and the producer (`make kafka-producer`)."
        )
        return
    badge(snap.provenance)
    if snap.provenance == "stale":
        st.caption(
            f"Streaming heartbeat age: {snap.heartbeat_age_s:.0f} s"
            if snap.heartbeat_age_s is not None
            else "Streaming job has stopped."
        )
    c1, c2, c3 = st.columns(3)
    c1.metric("Mode", snap.status.get("mode", "?"))
    c2.metric("Query restarts", sum(snap.status.get("restarts", {}).values()))
    c3.metric("Progress records", len(snap.progress))
    if not snap.progress.empty:
        p = snap.progress.dropna(subset=["batch_id"])
        st.subheader("Micro-batch throughput (rows/s)")
        st.line_chart(p.pivot_table(index="ts", columns="name", values="processed_rows_per_s"))
        st.subheader("Batch duration (ms)")
        st.line_chart(p.pivot_table(index="ts", columns="name", values="batch_duration_ms"))
        wm = p[p["name"].astype(str).str.startswith("gold_windows")][
            ["ts", "watermark", "dropped_by_watermark"]
        ]
        if not wm.empty:
            st.subheader("Event-time watermark (gold_windows)")
            st.dataframe(wm.tail(10), hide_index=True)
    st.subheader("Most recent micro-batch (provisional Silver)")
    if snap.recent_events.empty:
        st.write("No events in the latest micro-batch.")
    else:
        st.dataframe(snap.recent_events, hide_index=True)


# --- 3 simulator ------------------------------------------------------------------------


@st.cache_resource(max_entries=4, show_spinner="Generating trace and training model...")
def _sim_workbench(workload: str, n_events: int, seed: int, horizon: int):
    from cachemind.experiments.common import Workbench, add_models, make_trace
    from cachemind.ml.dataset import build_reuse_dataset

    trace = make_trace(workload, seed, n_events, max(200, n_events // 20))
    wb = Workbench(trace=trace, dataset=build_reuse_dataset(trace, horizon))
    import tempfile
    from pathlib import Path

    add_models(
        wb,
        Path(tempfile.mkdtemp(prefix="cm-dash-")),
        seed,
        ("logistic_regression",),
        max_train_rows=60_000,
    )
    return wb


def page_simulator() -> None:
    from cachemind.data.workloads import WORKLOAD_DESCRIPTIONS, WORKLOADS
    from cachemind.experiments.common import make_policy
    from cachemind.simulation.engine import simulate

    st.title("Cache simulator")
    badge("synthetic")
    with st.form("sim"):
        c1, c2, c3 = st.columns(3)
        workload = c1.selectbox(
            "Workload",
            WORKLOADS,
            index=1,
            format_func=lambda w: f"{w} - {WORKLOAD_DESCRIPTIONS[w]}",
        )
        policy = c2.selectbox("Policy", ["lru", "fifo", "lfu", "ml", "hybrid", "belady"])
        capacity = c3.number_input("Capacity (items)", 1, D.MAX_SIM_CAPACITY, 100)
        c4, c5, c6 = st.columns(3)
        n_events = c4.number_input("Trace length", 1_000, D.MAX_SIM_EVENTS, 30_000, step=1_000)
        seed = c5.number_input("Seed", 0, 10_000, 1)
        alpha = c6.slider("Hybrid alpha (ML weight)", 0.0, 1.0, 0.5, 0.05)
        go = st.form_submit_button("Run simulation", type="primary")
    if not go:
        st.caption(
            "ML/hybrid use a logistic-regression model trained on the first 60% of "
            "this trace (H = 5 x capacity); results are measured on the last 20%."
        )
        return
    try:
        D.validate_sim_request(int(n_events), int(capacity), policy)
    except D.InvalidInput as exc:
        st.error(str(exc))
        return
    horizon = 5 * int(capacity)
    try:
        wb = _sim_workbench(workload, int(n_events), int(seed), horizon)
    except ValueError as exc:
        st.error(f"Cannot build a train/validation/test split: {exc}")
        return
    pol = make_policy(policy, wb, "logistic_regression", horizon, alpha=alpha)
    res = simulate(
        wb.trace.keys,
        pol,
        int(capacity),
        measure_from=wb.bounds.test_start,
        timeseries_window=max(200, int(n_events) // 100),
        item_ids=wb.trace.item_ids,
        decision_log_limit=25,
    )
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Hit rate", f"{res.hit_rate:.2%}")
    c2.metric("Hits / misses", f"{res.hits:,} / {res.misses:,}")
    c3.metric("Evictions", f"{res.evictions_measured:,}")
    c4.metric("Wall time", f"{res.wall_time_s * 1e3:.0f} ms")
    if res.timeseries:
        st.line_chart(pd.DataFrame(res.timeseries).set_index("t_end"))
    st.subheader("Cache contents at the end (sample)")
    st.write(", ".join(res.final_cache_sample[:100]))
    if res.decision_log:
        st.subheader("Eviction decisions (first 25)")
        st.dataframe(pd.DataFrame(res.decision_log), hide_index=True)
    if res.policy_stats:
        st.json({k: v for k, v in res.policy_stats.items() if k != "inference_latency_us"})


# --- 4 algorithm comparison ----------------------------------------------------------------


def page_comparison() -> None:
    st.title("Algorithm comparison")
    df, meta = D.load_experiment("exp02_ml_vs_classical")
    if df.empty:
        st.warning("No saved results. Run `make experiments`.")
        return
    badge("saved")
    st.caption(meta.get("protocol", ""))
    t = D.hit_rate_table(df)
    st.subheader("Mean test-block hit rate (3 seeds)")
    st.dataframe(
        t.style.format("{:.4f}").highlight_max(
            axis=1, subset=[c for c in t.columns if c != "belady"]
        ),
        width="stretch",
    )
    st.bar_chart(t.drop(columns=["belady"], errors="ignore"))
    st.subheader("Paired difference vs LRU (percentage points, same trace & capacity)")
    st.dataframe(D.paired_delta(df).round(3), hide_index=True)
    st.caption(
        "Belady is the offline optimum (uses future knowledge) - a reference, not a competitor."
    )


# --- 5 ML evaluation ------------------------------------------------------------------------


def page_models() -> None:
    from cachemind.ml.features import CATEGORICAL_FEATURES, FEATURE_SCHEMA_VERSION, NUMERIC_FEATURES

    st.title("ML model evaluation")
    models = D.list_models()
    if models.empty:
        st.warning("No model artifacts. Run `make train` or `make spark-mllib`.")
        return
    badge("saved")
    st.dataframe(models.drop(columns=["path"]).head(200), hide_index=True)
    choice = st.selectbox("Inspect artifact", models["path"].tolist()[:200])
    meta = D.load_model_metadata(choice)
    c1, c2 = st.columns(2)
    c1.write(
        {
            k: meta.get(k)
            for k in (
                "model_type",
                "runtime",
                "model_version",
                "feature_schema_version",
                "label",
                "training_dataset_version",
                "selected_params",
                "decision_threshold_from_validation",
            )
        }
    )
    test = meta["metrics"]["test@val_threshold"]
    c2.write(
        {
            k: test.get(k)
            for k in (
                "n",
                "positive_rate",
                "precision",
                "recall",
                "f1",
                "roc_auc",
                "pr_auc",
                "brier",
                "ece",
            )
        }
    )
    st.subheader("Calibration (test block)")
    cal = pd.DataFrame(test.get("calibration", []))
    if not cal.empty:
        st.line_chart(cal.set_index("mean_pred")[["frac_pos"]])
    st.subheader("Non-ML baselines on the same test rows")
    st.dataframe(pd.DataFrame(meta.get("baselines_test", {})).T[["roc_auc", "pr_auc", "f1"]])
    st.subheader(f"Feature schema {FEATURE_SCHEMA_VERSION}")
    st.write({"numeric": list(NUMERIC_FEATURES), "categorical": list(CATEGORICAL_FEATURES)})
    df, m7 = D.load_experiment("exp07_lr_vs_rf")
    if m7:
        st.subheader("Logistic regression vs random forest (experiment 7, mean over runs)")
        st.dataframe(pd.DataFrame(m7.get("classification_summary", {})).T)


# --- 6 analytics ----------------------------------------------------------------------------


def page_analytics() -> None:
    st.title("Big Data analytics")
    datasets = D.list_datasets()
    if not datasets:
        st.warning("No Silver datasets. Run `make generate-data spark-etl`.")
        return
    badge("offline")
    ds = st.selectbox("Dataset", datasets)
    q = D.load_quality_report(ds)
    if q:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Bronze rows", f"{q.get('bronze_rows_total', 0):,}")
        c2.metric("Silver rows", f"{q.get('silver', {}).get('rows', 0):,}")
        c3.metric("Quarantined", f"{q.get('quarantine_total', 0):,}")
        c4.metric("Row accounting", "OK" if q.get("row_accounting_ok") else "MISMATCH")
        st.bar_chart(pd.Series(q.get("quarantine_by_reason", {}), name="records"))
        if "verification" in q:
            st.caption(
                "Verification against generator ground truth: "
                + ("all checks passed" if q["verification"].get("all_ok") else "FAILED")
            )
    tabs = st.tabs(
        ["Top items", "Categories", "Regions", "Hourly", "Daily", "User groups", "Popularity trend"]
    )
    with tabs[0]:
        t = D.load_gold("item_request_counts", ds, limit=50_000)
        if not t.empty:
            st.dataframe(t.sort_values("popularity_rank").head(25), hide_index=True)
            st.caption("Popularity distribution (rank vs requests)")
            st.line_chart(
                t.sort_values("popularity_rank")
                .head(2000)
                .set_index("popularity_rank")["total_requests"]
            )
    with tabs[1]:
        st.bar_chart(D.load_gold("category_counts", ds).set_index("category")[["requests"]])
    with tabs[2]:
        st.bar_chart(D.load_gold("region_counts", ds).set_index("region")[["requests"]])
    with tabs[3]:
        h = D.load_gold("hourly_totals", ds)
        if not h.empty:
            st.line_chart(h.set_index("hour_start")[["requests", "distinct_items"]])
    with tabs[4]:
        st.dataframe(D.load_gold("daily_totals", ds), hide_index=True)
    with tabs[5]:
        g = D.load_gold("user_group_patterns", ds)
        if not g.empty:
            st.bar_chart(
                g.pivot_table(index="category", columns="user_group", values="share_of_group")
            )
    with tabs[6]:
        tr = D.load_gold("item_popularity_trend", ds)
        st.dataframe(
            tr.sort_values("rank_change", ascending=False).head(25), hide_index=True
        ) if not tr.empty else st.write("Needs at least two days of data.")


# --- 7 health --------------------------------------------------------------------------------


@st.cache_data(ttl=10, show_spinner="Checking services...")
def _health() -> dict:
    from cachemind.monitoring.health import system_health

    return system_health()


def page_health() -> None:
    st.title("System health")
    h = _health()
    st.caption(
        f"Checked at {h['checked_at']} (cached for 10 s). Unreachable services are "
        "shown as unavailable - no values are substituted."
    )
    cols = st.columns(4)
    for col, (name, key) in zip(
        cols,
        [
            ("Kafka", "kafka"),
            ("HBase", "hbase"),
            ("Spark streaming", "spark_streaming"),
            ("Memory", "memory"),
        ],
        strict=True,
    ):
        part = h[key]
        if key == "memory":
            col.metric(name, f"{part['available_gb']} GB free", f"{part['available_pct']}%")
            continue
        ok = part.get("ok")
        col.metric(name, {True: "available", False: "unavailable", None: "not running"}[ok])
        if part.get("error"):
            col.caption(part["error"])
    with st.expander("Kafka"):
        st.json(h["kafka"])
    with st.expander("HBase"):
        st.json(h["hbase"])
    sh = h["spark_streaming"]
    st.subheader("Spark streaming - current run")
    if "current_run" in sh:
        cur = sh["current_run"]
        c1, c2, c3 = st.columns(3)
        c1.metric("State", sh.get("state", "unknown"))
        c2.metric("Run", cur.get("run_id") or "-")
        c3.metric("Queries failed in this run", cur["queries_terminated_with_error"])
        with st.expander("Current run: last batches, late / dropped rows, errors"):
            st.json({k: cur[k] for k in ("started_at", "stopped_at", "event_time", "errors")})
            st.json(sh.get("last_batches", {}))
        st.caption(
            "HISTORICAL - not current health: "
            f"{sh['history']['queries_terminated_with_error']} query terminations with an "
            "error across all recorded runs, including deliberate failure-injection "
            "experiments (HBase stopped, injected partial failures)."
        )
        with st.expander("Error history by class"):
            st.json(sh["history"])
    else:
        st.json(sh)
    st.subheader("Data freshness and error counts")
    st.json(h["data"])


# --- 8 research experiments -----------------------------------------------------------------


def page_research() -> None:
    st.title("Research experiments")
    exps = D.list_experiments()
    if not exps:
        st.warning("No saved experiments. Run `make experiments`.")
        return
    badge("saved")
    name = st.selectbox("Experiment", exps)
    df, meta = D.load_experiment(name)
    with st.expander("Run metadata"):
        st.json(
            {
                k: v
                for k, v in meta.items()
                if k not in ("summary", "paired_vs_lru", "paired_vs_lfu")
            }
        )
    if name == "exp03_capacity" and not df.empty:
        for wl in sorted(df["workload"].unique()):
            st.subheader(f"Hit rate vs capacity - {wl}")
            st.line_chart(
                df[df["workload"] == wl].pivot_table(
                    index="capacity", columns="policy", values="hit_rate"
                )
            )
    elif name in ("exp05_shift", "exp06_drift") and not df.empty:
        seed = st.selectbox("Seed", sorted(df["seed"].unique()))
        ts = []
        for r in df[df["seed"] == seed].itertuples():
            for p in json.loads(r.timeseries or "[]"):
                ts.append({"t": p["t_end"], "policy": r.policy, "hit_rate": p["hit_rate"]})
        if ts:
            st.line_chart(
                pd.DataFrame(ts).pivot_table(index="t", columns="policy", values="hit_rate")
            )
        if name == "exp05_shift" and "recovery" in meta:
            st.dataframe(pd.DataFrame(meta["recovery"]), hide_index=True)
        if name == "exp06_drift" and meta.get("lr_auc_over_test_block"):
            st.subheader("Model ROC-AUC across the test block")
            auc = pd.DataFrame(meta["lr_auc_over_test_block"])
            st.line_chart(auc.pivot_table(index="window_start", columns="seed", values="roc_auc"))
    elif name == "exp09_overhead" and meta.get("summary"):
        st.dataframe(pd.DataFrame(meta["summary"]), hide_index=True)
        st.write("Peak Python heap (KiB):", meta.get("peak_memory_kib"))
    elif "workload" in df.columns and "policy" in df.columns and not df.empty:
        st.dataframe(D.hit_rate_table(df).style.format("{:.4f}"), width="stretch")
    st.subheader("Raw results (bounded)")
    st.dataframe(df.drop(columns=["timeseries"], errors="ignore").head(500), hide_index=True)
    st.download_button(
        "Download CSV",
        df.drop(columns=["timeseries"], errors="ignore").to_csv(index=False),
        file_name=f"{name}.csv",
        mime="text/csv",
    )
