"""Generate docs/EXPERIMENT_RESULTS.md from saved experiment outputs.

    python -m cachemind.experiments.report

Every number in the generated file is computed from reports/experiments/*/ — nothing is
typed by hand — so the documentation cannot drift from the stored results.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pandas as pd

from cachemind.config import REPO_ROOT, get_settings

ORDER = ["fifo", "lru", "lfu", "ml-lr", "ml-rf", "hybrid-lr", "hybrid-rf", "belady"]


def _load(name: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    d = get_settings().reports_dir / "experiments" / name
    if not (d / "results.parquet").exists():
        return pd.DataFrame(), {}
    meta = json.loads((d / "meta.json").read_text()) if (d / "meta.json").exists() else {}
    return pd.read_parquet(d / "results.parquet"), meta


def _md(df: pd.DataFrame, floatfmt: str = "{:.4f}") -> str:
    if df.empty:
        return "_no data_\n"
    out = df.copy()
    for c in out.columns:
        if pd.api.types.is_float_dtype(out[c]):
            out[c] = out[c].map(lambda v: "" if pd.isna(v) else floatfmt.format(v))
    cols = [str(c) for c in out.columns]
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for row in out.itertuples(index=False):
        lines.append("| " + " | ".join(str(v) for v in row) + " |")
    return "\n".join(lines) + "\n"


def _pivot(df: pd.DataFrame, index: str, agg: str = "mean") -> pd.DataFrame:
    p = df.pivot_table(index=index, columns="policy", values="hit_rate", aggfunc=agg)
    p = p[[c for c in ORDER if c in p.columns]]
    return p.reset_index()


def _paired(df: pd.DataFrame, keys: list[str], policies: list[str]) -> pd.DataFrame:
    rows = []
    for key, g in df.groupby(keys):
        hr = g.set_index("policy")["hit_rate"]
        best = max(hr.get(p, float("-inf")) for p in ("fifo", "lru", "lfu"))
        rec = dict(zip(keys, key if isinstance(key, tuple) else (key,), strict=True))
        for p in policies:
            if p in hr:
                rec[f"{p} vs LRU"] = 100 * (hr[p] - hr["lru"])
                rec[f"{p} vs best classical"] = 100 * (hr[p] - best)
        rows.append(rec)
    out = pd.DataFrame(rows)
    group = [k for k in keys if k != "seed"]
    agg = out.groupby(group).agg(["mean", "min", "max"])
    agg.columns = [f"{a} ({b}, pp)" for a, b in agg.columns]
    return agg.drop(columns=[c for c in agg.columns if c.startswith("seed")]).reset_index()


def build() -> str:
    parts: list[str] = [
        "# Experiment results (generated)\n",
        f"_Generated {datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')} by "
        "`python -m cachemind.experiments.report` from `reports/experiments/`. "
        "All traces are synthetic. Hit rates are measured on the test block (last 20 % of "
        "each trace) after identical warm-up for every policy; Belady is the offline optimum "
        "(reference, not a competitor). pp = percentage points._\n",
    ]
    df, meta = _load("exp02_ml_vs_classical")
    if not df.empty:
        sc = meta.get("scale", {})
        parts += [
            "\n## Experiments 1, 2, 4 — policies across workloads\n",
            f"Scale: {sc.get('n_events')} requests per trace, {sc.get('n_items')} catalogue "
            f"items, capacity {sc.get('capacity')} items, seeds {sc.get('seeds')}. "
            f"Code version `{meta.get('code_version')}`.\n\n",
            "### Mean hit rate\n",
            _md(_pivot(df, "workload")),
            "\n### Standard deviation across seeds\n",
            _md(_pivot(df, "workload", "std")),
            "\n### Paired differences (same trace, same capacity, per seed)\n",
            _md(_paired(df, ["workload", "seed"], ["ml-lr", "ml-rf", "hybrid-lr"]), "{:+.2f}"),
            "\n### Selected label horizon H per workload (validation)\n",
            _md(
                df.groupby("workload")["horizon"]
                .agg(lambda s: ", ".join(str(v) for v in sorted(set(s))))
                .reset_index()
            ),
        ]
    mpath = get_settings().reports_dir / "experiments" / "model_metrics.json"
    if mpath.exists():
        m = pd.DataFrame(json.loads(mpath.read_text()))
        t = (
            m.groupby(["workload", "model_type"])[
                [
                    "test_positive_rate",
                    "roc_auc",
                    "pr_auc",
                    "f1",
                    "brier",
                    "ece",
                    "baseline_recency_roc_auc",
                    "baseline_frequency_roc_auc",
                    "selected_alpha",
                    "export_predict_one_us_p50",
                    "fit_seconds",
                ]
            ]
            .mean()
            .reset_index()
        )
        parts += [
            "\n## Experiment 7 — logistic regression vs random forest "
            "(classification, test block, mean over seeds)\n",
            _md(t, "{:.3f}"),
        ]
    df3, _ = _load("exp03_capacity")
    if not df3.empty:
        parts.append("\n## Experiment 3 — cache capacity\n")
        for wl, g in df3.groupby("workload"):
            parts += [
                f"\n### {wl}: mean hit rate by capacity\n",
                _md(_pivot(g, "capacity")),
                f"\n### {wl}: paired differences by capacity\n",
                _md(_paired(g, ["capacity", "seed"], ["ml-lr", "hybrid-lr"]), "{:+.2f}"),
            ]
    for name, title in (
        ("exp05_shift", "Experiment 5 — sudden popularity shift inside the test block"),
        ("exp06_drift", "Experiment 6 — concept drift"),
    ):
        d, mt = _load(name)
        if d.empty:
            continue
        parts += [f"\n## {title}\n", _md(_pivot(d, "workload"))]
        if mt.get("recovery"):
            parts += [
                "\nRecovery after the shift (mean over seeds):\n",
                _md(pd.DataFrame(mt["recovery"]), "{:.4f}"),
            ]
        if mt.get("lr_auc_over_test_block"):
            auc = pd.DataFrame(mt["lr_auc_over_test_block"])
            parts += [
                "\nLogistic-regression ROC-AUC on consecutive windows of the test "
                "block (model trained once on the train block):\n",
                _md(
                    auc.groupby("window_start")[["roc_auc", "positive_rate"]].mean().reset_index(),
                    "{:.4f}",
                ),
            ]
    d8, m8 = _load("exp08_hybrid")
    if m8.get("alpha_selection"):
        a = pd.DataFrame(m8["alpha_selection"])
        parts += [
            "\n## Experiment 8 — hybrid α selected on validation\n",
            _md(
                a.groupby(["workload", "model_type"])["selected_alpha"]
                .agg(lambda s: ", ".join(f"{v:g}" for v in s))
                .reset_index()
            ),
        ]
    d13, _ = _load("exp13_horizon_sensitivity")
    if not d13.empty:
        t13 = (
            d13.groupby(["workload", "factor", "horizon"])[["val_hit_rate", "hit_rate"]]
            .mean()
            .reset_index()
        )
        parts += [
            "\n## Experiment 13 — post-hoc horizon sensitivity (ML-LR, mean of seeds; "
            "does not change experiments 1–8)\n",
            _md(t13),
        ]
    d14, _ = _load("exp14_calibration_ablation")
    if not d14.empty:
        t14 = (
            d14.groupby(["workload", "variant"])[
                ["ece", "brier", "roc_auc", "mean_pred", "positive_rate", "hit_rate"]
            ]
            .mean()
            .reset_index()
        )
        parts += [
            "\n## Experiment 14 — post-hoc LR calibration ablation (mean of seeds)\n",
            _md(t14),
        ]
    d9, m9 = _load("exp09_overhead")
    if m9.get("summary"):
        s9 = pd.DataFrame(m9["summary"])
        keep = [c for c in s9.columns if c == "policy" or c.endswith("_median")]
        parts += [
            "\n## Experiment 9 — per-request overhead (medians of "
            f"{m9.get('repeats')} timed runs after 1 warm-up, microseconds)\n",
            _md(s9[keep], "{:.2f}"),
            "\nPeak Python heap during a separate "
            "tracemalloc run (KiB): "
            + json.dumps({k: round(v) for k, v in m9.get("peak_memory_kib", {}).items()})
            + "\n",
        ]
    d10, m10 = _load("exp10_spark_performance")
    if not d10.empty:
        cols = [
            c
            for c in (
                "size",
                "stage",
                "config",
                "seconds",
                "seconds_min",
                "seconds_max",
                "repeats",
                "silver_rows",
                "raw_input_bytes",
            )
            if c in d10.columns
        ]
        parts += [
            "\n## Experiment 10 — Spark batch performance\n",
            _md(d10[d10["stage"] != "plan evidence"][cols], "{:.2f}"),
        ]
        ev = d10[d10["stage"] == "plan evidence"]
        if not ev.empty:
            r = ev.iloc[0]
            parts.append(
                f"\nPlan evidence: pushed filter present = {r.get('pushed_filters')}, "
                f"pruned read schema = {r.get('read_schema_pruned')}.\n"
            )
    for name, title in (
        ("exp11_streaming_throughput", "Experiment 11 — streaming throughput"),
        ("exp12_failure_recovery", "Experiment 12 — failure recovery"),
    ):
        d, _ = _load(name)
        if not d.empty:
            show = d.drop(
                columns=[c for c in d.columns if c in ("notes", "quarantine")], errors="ignore"
            )
            parts += [f"\n## {title}\n", _md(show, "{:.2f}")]
    return "".join(parts)


def main(argv: list[str] | None = None) -> int:
    out = REPO_ROOT / "docs" / "EXPERIMENT_RESULTS.md"
    out.write_text(build())
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
