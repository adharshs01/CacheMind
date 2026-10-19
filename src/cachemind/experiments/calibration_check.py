"""Experiment 14 (POST-HOC diagnostic): why is logistic regression poorly calibrated?

    python -m cachemind.experiments.calibration_check

Observation (Exp. 7 and the Spark run on zipf-n100000-s42): LR ranks well (ROC-AUC
≈ 0.94) but over-predicts reuse on the test block (ECE ≈ 0.22–0.29); RF is calibrated.
Hypothesis: ``f_freq_log`` counts requests since the *start of the trace*, so it keeps
growing with trace age; test rows lie outside the training range, a linear model
extrapolates while trees saturate.

Test: train LR exactly as in Exp. 2 but with one numeric feature masked (set to a
constant, so the model cannot use it) and compare calibration, ranking and the ML-LR
cache hit rate. Also report how far each feature's test distribution lies outside the
training distribution. This does not change any earlier result.
"""

from __future__ import annotations

import json
from typing import Any, ClassVar

import numpy as np
from sklearn.linear_model import LogisticRegression

from cachemind.experiments.common import make_trace, run_policy, save_results
from cachemind.ml.dataset import build_reuse_dataset
from cachemind.ml.evaluation import classification_report
from cachemind.ml.features import NUMERIC_FEATURES
from cachemind.ml.sklearn_train import FittedEncoder

MASKS: dict[str, tuple[str, ...]] = {
    "all features": (),
    "without f_freq_log": ("f_freq_log",),
    "without f_cnt1000": ("f_cnt1000",),
    "without hour features": ("f_hour_sin", "f_hour_cos"),
}


class _MaskedLR:
    """Minimal scorer: shared assembly + sklearn LR, with masked numeric columns."""

    model_type = "logistic_regression"
    metadata: ClassVar[dict[str, Any]] = {}

    def __init__(self, enc: FittedEncoder, model: LogisticRegression, mask_idx: list[int]):
        self.enc, self.model, self.mask_idx = enc, model, mask_idx

    def _x(self, numeric: np.ndarray, cats: dict[str, np.ndarray]) -> np.ndarray:
        numeric = np.array(numeric, dtype=np.float64, copy=True)
        numeric[:, self.mask_idx] = 0.0
        return self.enc.transform(numeric, cats)

    def predict_proba(self, numeric: np.ndarray, cats: dict[str, np.ndarray]) -> np.ndarray:
        return self.model.predict_proba(self._x(np.atleast_2d(numeric), cats))[:, 1]

    def predict_one(self, numeric: list[float], category: str, region: str) -> float:
        return float(
            self.predict_proba(
                np.asarray([numeric]),
                {
                    "category": np.asarray([category], dtype=object),
                    "region": np.asarray([region], dtype=object),
                },
            )[0]
        )


def run(
    workloads: tuple[str, ...] = ("zipf", "bursty"),
    seeds: tuple[int, ...] = (1, 2, 3),
    n_events: int = 100_000,
    n_items: int = 5_000,
    capacity: int = 100,
    horizon: int = 1_000,
) -> list[dict[str, Any]]:
    from cachemind.experiments.common import Workbench
    from cachemind.ml.predictors import OnlinePredictor
    from cachemind.policies.learned import MLPolicy

    rows = []
    for workload in workloads:
        for seed in seeds:
            trace = make_trace(workload, seed, n_events, n_items)
            ds = build_reuse_dataset(trace, horizon)
            tr, te = ds.rows("train", max_rows=120_000, seed=seed), ds.rows("test")
            xn_tr, c_tr, y_tr = ds.take(tr)
            xn_te, c_te, y_te = ds.take(te)
            shift = {}
            for i, name in enumerate(NUMERIC_FEATURES):
                hi = np.percentile(xn_tr[:, i], 99)
                shift[name] = float((xn_te[:, i] > hi).mean())  # share above train p99
            for label, masked in MASKS.items():
                idx = [NUMERIC_FEATURES.index(f) for f in masked]
                xn_tr_m = xn_tr.copy()
                xn_tr_m[:, idx] = 0.0
                enc = FittedEncoder.fit(xn_tr_m, c_tr, standardize=True)
                model = LogisticRegression(max_iter=1000, C=1.0).fit(
                    enc.transform(xn_tr_m, c_tr), y_tr
                )
                scorer = _MaskedLR(enc, model, idx)
                rep = classification_report(y_te, scorer.predict_proba(xn_te, c_te))
                wb = Workbench(trace=trace, dataset=ds)
                sim = run_policy(
                    wb,
                    MLPolicy(OnlinePredictor(scorer, trace), horizon),  # type: ignore[arg-type]
                    capacity,
                )
                rows.append(
                    {
                        "experiment": "exp14_calibration_ablation",
                        "workload": workload,
                        "seed": seed,
                        "variant": label,
                        "horizon": horizon,
                        "capacity": capacity,
                        "ece": rep["ece"],
                        "brier": rep["brier"],
                        "roc_auc": rep["roc_auc"],
                        "pr_auc": rep["pr_auc"],
                        "mean_pred": float(scorer.predict_proba(xn_te, c_te).mean()),
                        "positive_rate": rep["positive_rate"],
                        "hit_rate": sim.hit_rate,
                        "test_share_above_train_p99": json.dumps(shift),
                    }
                )
    return rows


def main(argv: list[str] | None = None) -> int:
    import pandas as pd

    rows = run()
    df = pd.DataFrame(rows)
    summary = df.groupby(["workload", "variant"])[
        ["ece", "brier", "roc_auc", "mean_pred", "positive_rate", "hit_rate"]
    ].mean()
    save_results(
        "exp14_calibration_ablation",
        rows,
        {
            "post_hoc": True,
            "hypothesis": __doc__,
            "summary": json.loads(summary.reset_index().to_json(orient="records")),
        },
    )
    print(summary.round(4).to_string())
    print(json.loads(rows[0]["test_share_above_train_p99"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
