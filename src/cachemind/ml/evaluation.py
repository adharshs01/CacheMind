"""Classification metrics for reuse prediction.

Accuracy alone is misleading under class imbalance, so every evaluation reports
the positive rate together with precision/recall/F1, ROC-AUC, PR-AUC (average
precision), Brier score, log loss and expected calibration error.
"""

from __future__ import annotations

from typing import Any

import numpy as np
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    f1_score,
    log_loss,
    precision_score,
    recall_score,
    roc_auc_score,
)


def expected_calibration_error(y: np.ndarray, p: np.ndarray, bins: int = 10) -> tuple[float, list]:
    edges = np.linspace(0.0, 1.0, bins + 1)
    idx = np.clip(np.digitize(p, edges[1:-1]), 0, bins - 1)
    ece, table = 0.0, []
    for b in range(bins):
        m = idx == b
        if not m.any():
            continue
        conf, acc = float(p[m].mean()), float(y[m].mean())
        ece += m.mean() * abs(conf - acc)
        table.append({"bin": b, "count": int(m.sum()), "mean_pred": conf, "frac_pos": acc})
    return float(ece), table


def classification_report(y: np.ndarray, p: np.ndarray, threshold: float = 0.5) -> dict[str, Any]:
    y = np.asarray(y).astype(int)
    p = np.clip(np.asarray(p, dtype=np.float64), 0.0, 1.0)
    pred = (p >= threshold).astype(int)
    both = len(np.unique(y)) == 2
    ece, cal = expected_calibration_error(y, p)
    return {
        "n": len(y),
        "positive_rate": float(y.mean()) if len(y) else 0.0,
        "threshold": float(threshold),
        "accuracy": float((pred == y).mean()) if len(y) else 0.0,
        "precision": float(precision_score(y, pred, zero_division=0)),
        "recall": float(recall_score(y, pred, zero_division=0)),
        "f1": float(f1_score(y, pred, zero_division=0)),
        "roc_auc": float(roc_auc_score(y, p)) if both else None,
        "pr_auc": float(average_precision_score(y, p)) if both else None,
        "brier": float(brier_score_loss(y, p)),
        "log_loss": float(log_loss(y, np.clip(p, 1e-12, 1 - 1e-12), labels=[0, 1])),
        "ece": ece,
        "calibration": cal,
        "predicted_positive_rate": float(pred.mean()) if len(y) else 0.0,
    }


def best_f1_threshold(y: np.ndarray, p: np.ndarray) -> float:
    """Threshold maximising F1 — chosen on VALIDATION data only."""
    grid = np.unique(np.quantile(p, np.linspace(0.01, 0.99, 99)))
    best_t, best_f = 0.5, -1.0
    for t in grid:
        f = f1_score(y, (p >= t).astype(int), zero_division=0)
        if f > best_f:
            best_t, best_f = float(t), f
    return best_t


def heuristic_scores(numeric: np.ndarray, horizon: int) -> dict[str, np.ndarray]:
    """Non-ML predictors of reuse built from the same online features.

    Both treat an item's requests as a Poisson process and report
    P(at least one request in the next H) = 1 - exp(-rate * H):

    * recency:   rate = 1 / gap since the previous request (LRU-like belief);
                 first-ever requests use rate 0
    * frequency: rate = requests in the last 1000 positions / 1000 (LFU-like)
    """
    from cachemind.ml.features import NUMERIC_FEATURES

    first = numeric[:, NUMERIC_FEATURES.index("f_first")] > 0.5
    gap1 = np.expm1(numeric[:, NUMERIC_FEATURES.index("f_gap1_log")])
    cnt1000 = numeric[:, NUMERIC_FEATURES.index("f_cnt1000")]
    recency_rate = np.where(first, 0.0, 1.0 / np.maximum(gap1, 1.0))
    return {
        "heuristic_recency": 1.0 - np.exp(-recency_rate * horizon),
        "heuristic_frequency": 1.0 - np.exp(-(cnt1000 / 1000.0) * horizon),
    }
