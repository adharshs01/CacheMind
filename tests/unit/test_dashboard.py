"""Test group L: dashboard data loaders, input validation, metrics and page rendering."""

from __future__ import annotations

import json
from dataclasses import replace

import pandas as pd
import pytest

from cachemind.config import get_settings
from cachemind.dashboard import data as D


@pytest.fixture
def empty_settings(tmp_path):
    return replace(
        get_settings(),
        data_dir=tmp_path / "data",
        runtime_dir=tmp_path / "runtime",
        artifacts_dir=tmp_path / "artifacts",
        reports_dir=tmp_path / "reports",
    )


def test_loaders_tolerate_missing_backends(empty_settings):
    s = empty_settings
    assert D.list_experiments(s) == []
    df, meta = D.load_experiment("exp02_ml_vs_classical", s)
    assert df.empty and meta == {}
    assert D.list_models(s).empty
    assert D.list_datasets(s) == []
    assert D.load_gold("category_counts", "none", s).empty
    assert D.load_quality_report("none", s) == {}
    assert D.latest_pipeline_evidence(s) == {}


def test_path_traversal_is_rejected(empty_settings):
    with pytest.raises(ValueError):
        D.load_experiment("../../etc", empty_settings)
    with pytest.raises(ValueError):
        D.load_gold("category_counts", "../../x", empty_settings)


@pytest.mark.parametrize(
    ("n", "cap", "policy"),
    [
        (10, 5, "lru"),
        (300_000, 5, "lru"),
        (5_000, 0, "lru"),
        (5_000, 10**6, "lru"),
        (5_000, 10, "random"),
    ],
)
def test_simulation_input_validation_rejects(n, cap, policy):
    with pytest.raises(D.InvalidInput):
        D.validate_sim_request(n, cap, policy)


def test_simulation_input_validation_accepts():
    D.validate_sim_request(30_000, 100, "hybrid")


def _results() -> pd.DataFrame:
    rows = []
    for seed in (1, 2):
        for pol, hr in (("lru", 0.40), ("lfu", 0.45), ("ml-lr", 0.50), ("belady", 0.60)):
            rows.append(
                {
                    "workload": "zipf",
                    "seed": seed,
                    "capacity": 100,
                    "policy": pol,
                    "hit_rate": hr + 0.01 * seed,
                }
            )
    return pd.DataFrame(rows)


def test_hit_rate_table_and_paired_delta():
    t = D.hit_rate_table(_results())
    assert list(t.columns) == ["lru", "lfu", "ml-lr", "belady"]  # stable policy order
    assert t.loc["zipf", "ml-lr"] == pytest.approx(0.515)
    d = D.paired_delta(_results()).set_index("policy")
    assert d.loc["ml-lr", "mean"] == pytest.approx(10.0)  # percentage points vs LRU
    assert d.loc["lru", "mean"] == 0


def test_streaming_snapshot_without_streaming_is_not_live(empty_settings, monkeypatch):
    monkeypatch.setattr(D, "get_settings", lambda: empty_settings)
    snap = D.streaming_snapshot(empty_settings)
    assert snap.provenance == "none" and snap.recent_events.empty


def test_stale_heartbeat_is_never_labelled_live(empty_settings):
    rt = empty_settings.runtime_dir / "streaming"
    rt.mkdir(parents=True)
    (rt / "status.json").write_text(
        json.dumps({"mode": "continuous", "heartbeat": "2020-01-01T00:00:00+00:00"})
    )
    assert D.streaming_snapshot(empty_settings).provenance == "stale"


PAGES = [
    "page_overview",
    "page_live",
    "page_simulator",
    "page_comparison",
    "page_models",
    "page_analytics",
    "page_health",
    "page_research",
]


@pytest.mark.parametrize("page", PAGES)
def test_every_page_renders_without_exceptions(page):
    from streamlit.testing.v1 import AppTest

    script = f"from cachemind.dashboard import views\nviews.{page}()\n"
    at = AppTest.from_string(script, default_timeout=60).run()
    assert not at.exception, [e.value for e in at.exception]
    assert at.title  # every page has a title


def test_simulator_runs_a_simulation_through_the_form():
    from streamlit.testing.v1 import AppTest

    at = AppTest.from_string(
        "from cachemind.dashboard import views\nviews.page_simulator()\n", default_timeout=120
    ).run()
    at.number_input[1].set_value(5_000)  # trace length
    at.number_input[0].set_value(30)  # capacity
    at.button[0].click().run()
    assert not at.exception, [e.value for e in at.exception]
    labels = [m.label for m in at.metric]
    assert "Hit rate" in labels and "Evictions" in labels
