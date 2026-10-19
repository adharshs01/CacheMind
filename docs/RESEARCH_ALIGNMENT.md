# Research alignment: CacheMind vs. Learning Relaxed Belady (LRB)

## Reference

Zhenyu Song, Daniel S. Berger, Kai Li, Wyatt Lloyd. **Learning Relaxed Belady for
Content Distribution Network Caching.** 17th USENIX Symposium on Networked Systems
Design and Implementation (NSDI '20), pp. 529–544, Santa Clara, CA, February 2020.
<https://www.usenix.org/conference/nsdi20/presentation/song>
(author PDF: <https://www.cs.princeton.edu/~wlloyd/papers/lrb-nsdi20.pdf>)

> **Correction to the project brief.** The brief cites `https://arxiv.org/abs/1803.02376`.
> That arXiv record is *"Magnetic field strength of a neutron-star-powered ultraluminous
> X-ray source"* (Brightman et al., Nature Astronomy, 2018), not the caching paper. We
> found no arXiv version of the LRB paper; the USENIX page and the authors' PDF above
> are the primary sources and were read for this document.

## What the paper does (from the paper itself)

| Aspect | LRB |
|---|---|
| Problem | Reduce CDN **byte miss ratio** / WAN traffic. Heuristic policies (LRU variants) leave a large gap to Belady's MIN. |
| Key idea | Imitate **relaxed Belady**: evict *any* object whose next request is beyond the **Belady boundary** (the minimum time-to-next-request among objects Belady evicts), instead of the single farthest one. This makes the prediction problem easier. |
| Design metric | **Good decision ratio**: fraction of evictions whose next request is beyond the boundary; correlates with byte miss ratio and is cheap to compute. |
| Prediction target | **Regression** of `log(time-to-next-request)` with L2 loss; objects not requested within the sliding memory window are labelled 2× the window size. Binary classification relative to a boundary was explored and rejected. |
| Features | 32 *deltas* (gaps between consecutive requests), 10 *exponentially decayed counters* (EDCs), static features (object size, content type). |
| Training data | Periodic random sample of **objects** (not requests) in a sliding memory window; labels assigned when the object is requested again or falls out of the window. Retrained every 128K labelled samples. |
| Model | Gradient boosting machines (LightGBM); also tried linear/logistic regression, SVM, small NN. |
| Eviction | Sample **64 random cached objects**, batch-predict, evict the one with the largest predicted time to next request. |
| Evaluation | Six production CDN traces (Wikipedia, three CDNs), byte miss ratio vs. B-LRU, LRU, LRU-K, LFUDA, S4LRU, GDSF, Hyperbolic, TinyLFU, LeCaR, UCB, LFO, LHD, AdaptSize; Apache Traffic Server prototype. First 20 % of each trace is a validation prefix for tuning; a warm-up section is excluded from metrics. |
| Reported result | 4–25 % WAN traffic reduction vs. a production design (USENIX abstract says 5–24 %); closes about a quarter of the gap to Belady. |
| Stated limitations | A significant gap to Belady remains (prediction error); the validation prefix is short for choosing the sliding-window size; overheads must be controlled. |

## Component-by-component alignment

| Paper component | Our implementation | Reproduction or adaptation | Reason for adaptation | Evidence |
|---|---|---|---|---|
| Goal: approximate Belady with ML | ML-guided policy ranks residents by learned reuse likelihood; Belady MIN implemented as offline reference | **Adaptation** | Same goal, simpler decision rule (below) | `policies/learned.py`, `policies/belady.py`, exhaustive optimality tests |
| Relaxed Belady / Belady boundary | Not implemented. We use a fixed **reuse horizon H** (chosen on validation from {2,5,10}× capacity) as the decision threshold, and evict items whose horizon expired first | **Adaptation** | H plays the role of the boundary but is tuned, not derived from Belady's decisions | `ml/features.py` docstring, `experiments/common.py::select_horizon` |
| Prediction target: regression on log(time-to-next-request) | **Binary classification**: "requested again within the next H requests" | **Adaptation** (the brief requires binary classification) | The paper explicitly tried this target and found regression better; our results must be read with that in mind | `ml/features.py::reuse_labels` |
| Unit of time | Requests (positions in the ordered trace) | Partial match | LRB also measures the boundary in requests (e.g., "1M requests") | `ml/features.py` |
| Features: 32 deltas, 10 EDCs, static | 2 gaps (≈ delta1, delta2), windowed counts over 10/100/1000 requests, cumulative frequency, a trend term, cold-start flag, size, hour-of-day, category, region | **Adaptation** (smaller set) | Fewer features keep Spark ⇄ Python parity tractable and the model inspectable; windowed counts approximate EDCs | `ml/features.py`, Spark parity test |
| Training-data sampling: random objects in a sliding window | Every request is a sample (features at the request, label = reuse within H); chronological train/val/test split with label-horizon purging | **Adaptation** | Simpler, fully leakage-checked offline protocol; the paper's online sampler was not reproduced. Our sampling over *requests* is biased toward popular items — exactly the bias LRB avoids | `ml/dataset.py`, `ml/splits.py` |
| Labelling of never-again objects (2× window) | Rows whose label window runs past the end of a split are **excluded** (purged / censored) | **Adaptation** | Avoids inventing labels; prevents end-of-block bias | `tests/unit/test_features.py` |
| Online retraining every 128K samples | **Not implemented** — models trained once offline per trace (batch, Spark MLlib or scikit-learn) | **Not reproduced** | Brief requires controlled batch retraining, not per-request learning; drift experiment (6) measures the cost of not retraining | Exp. 6 |
| Model: GBM (LightGBM) | Logistic regression and random forest (Spark MLlib; scikit-learn in the lightweight profile) | **Adaptation** | Brief mandates Spark MLlib LR + RF; Spark's GBT is binary-classification only and was not evaluated | `bigdata/mllib_train.py` |
| Eviction: sample 64, evict largest predicted distance | Rank **all** residents: expired-horizon items first (oldest first), then lowest predicted reuse probability; one prediction per request cached with the item | **Adaptation** | Deterministic (no random sampling); inference cost independent of cache size | `policies/learned.py` |
| Hybrid with heuristics | Hybrid score α·p + (1−α)·recency, α tuned on validation | Our addition | Not in LRB | Exp. 8 |
| Objective: byte miss ratio, variable sizes | **Object hit/miss ratio, unit-size items** | **Adaptation** | Brief: establish fixed-capacity correctness before variable sizes | `simulation/engine.py` |
| Traces: 6 production CDN traces | **Synthetic** traces (10 workload shapes) | **Not reproduced** | No production traces available; results are not comparable to the paper's numbers | `data/workloads.py` |
| Validation prefix + warm-up | Chronological 60/20/20 split; H and α tuned on validation; metrics only on the last 20 %; all policies warmed on the same prefix | **Match in spirit** | Same discipline, different proportions | `experiments/common.py` |
| Baselines | FIFO, LRU, LFU (+ Belady). Not: B-LRU, LRU-K, GDSF, TinyLFU, LHD, AdaptSize, … | **Partial** | Brief scope | Exp. 1, 2 |
| Prototype in a real cache server | Not built; simulator + Spark/Kafka/HBase analytics pipeline instead | **Not reproduced** | Different project focus (ML + Big Data coursework) | – |

## Honest summary

CacheMind is **inspired by** LRB: it learns, from request history only, which cached
items are unlikely to be reused soon and evicts those, and it compares against Belady's
MIN as an upper bound under the same assumptions. It is **not a reproduction** of LRB:
the label (classification within H vs. regression of log distance), the features, the
training-data sampling, the model family, the eviction rule, the metric (object vs. byte
miss ratio) and the data (synthetic vs. production) all differ. Hit-rate differences
reported in `EXPERIMENTS.md` must not be compared to the paper's WAN-traffic numbers.
