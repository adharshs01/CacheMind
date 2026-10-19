# Experiment results (generated)
_Generated 2026-10-19 08:51 UTC by `python -m cachemind.experiments.report` from `reports/experiments/`. All traces are synthetic. Hit rates are measured on the test block (last 20 % of each trace) after identical warm-up for every policy; Belady is the offline optimum (reference, not a competitor). pp = percentage points._

## Experiments 1, 2, 4 — policies across workloads
Scale: 100000 requests per trace, 5000 catalogue items, capacity 100 items, seeds [1, 2, 3]. Code version `src-462f4ffbf765`.

### Mean hit rate
| workload | fifo | lru | lfu | ml-lr | ml-rf | hybrid-lr | hybrid-rf | belady |
|---|---|---|---|---|---|---|---|---|
| bursty | 0.2939 | 0.3342 | 0.4186 | 0.4664 | 0.4576 | 0.4664 | 0.4576 | 0.5443 |
| cold_start | 0.1284 | 0.1414 | 0.0249 | 0.2308 | 0.2415 | 0.2310 | 0.2411 | 0.3489 |
| drift | 0.2613 | 0.3008 | 0.0140 | 0.4050 | 0.4066 | 0.4020 | 0.4074 | 0.5186 |
| hot_set | 0.6713 | 0.7942 | 0.8066 | 0.8064 | 0.8062 | 0.8063 | 0.8063 | 0.8303 |
| mixed | 0.2623 | 0.2880 | 0.0368 | 0.3784 | 0.3792 | 0.3785 | 0.3794 | 0.4260 |
| periodic | 0.2636 | 0.3040 | 0.3868 | 0.4386 | 0.4289 | 0.4386 | 0.4296 | 0.5327 |
| scan | 0.1376 | 0.1540 | 0.2291 | 0.2183 | 0.2184 | 0.2183 | 0.2182 | 0.2544 |
| shift | 0.2629 | 0.3046 | 0.0313 | 0.4356 | 0.4319 | 0.4356 | 0.4311 | 0.5245 |
| uniform | 0.0199 | 0.0199 | 0.0204 | 0.0188 | 0.0197 | 0.0193 | 0.0200 | 0.1847 |
| zipf | 0.2629 | 0.3046 | 0.4170 | 0.4448 | 0.4342 | 0.4448 | 0.4338 | 0.5245 |

### Standard deviation across seeds
| workload | fifo | lru | lfu | ml-lr | ml-rf | hybrid-lr | hybrid-rf | belady |
|---|---|---|---|---|---|---|---|---|
| bursty | 0.0126 | 0.0116 | 0.0047 | 0.0091 | 0.0097 | 0.0091 | 0.0097 | 0.0086 |
| cold_start | 0.0021 | 0.0024 | 0.0007 | 0.0002 | 0.0008 | 0.0005 | 0.0003 | 0.0019 |
| drift | 0.0016 | 0.0024 | 0.0007 | 0.0041 | 0.0043 | 0.0067 | 0.0039 | 0.0035 |
| hot_set | 0.0015 | 0.0021 | 0.0012 | 0.0013 | 0.0012 | 0.0015 | 0.0013 | 0.0008 |
| mixed | 0.0023 | 0.0018 | 0.0012 | 0.0031 | 0.0021 | 0.0031 | 0.0022 | 0.0032 |
| periodic | 0.0032 | 0.0042 | 0.0044 | 0.0036 | 0.0030 | 0.0036 | 0.0036 | 0.0025 |
| scan | 0.0003 | 0.0008 | 0.0029 | 0.0015 | 0.0013 | 0.0013 | 0.0014 | 0.0017 |
| shift | 0.0016 | 0.0028 | 0.0014 | 0.0040 | 0.0061 | 0.0040 | 0.0050 | 0.0025 |
| uniform | 0.0003 | 0.0003 | 0.0003 | 0.0012 | 0.0019 | 0.0011 | 0.0017 | 0.0015 |
| zipf | 0.0016 | 0.0028 | 0.0037 | 0.0035 | 0.0044 | 0.0035 | 0.0051 | 0.0025 |

### Paired differences (same trace, same capacity, per seed)
| workload | ml-lr vs LRU (mean, pp) | ml-lr vs LRU (min, pp) | ml-lr vs LRU (max, pp) | ml-lr vs best classical (mean, pp) | ml-lr vs best classical (min, pp) | ml-lr vs best classical (max, pp) | ml-rf vs LRU (mean, pp) | ml-rf vs LRU (min, pp) | ml-rf vs LRU (max, pp) | ml-rf vs best classical (mean, pp) | ml-rf vs best classical (min, pp) | ml-rf vs best classical (max, pp) | hybrid-lr vs LRU (mean, pp) | hybrid-lr vs LRU (min, pp) | hybrid-lr vs LRU (max, pp) | hybrid-lr vs best classical (mean, pp) | hybrid-lr vs best classical (min, pp) | hybrid-lr vs best classical (max, pp) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| bursty | +13.22 | +12.98 | +13.54 | +4.78 | +3.70 | +5.50 | +12.34 | +11.88 | +12.69 | +3.90 | +2.85 | +4.43 | +13.22 | +12.98 | +13.54 | +4.78 | +3.70 | +5.50 |
| cold_start | +8.95 | +8.69 | +9.13 | +8.95 | +8.69 | +9.13 | +10.01 | +9.82 | +10.16 | +10.01 | +9.82 | +10.16 | +8.96 | +8.74 | +9.13 | +8.96 | +8.74 | +9.13 |
| drift | +10.42 | +10.15 | +10.56 | +10.42 | +10.15 | +10.56 | +10.59 | +10.30 | +10.73 | +10.59 | +10.30 | +10.73 | +10.12 | +9.62 | +10.54 | +10.12 | +9.62 | +10.54 |
| hot_set | +1.22 | +1.14 | +1.30 | -0.01 | -0.07 | +0.03 | +1.19 | +1.10 | +1.26 | -0.04 | -0.10 | +0.03 | +1.21 | +1.14 | +1.27 | -0.03 | -0.10 | +0.02 |
| mixed | +9.04 | +8.93 | +9.26 | +9.04 | +8.93 | +9.26 | +9.12 | +9.02 | +9.24 | +9.12 | +9.02 | +9.24 | +9.05 | +8.93 | +9.26 | +9.05 | +8.93 | +9.26 |
| periodic | +13.45 | +13.38 | +13.51 | +5.18 | +4.27 | +5.76 | +12.49 | +12.27 | +12.60 | +4.21 | +3.37 | +4.65 | +13.45 | +13.38 | +13.51 | +5.18 | +4.27 | +5.76 |
| scan | +6.44 | +6.27 | +6.60 | -1.08 | -1.21 | -0.89 | +6.44 | +6.30 | +6.58 | -1.08 | -1.23 | -0.86 | +6.43 | +6.27 | +6.58 | -1.08 | -1.24 | -0.89 |
| shift | +13.10 | +12.94 | +13.21 | +13.10 | +12.94 | +13.21 | +12.73 | +12.34 | +13.04 | +12.73 | +12.34 | +13.04 | +13.10 | +12.94 | +13.21 | +13.10 | +12.94 | +13.21 |
| uniform | -0.11 | -0.20 | +0.05 | -0.16 | -0.26 | -0.03 | -0.02 | -0.17 | +0.22 | -0.08 | -0.20 | +0.14 | -0.05 | -0.20 | +0.05 | -0.11 | -0.20 | -0.03 |
| zipf | +14.02 | +13.86 | +14.17 | +2.78 | +2.74 | +2.82 | +12.96 | +12.75 | +13.11 | +1.72 | +1.66 | +1.81 | +14.02 | +13.86 | +14.17 | +2.78 | +2.74 | +2.82 |

### Selected label horizon H per workload (validation)
| workload | horizon |
|---|---|
| bursty | 1000 |
| cold_start | 1000 |
| drift | 1000 |
| hot_set | 500, 1000 |
| mixed | 1000 |
| periodic | 1000 |
| scan | 1000 |
| shift | 1000 |
| uniform | 200, 1000 |
| zipf | 1000 |

## Experiment 7 — logistic regression vs random forest (classification, test block, mean over seeds)
| workload | model_type | test_positive_rate | roc_auc | pr_auc | f1 | brier | ece | baseline_recency_roc_auc | baseline_frequency_roc_auc | selected_alpha | export_predict_one_us_p50 | fit_seconds |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| bursty | logistic_regression | 0.569 | 0.941 | 0.962 | 0.847 | 0.209 | 0.253 | 0.898 | 0.886 | 1.000 | 0.716 | 0.037 |
| bursty | random_forest | 0.569 | 0.935 | 0.957 | 0.871 | 0.119 | 0.106 | 0.898 | 0.886 | 1.000 | 34.500 | 0.471 |
| cold_start | logistic_regression | 0.367 | 0.821 | 0.800 | 0.705 | 0.150 | 0.079 | 0.702 | 0.723 | 0.750 | 0.722 | 0.075 |
| cold_start | random_forest | 0.367 | 0.842 | 0.821 | 0.719 | 0.178 | 0.185 | 0.702 | 0.723 | 0.917 | 34.681 | 0.474 |
| drift | logistic_regression | 0.551 | 0.897 | 0.917 | 0.812 | 0.202 | 0.219 | 0.875 | 0.856 | 0.833 | 0.722 | 0.040 |
| drift | random_forest | 0.551 | 0.904 | 0.925 | 0.844 | 0.133 | 0.084 | 0.875 | 0.856 | 0.833 | 35.542 | 0.474 |
| hot_set | logistic_regression | 0.811 | 0.996 | 0.999 | 0.995 | 0.010 | 0.021 | 0.993 | 0.996 | 0.750 | 0.708 | 0.049 |
| hot_set | random_forest | 0.811 | 0.995 | 0.999 | 0.994 | 0.007 | 0.009 | 0.993 | 0.996 | 0.583 | 31.653 | 0.276 |
| mixed | logistic_regression | 0.395 | 0.988 | 0.983 | 0.914 | 0.062 | 0.113 | 0.984 | 0.948 | 0.917 | 0.723 | 0.099 |
| mixed | random_forest | 0.395 | 0.988 | 0.983 | 0.920 | 0.052 | 0.081 | 0.984 | 0.948 | 0.833 | 34.264 | 0.480 |
| periodic | logistic_regression | 0.574 | 0.921 | 0.949 | 0.828 | 0.196 | 0.229 | 0.875 | 0.868 | 1.000 | 0.709 | 0.079 |
| periodic | random_forest | 0.574 | 0.915 | 0.944 | 0.852 | 0.128 | 0.090 | 0.875 | 0.868 | 0.917 | 34.979 | 0.471 |
| scan | logistic_regression | 0.219 | 0.995 | 0.985 | 0.895 | 0.057 | 0.068 | 0.992 | 0.947 | 0.833 | 0.722 | 0.036 |
| scan | random_forest | 0.219 | 0.995 | 0.983 | 0.921 | 0.028 | 0.027 | 0.992 | 0.947 | 0.750 | 34.402 | 0.329 |
| shift | logistic_regression | 0.556 | 0.926 | 0.950 | 0.837 | 0.196 | 0.233 | 0.890 | 0.877 | 1.000 | 0.709 | 0.053 |
| shift | random_forest | 0.556 | 0.923 | 0.948 | 0.849 | 0.125 | 0.100 | 0.890 | 0.877 | 0.833 | 34.514 | 0.470 |
| uniform | logistic_regression | 0.133 | 0.493 | 0.131 | 0.226 | 0.111 | 0.007 | 0.498 | 0.499 | 0.833 | 0.736 | 0.022 |
| uniform | random_forest | 0.133 | 0.500 | 0.134 | 0.228 | 0.111 | 0.006 | 0.498 | 0.499 | 0.750 | 29.028 | 0.395 |
| zipf | logistic_regression | 0.556 | 0.937 | 0.958 | 0.830 | 0.238 | 0.286 | 0.890 | 0.877 | 1.000 | 0.736 | 0.077 |
| zipf | random_forest | 0.556 | 0.930 | 0.952 | 0.859 | 0.127 | 0.118 | 0.890 | 0.877 | 0.917 | 35.028 | 0.482 |

## Experiment 3 — cache capacity

### mixed: mean hit rate by capacity
| capacity | fifo | lru | lfu | ml-lr | hybrid-lr | belady |
|---|---|---|---|---|---|---|
| 25 | 0.1890 | 0.2210 | 0.0331 | 0.3030 | 0.3031 | 0.3484 |
| 50 | 0.2264 | 0.2532 | 0.0350 | 0.3397 | 0.3395 | 0.3877 |
| 100 | 0.2623 | 0.2880 | 0.0368 | 0.3784 | 0.3785 | 0.4260 |
| 200 | 0.3004 | 0.3262 | 0.1531 | 0.4223 | 0.4219 | 0.4624 |
| 400 | 0.3388 | 0.3672 | 0.1660 | 0.4316 | 0.4316 | 0.4911 |
| 800 | 0.3774 | 0.4064 | 0.2476 | 0.4624 | 0.4623 | 0.4943 |

### mixed: paired differences by capacity
| capacity | ml-lr vs LRU (mean, pp) | ml-lr vs LRU (min, pp) | ml-lr vs LRU (max, pp) | ml-lr vs best classical (mean, pp) | ml-lr vs best classical (min, pp) | ml-lr vs best classical (max, pp) | hybrid-lr vs LRU (mean, pp) | hybrid-lr vs LRU (min, pp) | hybrid-lr vs LRU (max, pp) | hybrid-lr vs best classical (mean, pp) | hybrid-lr vs best classical (min, pp) | hybrid-lr vs best classical (max, pp) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 25 | +8.20 | +8.13 | +8.30 | +8.20 | +8.13 | +8.30 | +8.21 | +8.15 | +8.30 | +8.21 | +8.15 | +8.30 |
| 50 | +8.65 | +8.54 | +8.78 | +8.65 | +8.54 | +8.78 | +8.63 | +8.54 | +8.72 | +8.63 | +8.54 | +8.72 |
| 100 | +9.04 | +8.93 | +9.26 | +9.04 | +8.93 | +9.26 | +9.05 | +8.93 | +9.26 | +9.05 | +8.93 | +9.26 |
| 200 | +9.60 | +9.53 | +9.67 | +9.60 | +9.53 | +9.67 | +9.57 | +9.51 | +9.67 | +9.57 | +9.51 | +9.67 |
| 400 | +6.44 | +6.41 | +6.49 | +6.44 | +6.41 | +6.49 | +6.44 | +6.41 | +6.49 | +6.44 | +6.41 | +6.49 |
| 800 | +5.59 | +5.52 | +5.70 | +5.59 | +5.52 | +5.70 | +5.59 | +5.52 | +5.70 | +5.59 | +5.52 | +5.70 |

### zipf: mean hit rate by capacity
| capacity | fifo | lru | lfu | ml-lr | hybrid-lr | belady |
|---|---|---|---|---|---|---|
| 25 | 0.1299 | 0.1523 | 0.2785 | 0.2961 | 0.2961 | 0.3615 |
| 50 | 0.1908 | 0.2241 | 0.3497 | 0.3672 | 0.3672 | 0.4411 |
| 100 | 0.2629 | 0.3046 | 0.4170 | 0.4448 | 0.4448 | 0.5245 |
| 200 | 0.3511 | 0.3951 | 0.4988 | 0.5276 | 0.5276 | 0.6128 |
| 400 | 0.4509 | 0.4995 | 0.5839 | 0.6157 | 0.6157 | 0.7026 |
| 800 | 0.5675 | 0.6157 | 0.6771 | 0.7097 | 0.7097 | 0.7961 |

### zipf: paired differences by capacity
| capacity | ml-lr vs LRU (mean, pp) | ml-lr vs LRU (min, pp) | ml-lr vs LRU (max, pp) | ml-lr vs best classical (mean, pp) | ml-lr vs best classical (min, pp) | ml-lr vs best classical (max, pp) | hybrid-lr vs LRU (mean, pp) | hybrid-lr vs LRU (min, pp) | hybrid-lr vs LRU (max, pp) | hybrid-lr vs best classical (mean, pp) | hybrid-lr vs best classical (min, pp) | hybrid-lr vs best classical (max, pp) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 25 | +14.38 | +14.29 | +14.44 | +1.76 | +1.30 | +2.29 | +14.38 | +14.29 | +14.44 | +1.76 | +1.30 | +2.29 |
| 50 | +14.31 | +13.69 | +14.66 | +1.75 | +1.03 | +2.56 | +14.31 | +13.69 | +14.66 | +1.75 | +1.03 | +2.56 |
| 100 | +14.02 | +13.86 | +14.17 | +2.78 | +2.74 | +2.82 | +14.02 | +13.86 | +14.17 | +2.78 | +2.74 | +2.82 |
| 200 | +13.26 | +13.20 | +13.34 | +2.88 | +2.67 | +3.08 | +13.26 | +13.20 | +13.34 | +2.88 | +2.67 | +3.08 |
| 400 | +11.62 | +11.45 | +11.72 | +3.18 | +3.13 | +3.25 | +11.62 | +11.45 | +11.72 | +3.18 | +3.13 | +3.25 |
| 800 | +9.39 | +9.22 | +9.63 | +3.25 | +3.16 | +3.36 | +9.39 | +9.22 | +9.63 | +3.25 | +3.16 | +3.36 |

## Experiment 5 — sudden popularity shift inside the test block
| workload | fifo | lru | lfu | ml-lr | ml-rf | hybrid-lr | hybrid-rf | belady |
|---|---|---|---|---|---|---|---|---|
| shift@0.9 | 0.2625 | 0.3040 | 0.2243 | 0.4030 | 0.4168 | 0.4030 | 0.4168 | 0.5216 |

Recovery after the shift (mean over seeds):
| policy | pre_change_hit_rate | min_after_change | requests_to_recover_90pct |
|---|---|---|---|
| belady | 0.5229 | 0.4747 | 1666.6667 |
| fifo | 0.2596 | 0.2407 | 1000.0000 |
| hybrid-lr | 0.4409 | 0.2130 | 7000.0000 |
| hybrid-rf | 0.4304 | 0.2650 | 2666.6667 |
| lfu | 0.4146 | 0.0223 |  |
| lru | 0.3015 | 0.2753 | 1000.0000 |
| ml-lr | 0.4409 | 0.2130 | 7000.0000 |
| ml-rf | 0.4303 | 0.2623 | 2666.6667 |

## Experiment 6 — concept drift
| workload | fifo | lru | lfu | ml-lr | ml-rf | hybrid-lr | hybrid-rf | belady |
|---|---|---|---|---|---|---|---|---|
| drift | 0.2613 | 0.3008 | 0.0140 | 0.4050 | 0.4066 | 0.4020 | 0.4074 | 0.5186 |

Logistic-regression ROC-AUC on consecutive windows of the test block (model trained once on the train block):
| window_start | roc_auc | positive_rate |
|---|---|---|
| 80000 | 0.9038 | 0.5551 |
| 82375 | 0.9007 | 0.5548 |
| 84750 | 0.8978 | 0.5523 |
| 87125 | 0.8989 | 0.5476 |
| 89500 | 0.8977 | 0.5475 |
| 91875 | 0.8862 | 0.5521 |
| 94250 | 0.8996 | 0.5492 |
| 96625 | 0.9023 | 0.5505 |

## Experiment 8 — hybrid α selected on validation
| workload | model_type | selected_alpha |
|---|---|---|
| bursty | logistic_regression | 1, 1, 1 |
| bursty | random_forest | 1, 1, 1 |
| cold_start | logistic_regression | 0.75, 0.75, 0.75 |
| cold_start | random_forest | 0.75, 1, 1 |
| drift | logistic_regression | 0.75, 1, 0.75 |
| drift | random_forest | 0.75, 0.75, 1 |
| hot_set | logistic_regression | 1, 0.5, 0.75 |
| hot_set | random_forest | 0.25, 0.5, 1 |
| mixed | logistic_regression | 0.75, 1, 1 |
| mixed | random_forest | 0.75, 1, 0.75 |
| periodic | logistic_regression | 1, 1, 1 |
| periodic | random_forest | 1, 0.75, 1 |
| scan | logistic_regression | 0.75, 0.75, 1 |
| scan | random_forest | 0.75, 0.75, 0.75 |
| shift | logistic_regression | 1, 1, 1 |
| shift | random_forest | 1, 0.75, 0.75 |
| uniform | logistic_regression | 1, 1, 0.5 |
| uniform | random_forest | 1, 1, 0.25 |
| zipf | logistic_regression | 1, 1, 1 |
| zipf | random_forest | 0.75, 1, 1 |

## Experiment 13 — post-hoc horizon sensitivity (ML-LR, mean of seeds; does not change experiments 1–8)
| workload | factor | horizon | val_hit_rate | hit_rate |
|---|---|---|---|---|
| bursty | 2 | 200 | 0.3825 | 0.3765 |
| bursty | 5 | 500 | 0.4488 | 0.4445 |
| bursty | 10 | 1000 | 0.4712 | 0.4664 |
| bursty | 20 | 2000 | 0.4775 | 0.4738 |
| bursty | 40 | 4000 | 0.4772 | 0.4739 |
| drift | 2 | 200 | 0.3477 | 0.3453 |
| drift | 5 | 500 | 0.4024 | 0.4018 |
| drift | 10 | 1000 | 0.4055 | 0.4050 |
| drift | 20 | 2000 | 0.3857 | 0.3873 |
| drift | 40 | 4000 | 0.3504 | 0.3503 |
| zipf | 2 | 200 | 0.3534 | 0.3521 |
| zipf | 5 | 500 | 0.4222 | 0.4221 |
| zipf | 10 | 1000 | 0.4458 | 0.4448 |
| zipf | 20 | 2000 | 0.4516 | 0.4522 |
| zipf | 40 | 4000 | 0.4529 | 0.4535 |

## Experiment 14 — post-hoc LR calibration ablation (mean of seeds)
| workload | variant | ece | brier | roc_auc | mean_pred | positive_rate | hit_rate |
|---|---|---|---|---|---|---|---|
| bursty | all features | 0.2746 | 0.2291 | 0.9411 | 0.8432 | 0.5686 | 0.4671 |
| bursty | without f_cnt1000 | 0.2750 | 0.2295 | 0.9411 | 0.8436 | 0.5686 | 0.4671 |
| bursty | without f_freq_log | 0.0628 | 0.1184 | 0.9206 | 0.6303 | 0.5686 | 0.4426 |
| bursty | without hour features | 0.0930 | 0.1137 | 0.9386 | 0.6617 | 0.5686 | 0.4626 |
| zipf | all features | 0.2868 | 0.2382 | 0.9368 | 0.8426 | 0.5559 | 0.4446 |
| zipf | without f_cnt1000 | 0.2956 | 0.2484 | 0.9368 | 0.8515 | 0.5559 | 0.4453 |
| zipf | without f_freq_log | 0.0623 | 0.1222 | 0.9142 | 0.6182 | 0.5559 | 0.4202 |
| zipf | without hour features | 0.0936 | 0.1155 | 0.9338 | 0.6495 | 0.5559 | 0.4396 |

## Experiment 9 — per-request overhead (medians of 5 timed runs after 1 warm-up, microseconds)
| policy | us_per_request_total_median | us_per_request_features_median | us_per_request_inference_median | eviction_decision_us_mean_median | eviction_decision_us_p99_median | inference_us_p50_median | inference_us_p95_median |
|---|---|---|---|---|---|---|---|
| hybrid-lr | 8.51 | 1.02 | 0.92 | 9.85 | 10.79 | 0.88 | 0.96 |
| lfu | 0.78 | 0.00 | 0.00 | 0.11 | 0.17 |  |  |
| lru | 0.59 | 0.00 | 0.00 | 0.08 | 0.12 |  |  |
| ml-lr | 3.10 | 0.90 | 0.84 | 0.47 | 1.67 | 0.79 | 0.88 |
| ml-rf | 38.25 | 1.15 | 35.38 | 0.61 | 1.96 | 34.92 | 36.75 |

Peak Python heap during a separate tracemalloc run (KiB): {"lru": 6288, "lfu": 5962, "ml-lr": 12967, "ml-rf": 12979, "hybrid-lr": 12925}

## Experiment 10 — Spark batch performance
| size | stage | config | seconds | seconds_min | seconds_max | repeats | raw_input_bytes |
|---|---|---|---|---|---|---|---|
| 20000 | bronze | default (4 partitions, broadcast metadata) | 0.76 | 0.75 | 0.81 | 3.00 | 0.00 |
| 20000 | silver | default (4 partitions, broadcast metadata) | 3.53 | 3.26 | 3.69 | 3.00 | 0.00 |
| 20000 | gold | default (4 partitions, broadcast metadata) | 2.18 | 2.12 | 2.37 | 3.00 | 0.00 |
| 20000 | features | default (4 partitions, broadcast metadata) | 0.42 | 0.41 | 0.46 | 3.00 | 0.00 |
| 100000 | generate (python) | - | 1.28 |  |  |  |  |
| 100000 | bronze | default (4 partitions, broadcast metadata) | 1.37 | 1.15 | 1.45 | 3.00 | 0.00 |
| 100000 | silver | default (4 partitions, broadcast metadata) | 10.07 | 9.33 | 10.13 | 3.00 | 0.00 |
| 100000 | gold | default (4 partitions, broadcast metadata) | 3.01 | 2.67 | 3.10 | 3.00 | 0.00 |
| 100000 | features | default (4 partitions, broadcast metadata) | 0.76 | 0.73 | 0.82 | 3.00 | 0.00 |
| 1000000 | generate (python) | - | 49.11 |  |  |  |  |
| 1000000 | bronze | default (4 partitions, broadcast metadata) | 3.70 | 3.70 | 3.70 | 1.00 | 0.00 |
| 1000000 | silver | default (4 partitions, broadcast metadata) | 72.18 | 72.18 | 72.18 | 1.00 | 0.00 |
| 1000000 | gold | default (4 partitions, broadcast metadata) | 7.50 | 7.50 | 7.50 | 1.00 | 0.00 |
| 1000000 | features | default (4 partitions, broadcast metadata) | 4.69 | 4.69 | 4.69 | 1.00 | 0.00 |
| 100000 | silver+gold | A: shuffle partitions=4 | 9.44 | 9.01 | 47.14 | 3.00 |  |
| 100000 | silver+gold | A: shuffle partitions=200 | 51.85 | 51.81 | 106.26 | 3.00 |  |
| 100000 | silver | B: broadcast metadata join | 7.46 | 7.24 | 7.58 | 3.00 |  |
| 100000 | silver | B: shuffle (sort-merge) metadata join | 10.17 | 9.73 | 10.95 | 3.00 |  |
| 100000 | silver read | C: pruned columns + pushed filter | 0.20 | 0.17 | 0.22 | 3.00 |  |
| 100000 | silver read | C: cache all columns, then filter | 3.29 | 3.16 | 3.32 | 3.00 |  |

Plan evidence: pushed filter present = True, pruned read schema = True.

## Experiment 11 — streaming throughput
| phase | events | seconds | events_per_s | batches | rows | processed_rows_per_s_median | batch_duration_ms_p50 | batch_duration_ms_max | note | target_rate | achieved_produce_rate | kafka_to_batch_start_ms_p50 | kafka_to_batch_start_ms_p95 | kafka_to_bronze_file_ms_p50 | kafka_to_bronze_file_ms_p95 | kafka_to_bronze_file_ms_max | latency_rows | checks | passed |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| produce_backlog | 20000.00 | 0.57 | 34982.60 |  |  |  |  |  | None |  |  |  |  |  |  |  |  | None | None |
| drain_backlog:bronze_ingest |  |  |  | 11.00 | 20000.00 | 7483.15 | 257.00 | 2159.00 | None |  |  |  |  |  |  |  |  | None | None |
| drain_backlog:silver_stream |  |  |  | 6.00 | 20000.00 | 1885.82 | 2033.00 | 3975.00 | None |  |  |  |  |  |  |  |  | None | None |
| drain_backlog:gold_windows_v2 |  |  |  | 6.00 | 20000.00 | 2442.48 | 1516.50 | 2133.00 | None |  |  |  |  |  |  |  |  | None | None |
| drain_backlog:total | 20000.00 | 36.14 | 553.41 |  |  |  |  |  | includes Spark/JVM start-up and three sequential available-now queries |  |  |  |  |  |  |  |  | None | None |
| live | 18000.00 |  |  |  |  |  |  |  | latency over rows appended to Kafka during the live phase only; processing-time trigger every 3 s | 400.00 | 400.00 | 1519.00 | 2869.00 | 1813.28 | 3144.91 | 4196.15 | 18000.00 | None | None |
| verification |  |  |  |  |  |  |  |  | None |  |  |  |  |  |  |  |  | {'all_clean_events_in_silver': True, 'all_events_in_hbase': True, 'bronze_exactly_once': True, 'reconcile_consistent': True} | True |

## Experiment 12 — failure recovery
| scenario | expectation | passed | checks | topic_messages | bronze_rows | silver_distinct | silver_cross_batch_duplicates | late_flagged | query_restarts | injected | continuous_run_restarts | continuous_run_last_errors | fault_triggered | fault_marker_exists | parquet_written_before_restart | manifest_written_before_restart | before | after |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| duplicate_malformed_late | malformed -> quarantine; duplicates never create extra HBase rows; late events kept in Silver and flagged | True | {"bronze_exactly_once": true, "all_clean_events_in_silver": true, "all_events_in_hbase": true, "reconcile_consistent": true, "late_events_flagged": true, "malformed_quarantined": true} | 4174.00 | 4174.00 | 4000.00 | 1.00 | 31.00 | {} | {"duplicate": 119, "late": 78, "malformed": 55} | None | None | None | None | None | None | None | None |
| spark_process_killed | restart from checkpoint: Bronze exactly-once, no loss | True | {"bronze_exactly_once": true, "all_clean_events_in_silver": true, "all_events_in_hbase": true, "reconcile_consistent": true} | 6000.00 | 6000.00 | 6000.00 | 0.00 | 0.00 | {} | None | None | None | None | None | None | None | None | None |
| kafka_broker_restart | query survives/restarts; no loss or duplication | True | {"bronze_exactly_once": true, "all_clean_events_in_silver": true, "all_events_in_hbase": true, "reconcile_consistent": true} | 4000.00 | 4000.00 | 4000.00 | 0.00 | 203.00 | {} | None | {} | {"bronze_ingest": "", "silver_stream": "", "gold_windows_v2": ""} | None | None | None | None | None | None |
| hbase_unavailable | failed batches are replayed after HBase returns; idempotent puts leave no gaps | True | {"bronze_exactly_once": true, "all_clean_events_in_silver": true, "all_events_in_hbase": true, "reconcile_consistent": true} | 3000.00 | 3000.00 | 3000.00 | 0.00 | 0.00 | {} | None | {"silver_stream": 2, "gold_windows_v2": 2} | {"bronze_ingest": "", "silver_stream": "An exception was raised by the Python Proxy. Return Message: Traceback (most recent call last):\n  File \"/Users/adharshsundaramurthy/Documents/CacheMind/.venv/lib/python3.12/site-packages/py4j/clientserver.py\", line 644, in _call_proxy\n    return_value = getattr(self.pool[obj_id], method)(*params)\n  ", "gold_windows_v2": "An exception was raised by the Python Proxy. Return Message: Traceback (most recent call last):\n  File \"/Users/adharshsundaramurthy/Documents/CacheMind/.venv/lib/python3.12/site-packages/py4j/clientserver.py\", line 644, in _call_proxy\n    return_value = getattr(self.pool[obj_id], method)(*params)\n  "} | None | None | None | None | None | None |
| partial_output_failure | batch replayed after restart; reconcile consistent | True | {"bronze_exactly_once": true, "all_clean_events_in_silver": true, "all_events_in_hbase": true, "reconcile_consistent": true} | 3000.00 | 3000.00 | 3000.00 | 0.00 | 0.00 | {} | None | None | None | True | True | True | False | None | None |
| redrain_without_new_data | a restart with no new offsets writes nothing | True | {"unchanged": true} |  |  |  |  |  | None | None | None | None | None | None | None | None | [3000, 3000] | [3000, 3000] |
