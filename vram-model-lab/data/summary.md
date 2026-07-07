# VRAM Probe Summary

- total rows: 276
- successful rows: 276
- OOM rows: 0
- near-capacity risk rows (>=90% reported GPU memory): 15
- families: {'mlp': 55, 'transformer': 128, 'cnn': 93}
- precisions: {'fp32': 105, 'fp16': 159, 'bf16': 12}
- GPU SKU labels: ['rtx-4090', 'unknown']
- GPUs: ['NVIDIA GeForce RTX 4090']
- GPU total MiB values: [24563]
- GPU total GiB values: [23.99]

Note: this WSL/NVIDIA runtime allowed synthetic allocations that reached
the reported card limit without surfacing a hard CUDA OOM. Treat the
near-capacity rows as risk calibration data, not as definitive bare-metal
OOM labels.

## Fitted Model

- fit: ridge_linear_interactions
- alpha: 25.0
- feature mode: interactions
- training rows: 276
- in-sample MAE MiB: 981.2
- in-sample p95 absolute error MiB: 2680.5
- leave-one-out MAE MiB: 1036.7
- leave-one-out p95 absolute error MiB: 2805.1
- leave-one-out max error MiB: 7636.0

## Top VRAM Driver Groups

| rank | group | normalized impact MiB/std |
| ---: | --- | ---: |
| 1 | synthetic headroom | 5048.6 |
| 2 | parameters | 2630.6 |
| 3 | activations | 994.1 |
| 4 | architecture | 267.8 |
| 5 | model family | 190.9 |
| 6 | precision | 91.1 |
| 7 | input shape | 88.7 |
| 8 | optimizer | 51.7 |

## Top VRAM Model Drivers

Impact is coefficient multiplied by observed feature standard deviation,
so columns with different units can be compared directionally. Negative
weights are model weights under correlated features, not causal claims
that the feature lowers true VRAM.

| rank | feature | group | model weight | impact MiB/std | meaning |
| ---: | --- | --- | --- | ---: | --- |
| 1 | reserve_extra_gib | synthetic headroom | positive_model_weight | 5024.2 | synthetic VRAM headroom probe allocation |
| 2 | param_x_precision | parameters | positive_model_weight | 2110.8 | parameter count multiplied by precision bytes |
| 3 | activation_x_precision | activations | negative_model_weight | -590.1 | activation footprint multiplied by precision bytes |
| 4 | param_count_m | parameters | negative_model_weight | -519.8 | parameter count in millions |
| 5 | activation_x_batch | activations | positive_model_weight | 238.5 | activation footprint multiplied by batch size |
| 6 | layers | architecture | positive_model_weight | 205.4 | model depth |
| 7 | family_cnn | model family | positive_model_weight | 176.3 | CNN model-family indicator |
| 8 | activation_units_m | activations | positive_model_weight | 165.5 | batch * sequence/image shape * hidden/layers activation footprint |
| 9 | precision_bytes | precision | positive_model_weight | 91.1 | bytes per tensor element from fp32/fp16/bf16/int8 |
| 10 | batch_size | input shape | negative_model_weight | -88.7 | training batch size |

## Family Models

| family | rows | usable | fit | alpha | in-sample MAE MiB | LOO MAE MiB | LOO p95 abs error MiB |
| --- | ---: | --- | --- | ---: | ---: | ---: | ---: |
| cnn | 93 | True | ridge_linear_interactions | 10.0 | 405.4 | 512.1 | 1431.4 |
| mlp | 55 | True | ridge_linear_base | 100.0 | 693.4 | 790.2 | 3232.0 |
| transformer | 128 | True | ridge_linear_interactions | 10.0 | 736.0 | 804.4 | 2204.9 |

## Rows

| scenario | family | precision | optimizer | checkpoint | reserve MiB | peak nvidia-smi MiB | peak torch reserved MiB | params |
| --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: |
| smoke-mlp | mlp | fp32 | adamw | False | 0 | 2021 | 94 | 2624768 |
| smoke-mlp | mlp | fp32 | adamw | False | 0 | 1945 | 94 | 2624768 |
| transformer-small-fp32 | transformer | fp32 | adamw | False | 0 | 4901 | 2974 | 91711232 |
| transformer-small-fp16 | transformer | fp16 | adamw | False | 0 | 4257 | 2324 | 91711232 |
| transformer-longseq-fp16 | transformer | fp16 | adamw | False | 0 | 5133 | 3200 | 166337792 |
| transformer-checkpointed-fp16 | transformer | fp16 | adamw | True | 0 | 5605 | 3672 | 166337792 |
| cnn-vision-fp32 | cnn | fp32 | sgd | False | 0 | 3425 | 1494 | 38328 |
| mlp-batch128-fp32 | mlp | fp32 | adamw | False | 0 | 2199 | 272 | 11018240 |
| mlp-batch256-fp16 | mlp | fp16 | adamw | False | 0 | 2067 | 154 | 11018240 |
| transformer-small-sgd-fp32 | transformer | fp32 | sgd | False | 0 | 3963 | 2286 | 91711232 |
| transformer-batch16-fp16 | transformer | fp16 | adamw | False | 0 | 5823 | 3890 | 91711232 |
| transformer-seq1024-fp32 | transformer | fp32 | adamw | False | 0 | 6319 | 4392 | 166337792 |
| transformer-wide-fp16 | transformer | fp16 | adamw | False | 0 | 6313 | 4368 | 381651200 |
| transformer-wide-checkpointed-fp16 | transformer | fp16 | adamw | True | 0 | 5947 | 4014 | 381651200 |
| cnn-vision-batch64-fp32 | cnn | fp32 | sgd | False | 0 | 4913 | 2982 | 38328 |
| cnn-vision-384-fp16 | cnn | fp16 | adamw | False | 0 | 3713 | 1774 | 82960 |
| transformer-wide-fp16-pad12g | transformer | fp16 | adamw | False | 12288 | 18412 | 16656 | 381651200 |
| transformer-wide-fp16-pad16g | transformer | fp16 | adamw | False | 16384 | 22685 | 20752 | 381651200 |
| transformer-wide-fp16-oom-pad22g | transformer | fp16 | adamw | False | 22528 | 24116 | 26896 | 381651200 |
| cnn-vision-384-fp16-pad12g | cnn | fp16 | adamw | False | 12288 | 14983 | 14062 | 82960 |
| mlp-forced-oom-pad32g | mlp | fp16 | sgd | False | 32768 | 23902 | 32790 | 262912 |
| grid-transformer-fp32-b2-s256 | transformer | fp32 | adamw | False | 0 | 1627 | 2034 | 91711232 |
| grid-transformer-fp32-b6-s256 | transformer | fp32 | adamw | False | 0 | 1625 | 2432 | 91711232 |
| grid-transformer-fp32-b2-s768 | transformer | fp32 | adamw | False | 0 | 3395 | 2432 | 91711232 |
| grid-transformer-fp32-b6-s768 | transformer | fp32 | adamw | False | 0 | 6185 | 5222 | 91711232 |
| grid-transformer-fp32-b2-s1536 | transformer | fp32 | adamw | False | 0 | 5221 | 5130 | 166337792 |
| grid-transformer-fp32-b6-s1536 | transformer | fp32 | adamw | False | 0 | 13389 | 12426 | 166337792 |
| grid-transformer-fp16-b2-s256 | transformer | fp16 | adamw | False | 0 | 1687 | 1032 | 91711232 |
| grid-transformer-fp16-b6-s256 | transformer | fp16 | adamw | False | 0 | 1227 | 1426 | 91711232 |
| grid-transformer-fp16-b2-s768 | transformer | fp16 | adamw | False | 0 | 1215 | 1426 | 91711232 |
| grid-transformer-fp16-b6-s768 | transformer | fp16 | adamw | False | 0 | 3649 | 2652 | 91711232 |
| grid-transformer-fp16-b2-s1536 | transformer | fp16 | adamw | False | 0 | 3561 | 2564 | 166337792 |
| grid-transformer-fp16-b6-s1536 | transformer | fp16 | adamw | False | 0 | 7095 | 6098 | 166337792 |
| grid-cnn-fp32-b64-i160 | cnn | fp32 | sgd | False | 0 | 2319 | 1324 | 33688 |
| grid-cnn-fp32-b24-i256 | cnn | fp32 | sgd | False | 0 | 2071 | 1076 | 33688 |
| grid-cnn-fp32-b8-i384 | cnn | fp32 | sgd | False | 0 | 2851 | 1856 | 82960 |
| grid-cnn-fp16-b64-i160 | cnn | fp16 | adamw | False | 0 | 1759 | 756 | 33688 |
| grid-cnn-fp16-b24-i256 | cnn | fp16 | adamw | False | 0 | 1523 | 728 | 33688 |
| grid-cnn-fp16-b8-i384 | cnn | fp16 | adamw | False | 0 | 1905 | 902 | 82960 |
| grid-pressure-transformer-pad8g | transformer | fp16 | adamw | False | 8192 | 13057 | 12558 | 381651200 |
| grid-pressure-transformer-pad18g | transformer | fp16 | adamw | False | 18432 | 20295 | 22798 | 381651200 |
| focus-cnn-fp32-b48-i224 | cnn | fp32 | sgd | False | 0 | 2686 | 2250 | 38328 |
| focus-cnn-fp32-b16-i320 | cnn | fp32 | sgd | False | 0 | 3008 | 2272 | 82960 |
| focus-cnn-fp16-b32-i224 | cnn | fp16 | adamw | False | 0 | 1894 | 846 | 38328 |
| focus-cnn-fp16-b12-i320 | cnn | fp16 | adamw | False | 0 | 1892 | 964 | 82960 |
| focus-cnn-fp16-b16-i384-pad6g | cnn | fp16 | adamw | False | 6144 | 8964 | 7918 | 82960 |
| focus-cnn-fp16-b16-i384-pad10g | cnn | fp16 | adamw | False | 10240 | 13062 | 12014 | 82960 |
| focus-mlp-fp32-b64-s1024-h2048 | mlp | fp32 | adamw | False | 0 | 1484 | 448 | 20982784 |
| focus-mlp-fp16-b256-s1024-h2048 | mlp | fp16 | adamw | False | 0 | 1100 | 258 | 20982784 |
| focus-mlp-fp32-b128-s2048-h2048 | mlp | fp32 | sgd | False | 0 | 1348 | 298 | 33570816 |
| focus-mlp-fp16-b512-s2048-h2048 | mlp | fp16 | sgd | False | 0 | 1176 | 202 | 33570816 |
| focus-mlp-fp16-b128-s1024-pad8g | mlp | fp16 | adamw | False | 8192 | 9252 | 8258 | 4198400 |
| focus-mlp-fp16-b128-s1024-pad16g | mlp | fp16 | adamw | False | 16384 | 17438 | 16450 | 4198400 |
| grid-transformer-fp32-b2-s256 | transformer | fp32 | adamw | False | 0 | 3267 | 2034 | 91711232 |
| grid-transformer-fp32-b6-s256 | transformer | fp32 | adamw | False | 0 | 2865 | 2432 | 91711232 |
| grid-transformer-fp32-b2-s768 | transformer | fp32 | adamw | False | 0 | 3287 | 2432 | 91711232 |
| grid-transformer-fp32-b6-s768 | transformer | fp32 | adamw | False | 0 | 6455 | 5222 | 91711232 |
| grid-transformer-fp32-b2-s1536 | transformer | fp32 | adamw | False | 0 | 6363 | 5130 | 166337792 |
| grid-transformer-fp32-b6-s1536 | transformer | fp32 | adamw | False | 0 | 13659 | 12426 | 166337792 |
| grid-transformer-fp16-b2-s256 | transformer | fp16 | adamw | False | 0 | 2045 | 1032 | 91711232 |
| grid-transformer-fp16-b6-s256 | transformer | fp16 | adamw | False | 0 | 1445 | 1426 | 91711232 |
| grid-transformer-fp16-b2-s768 | transformer | fp16 | adamw | False | 0 | 1443 | 1426 | 91711232 |
| grid-transformer-fp16-b6-s768 | transformer | fp16 | adamw | False | 0 | 3327 | 2652 | 91711232 |
| grid-transformer-fp16-b2-s1536 | transformer | fp16 | adamw | False | 0 | 1637 | 2564 | 166337792 |
| grid-transformer-fp16-b6-s1536 | transformer | fp16 | adamw | False | 0 | 6209 | 6098 | 166337792 |
| grid-cnn-fp32-b64-i160 | cnn | fp32 | sgd | False | 0 | 2255 | 1324 | 33688 |
| grid-cnn-fp32-b24-i256 | cnn | fp32 | sgd | False | 0 | 2313 | 1076 | 33688 |
| grid-cnn-fp32-b8-i384 | cnn | fp32 | sgd | False | 0 | 3093 | 1856 | 82960 |
| grid-cnn-fp16-b64-i160 | cnn | fp16 | adamw | False | 0 | 2001 | 756 | 33688 |
| grid-cnn-fp16-b24-i256 | cnn | fp16 | adamw | False | 0 | 1757 | 728 | 33688 |
| grid-cnn-fp16-b8-i384 | cnn | fp16 | adamw | False | 0 | 2147 | 902 | 82960 |
| grid-pressure-transformer-pad8g | transformer | fp16 | adamw | False | 8192 | 10231 | 12558 | 381651200 |
| grid-pressure-transformer-pad18g | transformer | fp16 | adamw | False | 18432 | 21687 | 22798 | 381651200 |
| focus-cnn-fp32-b48-i224 | cnn | fp32 | sgd | False | 0 | 3424 | 2250 | 38328 |
| focus-cnn-fp32-b16-i320 | cnn | fp32 | sgd | False | 0 | 3442 | 2272 | 82960 |
| focus-cnn-fp16-b32-i224 | cnn | fp16 | adamw | False | 0 | 2028 | 846 | 38328 |
| focus-cnn-fp16-b12-i320 | cnn | fp16 | adamw | False | 0 | 2146 | 964 | 82960 |
| focus-cnn-fp16-b16-i384-pad6g | cnn | fp16 | adamw | False | 6144 | 9100 | 7918 | 82960 |
| focus-cnn-fp16-b16-i384-pad10g | cnn | fp16 | adamw | False | 10240 | 13196 | 12014 | 82960 |
| focus-mlp-fp32-b64-s1024-h2048 | mlp | fp32 | adamw | False | 0 | 1210 | 448 | 20982784 |
| focus-mlp-fp16-b256-s1024-h2048 | mlp | fp16 | adamw | False | 0 | 1244 | 258 | 20982784 |
| focus-mlp-fp32-b128-s2048-h2048 | mlp | fp32 | sgd | False | 0 | 1468 | 298 | 33570816 |
| focus-mlp-fp16-b512-s2048-h2048 | mlp | fp16 | sgd | False | 0 | 1268 | 202 | 33570816 |
| focus-mlp-fp16-b128-s1024-pad8g | mlp | fp16 | adamw | False | 8192 | 9378 | 8258 | 4198400 |
| focus-mlp-fp16-b128-s1024-pad16g | mlp | fp16 | adamw | False | 16384 | 17578 | 16450 | 4198400 |
| overnight-cnn-resnet-fp32-b64-i160 | cnn | fp32 | sgd | False | 0 | 2154 | 1088 | 57032 |
| overnight-cnn-resnet-fp32-b32-i224 | cnn | fp32 | sgd | False | 0 | 2256 | 1084 | 57032 |
| overnight-cnn-resnet-fp32-b12-i320 | cnn | fp32 | sgd | False | 0 | 2288 | 1154 | 75848 |
| overnight-cnn-resnet-fp32-b8-i384 | cnn | fp32 | sgd | False | 0 | 2218 | 1046 | 75848 |
| overnight-cnn-resnet-fp16-b64-i160 | cnn | fp16 | adamw | False | 0 | 1742 | 562 | 57032 |
| overnight-cnn-resnet-fp16-b32-i224 | cnn | fp16 | adamw | False | 0 | 1654 | 534 | 57032 |
| overnight-cnn-resnet-fp16-b12-i320 | cnn | fp16 | adamw | False | 0 | 1756 | 576 | 75848 |
| overnight-cnn-resnet-fp16-b8-i384 | cnn | fp16 | adamw | False | 0 | 1726 | 592 | 75848 |
| smoke-mlp | mlp | fp32 | adamw | False | 0 | 1193 | 94 | 2624768 |
| mlp-batch128-fp32 | mlp | fp32 | adamw | False | 0 | 1447 | 272 | 11018240 |
| mlp-batch256-fp16 | mlp | fp16 | adamw | False | 0 | 1227 | 154 | 11018240 |
| transformer-small-fp32 | transformer | fp32 | adamw | False | 0 | 4149 | 2974 | 91711232 |
| transformer-small-sgd-fp32 | transformer | fp32 | sgd | False | 0 | 2703 | 2286 | 91711232 |
| transformer-small-fp16 | transformer | fp16 | adamw | False | 0 | 3505 | 2324 | 91711232 |
| transformer-batch16-fp16 | transformer | fp16 | adamw | False | 0 | 4571 | 3890 | 91711232 |
| transformer-seq1024-fp32 | transformer | fp32 | adamw | False | 0 | 5567 | 4392 | 166337792 |
| transformer-longseq-fp16 | transformer | fp16 | adamw | False | 0 | 4381 | 3200 | 166337792 |
| transformer-wide-fp16 | transformer | fp16 | adamw | False | 0 | 5547 | 4366 | 381651200 |
| transformer-checkpointed-fp16 | transformer | fp16 | adamw | True | 0 | 4853 | 3672 | 166337792 |
| transformer-wide-checkpointed-fp16 | transformer | fp16 | adamw | True | 0 | 5195 | 4014 | 381651200 |
| transformer-wide-fp16-pad12g | transformer | fp16 | adamw | False | 12288 | 17835 | 16654 | 381651200 |
| transformer-wide-fp16-pad16g | transformer | fp16 | adamw | False | 16384 | 20745 | 20750 | 381651200 |
| transformer-wide-fp16-oom-pad22g | transformer | fp16 | adamw | False | 22528 | 24064 | 26894 | 381651200 |
| mlp-forced-oom-pad32g | mlp | fp16 | sgd | False | 32768 | 23796 | 32790 | 262912 |
| cnn-vision-fp32 | cnn | fp32 | sgd | False | 0 | 2432 | 1494 | 38328 |
| cnn-vision-batch64-fp32 | cnn | fp32 | sgd | False | 0 | 3920 | 2982 | 38328 |
| cnn-vision-384-fp16 | cnn | fp16 | adamw | False | 0 | 2720 | 1774 | 82960 |
| cnn-vision-384-fp16-pad12g | cnn | fp16 | adamw | False | 12288 | 15008 | 14062 | 82960 |
| grid-transformer-fp32-b2-s256 | transformer | fp32 | adamw | False | 0 | 1584 | 2034 | 91711232 |
| grid-transformer-fp32-b6-s256 | transformer | fp32 | adamw | False | 0 | 2988 | 2432 | 91711232 |
| grid-transformer-fp32-b2-s768 | transformer | fp32 | adamw | False | 0 | 2874 | 2432 | 91711232 |
| grid-transformer-fp32-b6-s768 | transformer | fp32 | adamw | False | 0 | 6156 | 5222 | 91711232 |
| grid-transformer-fp32-b2-s1536 | transformer | fp32 | adamw | False | 0 | 6105 | 5130 | 166337792 |
| grid-transformer-fp32-b6-s1536 | transformer | fp32 | adamw | False | 0 | 13408 | 12426 | 166337792 |
| grid-transformer-fp16-b2-s256 | transformer | fp16 | adamw | False | 0 | 1174 | 1032 | 91711232 |
| grid-transformer-fp16-b6-s256 | transformer | fp16 | adamw | False | 0 | 1278 | 1426 | 91711232 |
| grid-transformer-fp16-b2-s768 | transformer | fp16 | adamw | False | 0 | 1606 | 1426 | 91711232 |
| grid-transformer-fp16-b6-s768 | transformer | fp16 | adamw | False | 0 | 3640 | 2652 | 91711232 |
| grid-transformer-fp16-b2-s1536 | transformer | fp16 | adamw | False | 0 | 3552 | 2564 | 166337792 |
| grid-transformer-fp16-b6-s1536 | transformer | fp16 | adamw | False | 0 | 7086 | 6098 | 166337792 |
| grid-cnn-fp32-b64-i160 | cnn | fp32 | sgd | False | 0 | 2310 | 1324 | 33688 |
| grid-cnn-fp32-b24-i256 | cnn | fp32 | sgd | False | 0 | 1858 | 1076 | 33688 |
| grid-cnn-fp32-b8-i384 | cnn | fp32 | sgd | False | 0 | 2842 | 1856 | 82960 |
| grid-cnn-fp16-b64-i160 | cnn | fp16 | adamw | False | 0 | 1750 | 756 | 33688 |
| grid-cnn-fp16-b24-i256 | cnn | fp16 | adamw | False | 0 | 1722 | 728 | 33688 |
| grid-cnn-fp16-b8-i384 | cnn | fp16 | adamw | False | 0 | 1896 | 902 | 82960 |
| grid-pressure-transformer-pad8g | transformer | fp16 | adamw | False | 8192 | 11456 | 12558 | 381651200 |
| grid-pressure-transformer-pad18g | transformer | fp16 | adamw | False | 18432 | 23288 | 22798 | 381651200 |
| focus-cnn-fp32-b48-i224 | cnn | fp32 | sgd | False | 0 | 2632 | 2250 | 38328 |
| focus-cnn-fp32-b16-i320 | cnn | fp32 | sgd | False | 0 | 3258 | 2272 | 82960 |
| focus-cnn-fp16-b32-i224 | cnn | fp16 | adamw | False | 0 | 1840 | 846 | 38328 |
| focus-cnn-fp16-b12-i320 | cnn | fp16 | adamw | False | 0 | 1958 | 964 | 82960 |
| focus-cnn-fp16-b16-i384-pad6g | cnn | fp16 | adamw | False | 6144 | 8912 | 7918 | 82960 |
| focus-cnn-fp16-b16-i384-pad10g | cnn | fp16 | adamw | False | 10240 | 13008 | 12014 | 82960 |
| focus-mlp-fp32-b64-s1024-h2048 | mlp | fp32 | adamw | False | 0 | 988 | 448 | 20982784 |
| focus-mlp-fp16-b256-s1024-h2048 | mlp | fp16 | adamw | False | 0 | 1056 | 258 | 20982784 |
| focus-mlp-fp32-b128-s2048-h2048 | mlp | fp32 | sgd | False | 0 | 1032 | 298 | 33570816 |
| focus-mlp-fp16-b512-s2048-h2048 | mlp | fp16 | sgd | False | 0 | 1080 | 202 | 33570816 |
| focus-mlp-fp16-b128-s1024-pad8g | mlp | fp16 | adamw | False | 8192 | 9198 | 8258 | 4198400 |
| focus-mlp-fp16-b128-s1024-pad16g | mlp | fp16 | adamw | False | 16384 | 17390 | 16450 | 4198400 |
| overnight-cnn-resnet-fp32-b64-i160 | cnn | fp32 | sgd | False | 0 | 2076 | 1088 | 57032 |
| overnight-cnn-resnet-fp32-b32-i224 | cnn | fp32 | sgd | False | 0 | 2028 | 1084 | 57032 |
| overnight-cnn-resnet-fp32-b12-i320 | cnn | fp32 | sgd | False | 0 | 2110 | 1154 | 75848 |
| overnight-cnn-resnet-fp32-b8-i384 | cnn | fp32 | sgd | False | 0 | 2002 | 1046 | 75848 |
| overnight-cnn-resnet-fp16-b64-i160 | cnn | fp16 | adamw | False | 0 | 1482 | 562 | 57032 |
| overnight-cnn-resnet-fp16-b32-i224 | cnn | fp16 | adamw | False | 0 | 1536 | 534 | 57032 |
| overnight-cnn-resnet-fp16-b12-i320 | cnn | fp16 | adamw | False | 0 | 1522 | 576 | 75848 |
| overnight-cnn-resnet-fp16-b8-i384 | cnn | fp16 | adamw | False | 0 | 1594 | 592 | 75848 |
| realistic-cnn-resnet-fp32-b32-i224 | cnn | fp32 | sgd | False | 0 | 2756 | 1732 | 133720 |
| realistic-cnn-resnet-fp32-b12-i320 | cnn | fp32 | sgd | False | 0 | 2884 | 1848 | 175768 |
| realistic-cnn-resnet-fp16-b32-i224 | cnn | fp16 | adamw | False | 0 | 1954 | 910 | 133720 |
| realistic-cnn-resnet-fp16-b12-i320 | cnn | fp16 | adamw | False | 0 | 2024 | 980 | 175768 |
| realistic-cnn-efficientnet-fp32-b32-i224 | cnn | fp32 | sgd | False | 0 | 5042 | 4004 | 75352 |
| realistic-cnn-efficientnet-fp32-b12-i320 | cnn | fp32 | sgd | False | 0 | 5292 | 4254 | 95224 |
| realistic-cnn-efficientnet-fp16-b32-i224 | cnn | fp16 | adamw | False | 0 | 3080 | 2036 | 75352 |
| realistic-transformer-bert-fp32-ckpt0-b2-s1024 | transformer | fp32 | adamw | False | 0 | 5424 | 4392 | 166337792 |
| realistic-transformer-bert-fp16-ckpt1-b2-s1024 | transformer | fp16 | adamw | True | 0 | 3236 | 2198 | 166337792 |
| realistic-transformer-gpt-fp16-ckpt1-b4-s512 | transformer | fp16 | adamw | True | 0 | 2475 | 1430 | 91711232 |
| realistic-transformer-t5-fp16-ckpt0-b4-s512 | transformer | fp16 | adamw | False | 0 | 2551 | 1506 | 91711232 |
| realistic-mlp-fp32-sgd-b32-s4096-acc4 | mlp | fp32 | sgd | False | 0 | 1419 | 380 | 41961472 |
| realistic-mlp-fp16-adamw-b128-s1024-acc1 | mlp | fp16 | adamw | False | 0 | 1377 | 332 | 29375488 |
| realistic-pressure-gpt-fp16-pad16g | transformer | fp16 | adamw | False | 16384 | 21795 | 20750 | 381651200 |
| realistic-cnn-efficientnet-fp16-b12-i320 | cnn | fp16 | adamw | False | 0 | 3299 | 2200 | 95224 |
| realistic-cnn-convnext-fp32-b32-i224 | cnn | fp32 | sgd | False | 0 | 2487 | 1394 | 134088 |
| realistic-cnn-convnext-fp32-b12-i320 | cnn | fp32 | sgd | False | 0 | 2533 | 1440 | 173896 |
| realistic-cnn-convnext-fp16-b32-i224 | cnn | fp16 | adamw | False | 0 | 1861 | 762 | 134088 |
| realistic-cnn-convnext-fp16-b12-i320 | cnn | fp16 | adamw | False | 0 | 1885 | 786 | 173896 |
| realistic-transformer-bert-fp32-ckpt0-b4-s512 | transformer | fp32 | adamw | False | 0 | 4061 | 2974 | 91711232 |
| realistic-transformer-bert-fp32-ckpt1-b4-s512 | transformer | fp32 | adamw | True | 0 | 3929 | 2842 | 91711232 |
| realistic-transformer-bert-fp32-ckpt1-b2-s1024 | transformer | fp32 | adamw | True | 0 | 5297 | 4210 | 166337792 |
| realistic-transformer-bert-fp16-ckpt0-b4-s512 | transformer | fp16 | adamw | False | 0 | 2599 | 1506 | 91711232 |
| realistic-transformer-bert-fp16-ckpt0-b2-s1024 | transformer | fp16 | adamw | False | 0 | 3161 | 2068 | 166337792 |
| realistic-transformer-bert-fp16-ckpt1-b4-s512 | transformer | fp16 | adamw | True | 0 | 2521 | 1428 | 91711232 |
| realistic-transformer-gpt-fp32-ckpt0-b4-s512 | transformer | fp32 | adamw | False | 0 | 4063 | 2976 | 91711232 |
| realistic-transformer-gpt-fp32-ckpt0-b2-s1024 | transformer | fp32 | adamw | False | 0 | 5487 | 4400 | 166337792 |
| realistic-transformer-gpt-fp32-ckpt1-b4-s512 | transformer | fp32 | adamw | True | 0 | 3931 | 2844 | 91711232 |
| realistic-transformer-gpt-fp32-ckpt1-b2-s1024 | transformer | fp32 | adamw | True | 0 | 5047 | 3960 | 166337792 |
| realistic-transformer-gpt-fp16-ckpt0-b4-s512 | transformer | fp16 | adamw | False | 0 | 2599 | 1506 | 91711232 |
| realistic-transformer-gpt-fp16-ckpt0-b2-s1024 | transformer | fp16 | adamw | False | 0 | 3295 | 2202 | 166337792 |
| realistic-transformer-gpt-fp16-ckpt1-b2-s1024 | transformer | fp16 | adamw | True | 0 | 3145 | 2052 | 166337792 |
| realistic-transformer-t5-fp32-ckpt0-b4-s512 | transformer | fp32 | adamw | False | 0 | 4061 | 2974 | 91711232 |
| realistic-transformer-t5-fp32-ckpt0-b2-s1024 | transformer | fp32 | adamw | False | 0 | 5479 | 4392 | 166337792 |
| realistic-transformer-t5-fp32-ckpt1-b4-s512 | transformer | fp32 | adamw | True | 0 | 3929 | 2842 | 91711232 |
| realistic-transformer-t5-fp32-ckpt1-b2-s1024 | transformer | fp32 | adamw | True | 0 | 5297 | 4210 | 166337792 |
| realistic-transformer-t5-fp16-ckpt0-b2-s1024 | transformer | fp16 | adamw | False | 0 | 3161 | 2068 | 166337792 |
| realistic-transformer-t5-fp16-ckpt1-b4-s512 | transformer | fp16 | adamw | True | 0 | 2521 | 1428 | 91711232 |
| realistic-transformer-t5-fp16-ckpt1-b2-s1024 | transformer | fp16 | adamw | True | 0 | 3291 | 2198 | 166337792 |
| realistic-mlp-fp32-sgd-b128-s1024-acc1 | mlp | fp32 | sgd | False | 0 | 1357 | 270 | 29375488 |
| realistic-mlp-fp32-sgd-b64-s2048-acc2 | mlp | fp32 | sgd | False | 0 | 1391 | 304 | 33570816 |
| realistic-mlp-fp32-adamw-b128-s1024-acc1 | mlp | fp32 | adamw | False | 0 | 1705 | 618 | 29375488 |
| realistic-mlp-fp32-adamw-b64-s2048-acc2 | mlp | fp32 | adamw | False | 0 | 1759 | 672 | 33570816 |
| realistic-mlp-fp32-adamw-b32-s4096-acc4 | mlp | fp32 | adamw | False | 0 | 1915 | 828 | 41961472 |
| realistic-mlp-fp16-sgd-b128-s1024-acc1 | mlp | fp16 | sgd | False | 0 | 1245 | 152 | 29375488 |
| realistic-mlp-fp16-sgd-b64-s2048-acc2 | mlp | fp16 | sgd | False | 0 | 1301 | 208 | 33570816 |
| realistic-mlp-fp16-sgd-b32-s4096-acc4 | mlp | fp16 | sgd | False | 0 | 1319 | 226 | 41961472 |
| realistic-mlp-fp16-adamw-b64-s2048-acc2 | mlp | fp16 | adamw | False | 0 | 1521 | 428 | 33570816 |
| realistic-mlp-fp16-adamw-b32-s4096-acc4 | mlp | fp16 | adamw | False | 0 | 1579 | 486 | 41961472 |
| realistic-pressure-gpt-fp16-pad4g | transformer | fp16 | adamw | False | 4096 | 9555 | 8462 | 381651200 |
| realistic-pressure-gpt-fp16-pad8g | transformer | fp16 | adamw | False | 8192 | 13651 | 12558 | 381651200 |
| realistic-pressure-gpt-fp16-pad12g | transformer | fp16 | adamw | False | 12288 | 17747 | 16654 | 381651200 |
| iter2-repeat-bert-fp16-b2-s1024-ckpt1-r1 | transformer | fp16 | adamw | True | 0 | 3761 | 2198 | 166337792 |
| iter2-repeat-gpt-fp16-b4-s512-ckpt1-r1 | transformer | fp16 | adamw | True | 0 | 2993 | 1430 | 91711232 |
| iter2-repeat-efficientnet-fp16-b32-i224-r1 | cnn | fp16 | adamw | False | 0 | 3605 | 2036 | 75352 |
| iter2-repeat-mlp-fp32-adamw-b32-s4096-acc4-r1 | mlp | fp32 | adamw | False | 0 | 2385 | 828 | 41961472 |
| iter2-repeat-bert-fp16-b2-s1024-ckpt1-r2 | transformer | fp16 | adamw | True | 0 | 3761 | 2198 | 166337792 |
| iter2-repeat-gpt-fp16-b4-s512-ckpt1-r2 | transformer | fp16 | adamw | True | 0 | 2993 | 1430 | 91711232 |
| iter2-repeat-efficientnet-fp16-b32-i224-r2 | cnn | fp16 | adamw | False | 0 | 3605 | 2036 | 75352 |
| iter2-repeat-mlp-fp32-adamw-b32-s4096-acc4-r2 | mlp | fp32 | adamw | False | 0 | 2385 | 828 | 41961472 |
| iter2-repeat-bert-fp16-b2-s1024-ckpt1-r3 | transformer | fp16 | adamw | True | 0 | 3761 | 2198 | 166337792 |
| iter2-repeat-gpt-fp16-b4-s512-ckpt1-r3 | transformer | fp16 | adamw | True | 0 | 2993 | 1430 | 91711232 |
| iter2-repeat-efficientnet-fp16-b32-i224-r3 | cnn | fp16 | adamw | False | 0 | 3605 | 2036 | 75352 |
| iter2-repeat-mlp-fp32-adamw-b32-s4096-acc4-r3 | mlp | fp32 | adamw | False | 0 | 2385 | 828 | 41961472 |
| iter2-long-bert-fp32-ckpt0-b1-s2048 | transformer | fp32 | adamw | False | 0 | 5975 | 4418 | 166337792 |
| iter2-long-bert-fp16-ckpt0-b1-s2048 | transformer | fp16 | adamw | False | 0 | 3749 | 2186 | 166337792 |
| iter2-long-bert-fp16-ckpt1-b1-s2048 | transformer | fp16 | adamw | True | 0 | 3619 | 2056 | 166337792 |
| iter2-long-gpt-fp32-ckpt0-b1-s2048 | transformer | fp32 | adamw | False | 0 | 5895 | 4338 | 166337792 |
| iter2-long-gpt-fp16-ckpt0-b1-s2048 | transformer | fp16 | adamw | False | 0 | 3631 | 2068 | 166337792 |
| iter2-long-gpt-fp16-ckpt1-b1-s2048 | transformer | fp16 | adamw | True | 0 | 3741 | 2178 | 166337792 |
| iter2-pressure-gpt-fp16-pad18g | transformer | fp16 | adamw | False | 18432 | 23817 | 22798 | 381651200 |
| iter2-pressure-gpt-fp16-pad19g | transformer | fp16 | adamw | False | 19456 | 24069 | 23822 | 381651200 |
| iter2-pressure-gpt-fp16-pad20g | transformer | fp16 | adamw | False | 20480 | 24089 | 24846 | 381651200 |
| iter2-pressure-gpt-fp16-pad21g | transformer | fp16 | adamw | False | 21504 | 24095 | 25870 | 381651200 |
| iter2-pressure-gpt-fp16-pad22g | transformer | fp16 | adamw | False | 22528 | 24083 | 26894 | 381651200 |
| smoke-mlp | mlp | fp32 | adamw | False | 0 | 1207 | 94 | 2624768 |
| cov-xf-fp32-b4-s512-h1024-l10 | transformer | fp32 | adamw | False | 0 | 5183 | 4980 | 191530240 |
| cov-xf-fp32-b3-s1024-h1280-l12 | transformer | fp32 | adamw | False | 0 | 10511 | 9398 | 318081280 |
| cov-xf-fp32-b1-s2048-h1536-l12 | transformer | fp32 | adamw | False | 0 | 11095 | 9982 | 438314240 |
| cov-xf-fp32-b8-s384-h768-l8 | transformer | fp32 | adamw | False | 0 | 5481 | 4368 | 105886976 |
| cov-xf-fp16-b4-s512-h1024-l10 | transformer | fp16 | adamw | False | 0 | 3573 | 2454 | 191530240 |
| cov-xf-fp16-b3-s1024-h1280-l12 | transformer | fp16 | adamw | False | 0 | 4635 | 4458 | 318081280 |
| cov-xf-fp16-b1-s2048-h1536-l12 | transformer | fp16 | adamw | False | 0 | 6103 | 4984 | 438314240 |
| cov-xf-fp16-b8-s384-h768-l8 | transformer | fp16 | adamw | False | 0 | 1363 | 2160 | 105886976 |
| cov-xf-bf16-b4-s512-h1024-l10 | transformer | bf16 | adamw | False | 0 | 1589 | 2454 | 191530240 |
| cov-xf-bf16-b3-s1024-h1280-l12 | transformer | bf16 | adamw | False | 0 | 4109 | 4458 | 318081280 |
| cov-xf-bf16-b1-s2048-h1536-l12 | transformer | bf16 | adamw | False | 0 | 4259 | 4984 | 438314240 |
| cov-xf-bf16-b8-s384-h768-l8 | transformer | bf16 | adamw | False | 0 | 1365 | 2160 | 105886976 |
| cov-cnn-fp32-b48-i192-h160-l10 | cnn | fp32 | sgd | False | 0 | 2909 | 1792 | 54140 |
| cov-cnn-fp32-b16-i320-h192-l14 | cnn | fp32 | sgd | False | 0 | 3377 | 2572 | 93376 |
| cov-cnn-fp32-b96-i128-h96-l8 | cnn | fp32 | sgd | False | 0 | 2389 | 1272 | 33688 |
| cov-cnn-fp32-b6-i448-h224-l16 | cnn | fp32 | sgd | False | 0 | 3863 | 2746 | 136044 |
| cov-cnn-fp16-b48-i192-h160-l10 | cnn | fp16 | sgd | False | 0 | 1975 | 1016 | 54140 |
| cov-cnn-fp16-b16-i320-h192-l14 | cnn | fp16 | sgd | False | 0 | 2535 | 1410 | 93376 |
| cov-cnn-fp16-b96-i128-h96-l8 | cnn | fp16 | sgd | False | 0 | 1633 | 626 | 33688 |
| cov-cnn-fp16-b6-i448-h224-l16 | cnn | fp16 | sgd | False | 0 | 2345 | 1522 | 136044 |
| cov-cnn-bf16-b48-i192-h160-l10 | cnn | bf16 | sgd | False | 0 | 2281 | 1156 | 54140 |
| cov-cnn-bf16-b16-i320-h192-l14 | cnn | bf16 | sgd | False | 0 | 2535 | 1410 | 93376 |
| cov-cnn-bf16-b96-i128-h96-l8 | cnn | bf16 | sgd | False | 0 | 1639 | 626 | 33688 |
| cov-cnn-bf16-b6-i448-h224-l16 | cnn | bf16 | sgd | False | 0 | 2647 | 1522 | 136044 |
| cov-mlp-fp32-b256-h4096-l6 | mlp | fp32 | adamw | False | 0 | 2589 | 1468 | 71324160 |
| cov-mlp-fp32-b512-h2048-l8 | mlp | fp32 | adamw | False | 0 | 1689 | 568 | 27277824 |
| cov-mlp-fp32-b128-h8192-l4 | mlp | fp32 | adamw | False | 0 | 3889 | 2768 | 142631424 |
| cov-mlp-fp32-b1024-h1024-l10 | mlp | fp32 | adamw | False | 0 | 1115 | 242 | 9446912 |
| cov-mlp-fp16-b256-h4096-l6 | mlp | fp16 | adamw | False | 0 | 1319 | 724 | 71324160 |
| cov-mlp-fp16-b512-h2048-l8 | mlp | fp16 | adamw | False | 0 | 1413 | 348 | 27277824 |
| cov-mlp-fp16-b128-h8192-l4 | mlp | fp16 | adamw | False | 0 | 2531 | 1404 | 142631424 |
| cov-mlp-fp16-b1024-h1024-l10 | mlp | fp16 | adamw | False | 0 | 1261 | 134 | 9446912 |
| cov-mlp-bf16-b256-h4096-l6 | mlp | bf16 | adamw | False | 0 | 1851 | 724 | 71324160 |
| cov-mlp-bf16-b512-h2048-l8 | mlp | bf16 | adamw | False | 0 | 1253 | 348 | 27277824 |
| cov-mlp-bf16-b128-h8192-l4 | mlp | bf16 | adamw | False | 0 | 2531 | 1404 | 142631424 |
| cov-mlp-bf16-b1024-h1024-l10 | mlp | bf16 | adamw | False | 0 | 1261 | 134 | 9446912 |
| cov-transformer-ckpt-fp16 | transformer | fp16 | adamw | True | 0 | 6293 | 5166 | 357436160 |
| cov-cnn-ckpt-fp16 | cnn | fp16 | adamw | True | 0 | 3339 | 2206 | 103792 |
| oom-pressure-xf-fp16-res22000 | transformer | fp16 | adamw | False | 22000 | 24047 | 27010 | 438314240 |
| oom-pressure-xf-fp16-res24000 | transformer | fp16 | adamw | False | 24000 | 24049 | 29010 | 438314240 |
| oom-pressure-xf-fp16-res26000 | transformer | fp16 | adamw | False | 26000 | 24045 | 31010 | 438314240 |
| oom-pressure-xf-fp16-res28000 | transformer | fp16 | adamw | False | 28000 | 24043 | 33010 | 438314240 |
| rf-resnet18-fp16-b128-i224 | cnn | fp16 | sgd | False | 0 | 3968 | 2558 | 11689512 |
| rf-resnet50-fp32-b32-i224 | cnn | fp32 | sgd | False | 0 | 4703 | 3286 | 25557032 |
| rf-resnet50-fp16-b64-i224 | cnn | fp16 | sgd | False | 0 | 4711 | 3264 | 25557032 |
| rf-convnext-tiny-fp16-b32-i224 | cnn | fp16 | adamw | False | 0 | 3547 | 2100 | 28589128 |
| rf-vit-b16-fp16-b32-i224 | cnn | fp16 | adamw | False | 0 | 4162 | 2718 | 86567656 |
