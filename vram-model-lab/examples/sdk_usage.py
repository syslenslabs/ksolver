import os

import torch.nn as nn

import ksolver_vram_profile as ks


model = nn.Sequential(nn.Linear(1024, 4096), nn.GELU(), nn.Linear(4096, 1024))

ks.report_training_job(
    model=model,
    framework="pytorch",
    model_name="example-mlp",
    precision="fp16",
    batch_size=64,
    sequence_length=1024,
    optimizer="adamw",
    distributed_strategy="single-gpu",
    output_path=os.environ.get("KSOLVER_PROFILE_OUTPUT", ks.DEFAULT_OUTPUT),
)
