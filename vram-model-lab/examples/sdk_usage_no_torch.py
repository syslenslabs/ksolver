import os

import ksolver_vram_profile as ks


class FakeParam:
    def __init__(self, count):
        self.count = count

    def numel(self):
        return self.count


class FakeModel:
    def parameters(self):
        return [FakeParam(1024 * 4096), FakeParam(4096), FakeParam(4096 * 1024), FakeParam(1024)]


ks.report_training_job(
    model=FakeModel(),
    framework="pytorch",
    model_name="example-mlp-no-torch",
    precision="fp16",
    batch_size=64,
    sequence_length=1024,
    optimizer="adamw",
    distributed_strategy="single-gpu",
    output_path=os.environ.get("KSOLVER_PROFILE_OUTPUT", ks.DEFAULT_OUTPUT),
)
