"""
Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
University of Zurich

Original authors:
Tommaso Comparin <tommaso.comparin@exact-lab.it>
Marco Franzon <marco.franzon@exact-lab.it>
Jacopo Nespolo <jacopo.nespolo@exact-lab.it>

This file is part of Fractal and was originally developed by eXact lab S.r.l.
<exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
Institute for Biomedical Research and Pelkmans Lab from the University of
Zurich.
"""
import os

import parsl
import pytest
from parsl.addresses import address_by_hostname
from parsl.app.app import python_app
from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider

try:
    import torch

    HAS_TORCH = True
except ImportError:
    HAS_TORCH = False


@pytest.mark.skipif(
    not (HAS_TORCH and torch.cuda.is_available()),
    reason="PyTorch could not be imported or GPU not available",
)
def test_use_cellpose_on_gpu():

    fmt = "%8i %.12u %.10a %.30j %.8t %.10M %.10l %.4C %.10m %R %E"
    os.environ["SQUEUE_FORMAT"] = fmt
    slurm = SlurmProvider(
        partition="gpu",
        channel=LocalChannel(),
        launcher=SrunLauncher(debug=True),
    )
    htex = HighThroughputExecutor(
        address=address_by_hostname(), worker_debug=True, provider=slurm
    )
    config = Config(executors=[htex])
    parsl.clear()
    parsl.load(config)

    @python_app
    def dummy_app():

        os.environ["OPENBLAS_NUM_THREADS"] = "1"
        import torch
        from cellpose import core

        # Write torch info
        info = (
            f"torch.cuda.is_available(): {torch.cuda.is_available()}\n"
            f"torch.cuda.device_count(): {torch.cuda.device_count()}\n"
            f"torch.cuda.current_device(): {torch.cuda.current_device()}\n"
            f"torch.cuda.device(0): {torch.cuda.device(0)}\n"
            "torch.cuda.get_device_name(0): "
            f"{torch.cuda.get_device_name(0)}\n"
        )
        with open("output_torch.dat", "w") as out:
            out.write(info)

        # Write cellpose info
        gpu0 = core.use_gpu(gpu_number=0)
        with open("output_cellpose.dat", "w") as out:
            out.write(f"cellpose.core.use_gpu(): {gpu0}")
        assert gpu0

    future = dummy_app()
    future.result()


if __name__ == "__main__":
    test_use_cellpose_on_gpu()
