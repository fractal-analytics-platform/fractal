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
import itertools
import os

import parsl
import pytest
from parsl.addresses import address_by_hostname
from parsl.app.app import bash_app
from parsl.app.app import python_app
from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.data_provider.files import File
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider


try:
    import tensorflow as tf

    HAS_TENSORFLOW = True
except ImportError:
    HAS_TENSORFLOW = False


skiif_no_tensorflow_gpu = pytest.mark.skipif(
    not (HAS_TENSORFLOW and tf.config.list_physical_devices("GPU")),
    reason="PyTorch could not be imported or GPU not available",
)


def initialize_SlurmProvider():
    slurm = SlurmProvider(
        channel=LocalChannel(),
        nodes_per_block=1,
        cores_per_node=16,
        mem_per_node=1,  # specified in GB
        parallelism=0,
        partition="main",
        worker_init=(
            "source /opt/easybuild/software/Anaconda3/2019.07/"
            "etc/profile.d/conda.sh\n"
            "conda activate fractal"
        ),
        launcher=SrunLauncher(debug=True),
        walltime="01:00:00",
        cmd_timeout=60,
    )
    return slurm


def initialize_HighThroughputExecutor(provider=None, max_workers=10):
    assert provider is not None
    htex = HighThroughputExecutor(
        label="htex",
        address=address_by_hostname(),
        worker_debug=True,
        max_workers=max_workers,
        provider=provider,
    )
    return htex


def AUX_single_app(provider):
    htex = initialize_HighThroughputExecutor(provider=provider)
    config = Config(executors=[htex])
    parsl.clear()
    parsl.load(config)

    @python_app
    def increment_by_one(num):
        return num + 1

    N = 3
    apps = []
    for i in range(N):
        apps.append(increment_by_one(i))
    incremented_numbers = [app.result() for app in apps]
    print(f"AUX_single_app: {incremented_numbers}")
    assert incremented_numbers == [i + 1 for i in range(N)]


def AUX_workflow_generate_combine_split(provider):
    htex = initialize_HighThroughputExecutor(provider=provider)
    config = Config(executors=[htex])
    parsl.clear()
    parsl.load(config)

    # Generate a file with three numbers (well, channel, randint(0, 100))
    @python_app
    def task_generate(inputs=[], outputs=[]):
        import random

        with open(outputs[0], "w") as out:
            random_number = random.randint(0, 100)
            out.write(f"{inputs[0]} {inputs[1]} {random_number}\n")

    # Combine input files into a single output file
    @bash_app
    def task_combine(inputs=[], outputs=[]):
        return "cat {0} > {1}".format(
            " ".join([i.filepath for i in inputs]), outputs[0]
        )

    # Scan input file, find relevant line, write it on output file
    @python_app
    def task_split(inputs=[], outputs=[]):
        with open(inputs[0], "r") as f1:
            for line in f1:
                w, c, num = [int(i) for i in line.split()]
                if w == inputs[1] and c == inputs[2]:
                    with open(outputs[0], "w") as f2:
                        f2.write(f"{w} {c} {num}\n")

    # Create files
    if not os.path.isdir("tmp_data"):
        os.makedirs("tmp_data")
    n_wells = 2
    n_channels = 2
    output_files = []
    for well, channel in itertools.product(range(n_wells), range(n_channels)):
        output_files.append(
            task_generate(
                inputs=[well, channel],
                outputs=[
                    File(
                        os.path.join(
                            os.getcwd(),
                            f"tmp_data/file-w{well}-c{channel}.txt",
                        )
                    )
                ],
            )
        )

    # Concatenate the files into a single file
    cc = task_combine(
        inputs=[i.outputs[0] for i in output_files],
        outputs=[
            File(os.path.join(os.getcwd(), "tmp_data/combined_data.txt"))
        ],
    )

    # Split the single file back into many files
    inputs = []
    outputs = []
    for well, channel in itertools.product(range(n_wells), range(n_channels)):
        inputs.append([cc.outputs[0], well, channel])
        outputs.append(
            [
                File(
                    os.path.join(
                        os.getcwd(),
                        f"tmp_data/processed_file-w{well}-c{channel}.txt",
                    )
                )
            ]
        )

    split = [
        task_split(inputs=inputs[i], outputs=outputs[i])
        for i in range(len(inputs))
    ]
    [x.result() for x in split]

    # Verify that all files are generated
    for well, channel in itertools.product(range(n_wells), range(n_channels)):
        assert os.path.isfile(
            os.path.join(os.getcwd(), f"tmp_data/file-w{well}-c{channel}.txt")
        )
    assert os.path.isfile(
        os.path.join(os.getcwd(), "tmp_data/combined_data.txt")
    )
    for well, channel in itertools.product(range(n_wells), range(n_channels)):
        assert os.path.isfile(
            os.path.join(
                os.getcwd(), f"tmp_data/processed_file-w{well}-c{channel}.txt"
            )
        )


def AUX_import_numpy(provider, providername):
    htex = initialize_HighThroughputExecutor(provider=provider)
    config = Config(executors=[htex])
    parsl.clear()
    parsl.load(config)

    @python_app
    def importnumpy():
        import os

        os.environ["OPENBLAS_NUM_THREADS"] = "10"
        import sys
        import socket
        import numpy

        hostname = socket.gethostname()
        python_version = sys.version
        numpy_version = numpy.__version__
        info = (
            f"{hostname}\n"
            + f"python version: {python_version}\n'"
            + f"numpy version: {numpy_version}\n"
        )
        return info

    app = importnumpy()
    info = app.result()

    if not os.path.isdir("tmp_data"):
        os.makedirs("tmp_data")
    with open(
        os.path.join(os.getcwd(), f"tmp_data/info_{providername}.txt"), "w"
    ) as out:
        out.write(info)


def AUX_workflow_compute_pi(provider):
    htex = initialize_HighThroughputExecutor(provider=provider)
    config = Config(executors=[htex])
    parsl.clear()
    parsl.load(config)

    @python_app
    def compute_pi(num_points, seed=0):
        import random

        random.seed(seed)
        inside = 0
        for i in range(num_points):
            x, y = (
                random.random(),
                random.random(),
            )  # Drop a random point in the box.
            if x**2 + y**2 < 1:  # Count points within the circle.
                inside += 1
        return inside * 4 / num_points

    @python_app
    def average(inputs=[]):
        return sum(inputs) / len(inputs)

    n_tasks = 10
    tasks_compute_pi = [compute_pi(10**6, seed=i) for i in range(n_tasks)]
    task_compute_average = average(inputs=tasks_compute_pi)
    av = task_compute_average.result()

    import numpy

    assert abs(av - numpy.pi) < 0.01


def test_single_app_slurm():
    fmt = "%8i %.12u %.10a %.30j %.8t %.10M %.10l %.4C %.10m %R %E"
    os.environ["SQUEUE_FORMAT"] = fmt
    provider = initialize_SlurmProvider()
    AUX_single_app(provider)


def test_workflow_generate_combine_split_slurm():
    fmt = "%8i %.12u %.10a %.30j %.8t %.10M %.10l %.4C %.10m %R %E"
    os.environ["SQUEUE_FORMAT"] = fmt
    provider = initialize_SlurmProvider()
    AUX_workflow_generate_combine_split(provider)


def test_workflow_compute_pi_slurm():
    fmt = "%8i %.12u %.10a %.30j %.8t %.10M %.10l %.4C %.10m %R %E"
    os.environ["SQUEUE_FORMAT"] = fmt
    provider = initialize_SlurmProvider()
    AUX_workflow_compute_pi(provider)


def test_import_numpy_slurm():
    fmt = "%8i %.12u %.10a %.30j %.8t %.10M %.10l %.4C %.10m %R %E"
    os.environ["SQUEUE_FORMAT"] = fmt
    provider = initialize_SlurmProvider()
    AUX_import_numpy(provider, "SlurmProvider")


@skiif_no_tensorflow_gpu
def test_use_tensorflow_on_gpu():
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
    def increment_by_one(num):
        os.environ["OPENBLAS_NUM_THREADS"] = "1"
        import tensorflow as tf

        gpus = tf.config.list_physical_devices("GPU")
        with open("output_gpus.txt", "w") as out:
            out.write(f"{gpus}\n")
        return num + 1

    two = increment_by_one(1).result()
    print(f"two: {two}")
    assert two == 2


@skiif_no_tensorflow_gpu
def test_multiexecutor_workflow():
    fmt = "%8i %.12u %.10a %.30j %.8t %.10M %.10l %.4C %.10m %R %E"
    os.environ["SQUEUE_FORMAT"] = fmt
    slurm_cpu = SlurmProvider(
        partition="main",
        channel=LocalChannel(),
        launcher=SrunLauncher(debug=True),
    )
    slurm_gpu = SlurmProvider(
        partition="gpu",
        channel=LocalChannel(),
        launcher=SrunLauncher(debug=True),
    )
    htex_cpu = HighThroughputExecutor(
        address=address_by_hostname(),
        label="htex_cpu",
        worker_debug=True,
        provider=slurm_cpu,
    )
    htex_gpu = HighThroughputExecutor(
        address=address_by_hostname(),
        label="htex_gpu",
        worker_debug=True,
        provider=slurm_gpu,
    )
    config = Config(executors=[htex_cpu, htex_gpu])
    parsl.clear()
    parsl.load(config)

    @python_app(executors=["htex_gpu"])
    def increment_by_one_gpu(num):
        os.environ["OPENBLAS_NUM_THREADS"] = "1"
        import tensorflow as tf

        gpus = tf.config.list_physical_devices("GPU")
        with open(
            "output_gpus_from_test_multiexecutor_workflow.txt", "w"
        ) as out:
            out.write(f"{gpus}\n")
        return num + 1

    @python_app(executors=["htex_cpu"])
    def increment_by_one_cpu(num):
        return num + 1

    one = increment_by_one_cpu(0)
    two = increment_by_one_gpu(one).result()
    print(f"two: {two}")
    assert two == 2


if __name__ == "__main__":
    test_single_app_slurm()
    test_workflow_generate_combine_split_slurm()
    test_workflow_compute_pi_slurm()
    test_import_numpy_slurm()
    test_use_tensorflow_on_gpu()
    test_multiexecutor_workflow()
