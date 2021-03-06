import json
from pathlib import Path

import pytest
from devtools import debug
from sqlmodel import select

from fractal.server.app.models import Subtask
from fractal.server.app.models import Task
from fractal.server.app.models import TaskRead
from fractal.server.app.runner import _atomic_task_factory
from fractal.server.app.runner import _process_workflow
from fractal.server.app.runner import submit_workflow


LEN_NONTRIVIAL_WORKFLOW = 3


@pytest.fixture
def nontrivial_workflow():
    workflow = Task(
        name="outer workflow",
        resource_type="workflow",
        subtask_list=[
            Subtask(
                subtask=Task(
                    name="inner workflow",
                    resource_type="workflow",
                    subtask_list=[
                        Subtask(
                            args={"message": "dummy0"},
                            subtask=Task(
                                name="dummy0",
                                module="fractal.tasks.dummy:dummy",
                                default_args=dict(message="dummy0"),
                            ),
                        ),
                        Subtask(
                            args={"message": "dummy1"},
                            subtask=Task(
                                name="dummy1",
                                module="fractal.tasks.dummy:dummy",
                                default_args=dict(message="dummy1"),
                            ),
                        ),
                    ],
                ),
            ),
            Subtask(
                subtask=Task(
                    name="dummy2",
                    module="fractal.tasks.dummy:dummy",
                    default_args=dict(message="dummy2"),
                )
            ),
        ],
    )
    return workflow


N_INDICES = 3
DUMMY_MESSAGE = "dummy task message"
DUMMY_SUBTASK_MESSAGE = "dummy subtask message"


dummy_task = Task(
    name="dummy",
    resource_type="core task",
    module="fractal.tasks.dummy:dummy",
    default_args={"message": DUMMY_MESSAGE},
)
dummy_subtask = Subtask(
    subtask=dummy_task, args={"message": DUMMY_SUBTASK_MESSAGE}
)
dummy_subtask_parallel = Subtask(
    subtask=dummy_task,
    args={"message": DUMMY_SUBTASK_MESSAGE, "parallelization_level": "index"},
)


@pytest.mark.parametrize(
    ("task", "message", "nfiles"),
    [
        (dummy_task, DUMMY_MESSAGE, 1),
        (dummy_subtask, DUMMY_SUBTASK_MESSAGE, 1),
        (dummy_subtask_parallel, DUMMY_SUBTASK_MESSAGE, N_INDICES),
    ],
)
def test_atomic_task_factory(task, message, nfiles, tmp_path):
    """
    GIVEN
        * a task or subtask
        * input_path iterable
        * output_path
    WHEN passed to the task factory
    THEN
        * the relative PARSL workflow is correctly
        * it can run
        * the output is as expected
    """
    input_path_str = "/input/path"
    output_path = tmp_path
    metadata = {"index": list(range(N_INDICES))}

    parsl_app = _atomic_task_factory(
        task=task,
        input_paths=[Path(input_path_str)],
        output_path=output_path,
        metadata=metadata,
    )

    debug(parsl_app)
    res = parsl_app.result()
    debug(res)
    assert res

    assert sum(1 for item in output_path.glob("*.json")) == nfiles

    if not isinstance(res, list):
        res = [res]

    for r in res:
        with open(r, "r") as output_file:
            data = json.load(output_file)
            debug(data)
            assert len(data) == 1
            assert data[0]["message"] == message


def test_preprocess_workflow(nontrivial_workflow):
    """
    GIVEN a workflow with nested tasks
    WHEN the workflow is preprocessed
    THEN
        * the workflow is correctly unwrapped into a list
        * the order of the tasks is correctly preserved
    """
    workflow = nontrivial_workflow

    debug(workflow.preprocess())
    preprocessed_workflow = workflow.preprocess()
    for i, preprocessed_task in enumerate(preprocessed_workflow):
        assert str(i) in preprocessed_task.name

    assert i + 1 == LEN_NONTRIVIAL_WORKFLOW


def test_process_workflow(tmp_path, nontrivial_workflow):
    """
    GIVEN a nontrivial workflow
    WHEN the workflow is processed
    THEN
        * a single PARSL python_app which will execute the workflow is produced
        * it is executable
        * the output is the one expected from the workflow
    """
    app = _process_workflow(
        task=nontrivial_workflow,
        input_paths=[tmp_path / "0.json"],
        output_path=tmp_path / "0.json",
    )
    debug(app)
    app.result()

    print(list(tmp_path.glob("*.json")))
    for f in tmp_path.glob("*.json"):
        with open(f, "r") as output_file:
            data = json.load(output_file)
            debug(data)
    assert len(data) == LEN_NONTRIVIAL_WORKFLOW
    assert data[0]["message"] == "dummy0"
    assert data[1]["message"] == "dummy1"
    assert data[2]["message"] == "dummy2"


@pytest.mark.xfail
async def test_apply_workflow(
    db,
    client,
    collect_tasks,
    MockCurrentUser,
    project_factory,
    dataset_factory,
    resource_factory,
    task_factory,
):
    """
    GIVEN
        * an input dataset and relative resource(s)
        * an output dataset and relative resource
        * a non-trivial workflow
    WHEN one applys the workflow to the input dataset
    THEN
        * the workflow is executed correctly
        * the output is correctly written in the output resource
    """

    # CREATE RESOURCES
    async with MockCurrentUser(persist=True) as user:
        prj = await project_factory(user)
        ds = await dataset_factory(prj, type="image")
        out_ds = await dataset_factory(prj, type="image", name="out_ds")

        resource = await resource_factory(ds)
        resource = await resource_factory(out_ds)

        debug(ds)
        debug(resource)

    # CREATE NONTRIVIAL WORKFLOW
    wf = await task_factory(
        name="worfklow",
        module=None,
        resource_type="workflow",
        input_type="image",
    )
    debug(wf)

    stm = select(Task).where(Task.name == "dummy")
    res = await db.execute(stm)
    dummy_task = res.scalar()

    await wf.add_subtask(db, subtask=dummy_task)
    debug(TaskRead.from_orm(wf))

    await submit_workflow(input_dataset=ds, output_dataset=out_ds, workflow=wf)
