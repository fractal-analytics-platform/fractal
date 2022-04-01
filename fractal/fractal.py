import json
import os
from pathlib import Path
from subprocess import PIPE
from subprocess import Popen
from typing import List
from typing import Optional

import click
from devtools import debug  # type: ignore
from pydantic import BaseModel


"""
#fractal.json
{
    "fractal":{
            "version": "0.1",
            "projects":{project_name : {path: path, datasets: []}
                        },
            "tasks" : {}
            "users" : {},
            "groups": {},
             }
}

#project_name.json
{dataset_name : {resources: [],
                 type: type
                },
 "workflows" : [name : {}]
}
"""


class TaskModel(BaseModel):
    name: str
    depends_on: Optional[str]
    input_type: str
    output_type: str


class Workflow(BaseModel):
    tasks: List[TaskModel]


def db_load():
    """
    Mocks read from global fractal database
    """
    with open("./fractal.json", "r") as f:
        db = json.load(f)
        return db


def db_save(db):
    """
    Mocks writes to the global fractal database
    """
    with open("./fractal.json", "w") as f:
        json.dump(db, f)


def get_project(project_name):
    """
    Loads a single project from the mock database
    """
    db = db_load()
    project_path = Path(db["fractal"]["projects"][project_name]["path"])
    ds_names = db["fractal"]["projects"][project_name]["datasets"]
    return project_path, ds_names


def project_file_load(project_name):
    """
    Loads a project file
    """
    project_path, ds_names = get_project(project_name)
    pj_file = project_name + ".json"
    with open(project_path / pj_file, "r") as f:
        prj = json.load(f)
    return prj, ds_names


def get_ds_names(project_name):
    """
    Returns datasets information from the project file
    """
    prj, ds_names = project_file_load(project_name)
    ds = prj["datasets"]
    return ds_names, ds


def save_project_file(project_name, prj_dict):
    """
    Writes project file
    """
    project_path, ds_names = get_project(project_name)
    pj_file = project_name + ".json"
    with open(project_path / pj_file, "w") as f:
        json.dump(prj_dict, f)


def check_I_O(task_p, task_n):
    """
    Checks compatibility of tasks input/output
    """
    return task_p["output_type"] == task_n["input_type"]


@click.group()
def cli():
    pass


# ###################PROJECT#########


@cli.group()
def project():
    pass


@project.command(name="new")
@click.argument("project_name", required=True, nargs=1)
@click.argument("path", required=True, nargs=1)
@click.argument("dataset", required=True, nargs=1)
def project_new(project_name, path, dataset):
    path_obj = Path(path)
    if not path_obj.exists():
        try:
            os.mkdir(path)
        except OSError:
            print(f"Creation of the directory {path} failed")
        else:
            print(f"Successfully created the directory {path}")

    if not os.path.isfile(os.getcwd() + "/" + "fractal.json"):
        with open(os.getcwd() + "/" + "fractal.json", "w") as f:
            json.dump(
                {
                    "fractal": {
                        "version": "0.1",
                        "projects": {
                            project_name: {"path": path, "datasets": [dataset]}
                        },
                        "tasks": {},
                        "users": {},
                        "groups": {},
                    }
                },
                f,
            )

    db = db_load()

    db["fractal"]["projects"].update(
        {
            project_name: {
                "path": path_obj.resolve().as_posix(),
                "datasets": [dataset],
                "user": "",
            }
        }
    )

    pj_file = project_name + ".json"
    if not os.path.isfile(path_obj / pj_file):
        with open(path_obj / pj_file, "w") as f:
            json.dump(
                {
                    "datasets": {
                        dataset: {
                            "resources": [],
                            "type": "",
                        },
                    },
                    "workflows": {},
                },
                f,
            )

    db_save(db)


@project.command(name="list")
def projects_list():
    db = db_load()
    debug(db["fractal"]["projects"])


# #################DATASET##########


@cli.group()
def dataset():
    pass


@dataset.command(name="list")
@click.argument("project_name", required=True, nargs=1)
def datasets_list(project_name):
    ds_names, ds = get_ds_names(project_name)
    debug(ds_names, ds)


@dataset.command(name="new")
@click.argument("project_name", required=True, nargs=1)
@click.argument("dataset_name", required=True, nargs=1)
@click.argument("resources", required=True, nargs=-1)
@click.argument("ds_type", required=True, nargs=1)
def dataset_new(project_name, dataset_name, resources, ds_type):
    prj, _ = project_file_load(project_name)
    prj["datasets"].update(
        {dataset_name: {"resources": resources, "type": ds_type}}
    )
    db = db_load()
    db["fractal"]["projects"][project_name]["datasets"].append(dataset_name)
    db_save(db)
    save_project_file(project_name, prj)


@dataset.command(name="add-resources")
@click.argument("project_name", required=True, nargs=1)
@click.argument("dataset_name", required=True, nargs=1)
@click.argument("resources", required=True, nargs=-1)
def datasets_add_resources(project_name, dataset_name, resources):

    prj, _ = project_file_load(project_name)
    prj["datasets"][dataset_name]["resources"].extend(resources)
    save_project_file(project_name, prj)


@dataset.command(name="update-type")
@click.argument("project_name", required=True, nargs=1)
@click.argument("dataset_name", required=True, nargs=1)
@click.argument("ds_type", required=True, nargs=1)
def dataset_update_type(project_name, dataset_name, ds_type):

    prj, _ = project_file_load(project_name)
    prj["datasets"][dataset_name]["type"] = ds_type
    save_project_file(project_name, prj)


# ################TASK##################
@cli.group()
def task():
    pass


@task.command(name="list")
def task_list():
    db = db_load()
    debug(db["fractal"]["tasks"])


@task.command(name="add")
@click.argument("task_name", required=True, nargs=1)
@click.argument("input_type", required=True, nargs=1)
@click.argument("output_type", required=True, nargs=1)
@click.argument("depends_on", required=False)
def task_add(task_name, input_type, output_type, depends_on=None):
    db = db_load()
    db["fractal"]["tasks"][task_name] = TaskModel(
        name=task_name,
        input_type=input_type,
        output_type=output_type,
        depends_on=depends_on,
    ).dict()
    db_save(db)


# ########################WORKFLOW######


@cli.group()
def workflow():
    pass


@workflow.command(name="list")
@click.argument("project_name", required=True, nargs=1)
def workflow_list(project_name):
    prj, _ = project_file_load(project_name)
    debug(prj["workflows"])


@workflow.command(name="new")
@click.argument("project_name", required=True, nargs=1)
@click.argument("workflow_name", required=True, nargs=1)
@click.argument("tasks", required=True, nargs=-1)
def workflow_new(project_name, workflow_name, tasks):

    db = db_load()
    if len(tasks) > 1:
        task_p = db["fractal"]["tasks"][tasks[0]]
        for task_n in db["fractal"]["tasks"][tasks[1:]]:
            if not check_I_O(task_p, task_n):
                raise
            task_p = task_n

    prj, _ = project_file_load(project_name)

    prj["workflows"].update(
        {
            workflow_name: Workflow(
                tasks=[
                    db["fractal"]["tasks"][task_name] for task_name in tasks
                ],
            ).dict()
        }
    )
    save_project_file(project_name, prj)


@workflow.command(name="add-task")
@click.argument("project_name", required=True, nargs=1)
@click.argument("workflow_name", required=True, nargs=1)
@click.argument("tasks", required=True, nargs=-1)
def workflow_add_task(project_name, workflow_name, tasks):

    db = db_load()
    prj, _ = project_file_load(project_name)

    if len(tasks) > 1:
        task_p = db["fractal"]["tasks"][tasks[0]]
        for task in tasks[1:]:
            task_n = db["fractal"]["tasks"][task]
            if not check_I_O(task_p, task_n):
                raise
            task_p = task_n
            prj["workflows"][workflow_name]["tasks"].append(task_p)

    task = db["fractal"]["tasks"][tasks[0]]
    prj["workflows"][workflow_name]["tasks"].append(task)

    save_project_file(project_name, prj)


@cli.command()
@click.argument("filename", required=True, nargs=1)
def apply(filename):

    with open(filename, "r") as jsonfile:
        f = json.load(jsonfile)
        jsonfile.close()

    project_name = f["arguments"]["project_name"]
    workflow_name = f["arguments"]["workflow_name"]
    input_dataset = [ds for ds in f["arguments"]["input_dataset"]]
    output_dataset = [ds for ds in f["arguments"]["output_dataset"]]
    resource_in = [r_in for r_in in f["arguments"]["resource_in"]]
    resource_out = [r_out for r_out in f["arguments"]["resource_out"]]

    prj, _ = project_file_load(project_name)

    for in_ds in input_dataset:
        path_in = [p for p in prj["datasets"][in_ds]["resources"]]
        debug(path_in)
    if not any(p in resource_in for p in path_in):
        raise

    resources_out = [Path(r_out) for r_out in resource_out]
    for i, res_out in enumerate(resources_out):
        if not res_out.exists():
            res_out = get_project(project_name)[0] / output_dataset[i]
            os.mkdir(res_out)
            prj["datasets"][output_dataset[i]]["resources"].extend(
                [res_out.resolve().as_posix()]
            )
            save_project_file(project_name, prj)

    task_names = [t["name"] for t in prj["workflows"][workflow_name]["tasks"]]

    db = db_load()
    f.update({"tasks": {}})
    for task in task_names:
        dependencies = db["fractal"]["tasks"][task]["depends_on"]
        f["tasks"].update({task: {"dependencies": dependencies}})

    cmd = ["python", os.getcwd() + "/luigi_wrap.py"]
    cmd.extend([json.dumps(f)])

    process = Popen(cmd, stderr=PIPE)
    stdout, stderr = process.communicate()
    debug(stderr)

    return stderr


@cli.group()
def ps():
    pass


@ps.command(name="list")
@click.argument("workflow_name", required=True, nargs=1)
def ps_list(workflow_name):
    raise NotImplementedError


if __name__ == "__main__":
    cli()
