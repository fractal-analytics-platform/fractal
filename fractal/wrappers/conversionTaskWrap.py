import copy
import datetime
import os
import random
import re
import uuid
from glob import glob
from subprocess import PIPE  # nosec
from subprocess import Popen  # nosec

import jinja2
import luigi
import zarr
from devtools import debug


class ConversionTaskWrap(luigi.Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.complete_flag = False

    retry_count = 5
    # accepts_messages = True

    task_name = luigi.parameter.Parameter(significant=True)

    wf_name = luigi.parameter.Parameter(significant=True)

    in_path = luigi.parameter.Parameter(significant=True)

    out_path = luigi.parameter.Parameter(significant=True)

    delete_in = luigi.parameter.Parameter()

    sclr = luigi.parameter.Parameter()

    ext = luigi.parameter.Parameter()

    slurm_param = luigi.parameter.DictParameter()

    other_param = luigi.parameter.DictParameter()

    tasks_path = os.getcwd() + "/tasks/"

    done = False

    def metadata(self, filename):
        ##
        f = filename.rsplit(".", 1)[0]
        plate = f.split("_")[0]
        well = re.findall(r"_(.*)_F", f)[0].split("_")[-1]
        site = re.findall(r"F(.*)L", f)[0]
        chl = re.findall(r"C(.*)", f)[0]
        time_s = re.findall(r"T(.*)F", f)[0]
        z_ind = re.findall(r"Z(.*)C", f)[0]
        return [plate, well, time_s, chl, z_ind, site]

    def unique(self, list1):

        unique_list = []

        for x in list1:
            if x not in unique_list:
                unique_list.append(x)
        ch = []
        for x in unique_list:
            ch.append(x)
        return ch

    def complete(self):
        if self.done:
            return True
        return False

    def output(self):

        f_log = (
            f"./log/{self.task_name}_"
            + str(uuid.uuid4())[:8]
            + str(datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S"))
            + ".txt"
        )

        return luigi.LocalTarget(f_log)

    def do_proc(self, cmd, c):

        process = Popen(cmd + [f"{c}"], stdout=PIPE, stderr=PIPE)

        stdout, stderr = process.communicate()
        if not stderr:
            debug("--No errors--\n", stdout.decode())
        else:
            debug("--Error--\n", stderr.decode())

        with self.output().open("w") as outfile:
            outfile.write(f"{stderr}\n")
        return process

    def run(self):

        rows_cols = self.other_param["dims"]
        rows = rows_cols.split(",")[0]
        cols = rows_cols.split(",")[1]
        chl = []
        well = []
        plate = []
        for i in glob(self.in_path + "*." + self.ext):
            plate.append(self.metadata(os.path.basename(i))[0])

        plate_unique = self.unique(plate)

        if self.sclr == "local":
            run = ["python"]

            for plate in plate_unique:
                group_plate = zarr.group(self.out_path + f"{plate}.zarr")
                well = [
                    self.metadata(os.path.basename(fn))[1]
                    for fn in glob(self.in_path + f"{plate}_*." + self.ext)
                ]
                well_unique = self.unique(well)

                well_rows_columns = [
                    ind for ind in sorted([(n[0], n[1:]) for n in well_unique])
                ]

                group_plate.attrs["plate"] = {
                    "acquisitions": [
                        {"id": id_, "name": name}
                        for id_, name in enumerate(plate_unique)
                    ],
                    "columns": [
                        {"name": well_row_column[1]}
                        for well_row_column in well_rows_columns
                    ],
                    "rows": [
                        {"name": well_row_column[0]}
                        for well_row_column in well_rows_columns
                    ],
                    "wells": [
                        {
                            "path": well_row_column[0]
                            + "/"
                            + well_row_column[1],
                        }
                        for well_row_column in well_rows_columns
                    ],
                }

                for row, column in well_rows_columns:

                    group_well = group_plate.create_group(f"{row}/{column}/")

                    chl = [
                        self.metadata(os.path.basename(fn))[3]
                        for fn in glob(
                            self.in_path
                            + f"{plate}_*_*_{row+column}_*."
                            + self.ext
                        )
                    ]

                    chl_unique = self.unique(chl)

                    group_well.attrs["well"] = {
                        "images": [
                            {"path": f"{int(chl)-1}"} for chl in chl_unique
                        ],
                        "version": "0.3",
                    }

                    for ch in chl_unique:
                        group_field = group_well.create_group(
                            f"{int(ch)-1}/"
                        )  # noqa: F841

                        group_field.attrs["multiscales"] = [
                            {
                                "version": "0.3",
                                "axes": [
                                    {"name": "y", "type": "space"},
                                    {"name": "x", "type": "space"},
                                ],
                                "datasets": [{"path": "0"}],
                                "metadata": {
                                    "kwargs": {"axes_names": ["y", "x"]}
                                },
                            }
                        ]

                        cmd = copy.copy(run)
                        cmd.extend(
                            [
                                self.tasks_path + self.task_name + ".py",
                                self.in_path,
                                self.out_path,
                                f"{plate}.zarr/" + f"{well}_{ch}/",
                                self.delete_in,
                                rows,
                                cols,
                                self.ext,
                            ]
                        )
                        process = Popen(
                            cmd + [f"{ch}"], stdout=PIPE, stderr=PIPE
                        )
                        stdout, stderr = process.communicate()
                        debug(process.communicate())
                        with self.output().open("w") as outfile:
                            outfile.write(f"{stderr}\n")

        elif self.sclr == "slurm":

            srun = ""
            # loop over plate, each plate could have n wells
            for plate in plate_unique:
                group_plate = zarr.group(self.out_path + f"{plate}.zarr")
                well = [
                    self.metadata(os.path.basename(fn))[1]
                    for fn in glob(self.in_path + f"{plate}_*." + self.ext)
                ]
                well_unique = self.unique(well)

                well_rows_columns = [
                    ind for ind in sorted([(n[0], n[1:]) for n in well_unique])
                ]

                group_plate.attrs["plate"] = {
                    "acquisitions": [
                        {"id": id_, "name": name}
                        for id_, name in enumerate(plate_unique)
                    ],
                    "columns": [
                        {"name": well_row_column[1]}
                        for well_row_column in well_rows_columns
                    ],
                    "rows": [
                        {"name": well_row_column[0]}
                        for well_row_column in well_rows_columns
                    ],
                    "wells": [
                        {
                            "path": well_row_column[0]
                            + "/"
                            + well_row_column[1],
                        }
                        for well_row_column in well_rows_columns
                    ],
                }

                for row, column in well_rows_columns:

                    group_well = group_plate.create_group(f"{row}/{column}/")

                    chl = [
                        self.metadata(os.path.basename(fn))[3]
                        for fn in glob(
                            self.in_path
                            + f"{plate}_*_*_{row+column}_*."
                            + self.ext
                        )
                    ]

                    chl_unique = self.unique(chl)

                    group_well.attrs["well"] = {
                        "images": [
                            {"path": f"{int(chl)-1}"} for chl in chl_unique
                        ],
                        "version": "0.3",
                    }

                    cores = str(
                        self.slurm_param["cores"]
                    )  # "4" #slurm_param["cores"]
                    mem = str(
                        self.slurm_param["mem"]
                    )  # "1024" #slurm_param["mem"]
                    nodes = str(self.slurm_param["nodes"])

                    loader = jinja2.FileSystemLoader(searchpath="./")
                    env = jinja2.Environment(loader=loader, autoescape=True)
                    t = env.get_template("job.default.j2")
                    job = (
                        self.wf_name
                        + "_"
                        + self.task_name
                        + str(random.randrange(0, 101, 5))  # nosec
                    )

                    for ch in chl_unique:
                        group_field = group_well.create_group(
                            f"{int(ch)-1}/"
                        )  # noqa: F841

                        group_field.attrs["multiscales"] = [
                            {
                                "version": "0.3",
                                "axes": [
                                    {"name": "y", "type": "space"},
                                    {"name": "x", "type": "space"},
                                ],
                                "datasets": [{"path": "0"}],
                                "metadata": {
                                    "kwargs": {"axes_names": ["y", "x"]}
                                },
                            }
                        ]

                        srun += " ".join(
                            [
                                " srun python",
                                self.tasks_path + self.task_name + ".py ",
                                self.in_path,
                                self.out_path,
                                f"{plate}.zarr/"
                                + f"{row}/{column}/{int(ch)-1}/0",
                                self.delete_in,
                                rows,
                                cols,
                                self.ext,
                                f"{ch}",
                                "&",
                            ]
                        )

                srun = srun + " wait"

                with open(f"./jobs/{job}", "w") as f:
                    f.write(
                        t.render(
                            job_name="test"
                            + str(random.randrange(0, 101, 5)),  # nosec
                            nodes=nodes,
                            cores=cores,
                            mem=mem + "MB",
                            command=srun,
                        )
                    )

                cmd = ["sbatch", f"./jobs/{job}"]
                debug(cmd)
                process = Popen(cmd, stderr=PIPE)
                stdout, stderr = process.communicate()
                if not stderr:
                    debug("--No errors--\n", stdout.decode())
                else:
                    debug("--Error--\n", stderr.decode())

        self.done = True
