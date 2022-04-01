import datetime
import os
import uuid
from functools import partial
from glob import glob
from multiprocessing import Pool
from subprocess import PIPE  # nosec
from subprocess import Popen  # nosec

import jinja2
import luigi
from devtools import debug


class CompressionTaskWrap(luigi.Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.complete_flag = False

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

    def do_comp(self, cmd, interval):

        process = Popen(  # nosec
            cmd + [f"{interval[0]}"] + [f"{interval[1]}"],  # nosec
            stderr=PIPE,  # nosec
        )  # nosec

        stdout, stderr = process.communicate()

        if not stderr:
            debug("--No errors--\n", stdout.decode())
        else:
            debug("--Error--\n", stderr.decode())

        with self.output().open("w") as outfile:
            outfile.write(f"{stderr}\n")
        return process

    def run(self):

        self.done = True

        batch_size = self.other_param["batch_size"]
        l_file = glob(self.in_path + "*." + self.ext)

        batch = len(l_file) // batch_size

        tmp_s = 0
        intervals = []
        for tmp_e in range(batch, len(l_file) + 1, batch):
            intervals.append((tmp_s, tmp_e))
            tmp_s = tmp_e

        if self.sclr == "local":

            cmd = ["python"]

            cmd.extend(
                [
                    self.tasks_path + self.task_name + ".py",
                    self.in_path,
                    self.out_path,
                    self.delete_in,
                    self.ext,
                ]
            )

            p = Pool()
            do_comp_part = partial(self.do_comp, cmd)
            p.map_async(do_comp_part, intervals)
            p.close()
            p.join()

        elif self.sclr == "slurm":

            cores = str(self.slurm_param["cores"])  # "4" #slurm_param["cores"]
            mem = str(self.slurm_param["mem"])  # "1024" #slurm_param["mem"]
            nodes = str(self.slurm_param["nodes"])

            loader = jinja2.FileSystemLoader(searchpath="./")
            env = jinja2.Environment(
                loader=loader, autoescape=True
            )  # nosec  # nosec  # nosec
            t = env.get_template("job.default.j2")
            job = self.wf_name + "_" + self.task_name

            srun = ""
            for interval0, interval1 in intervals:

                srun += " ".join(
                    [
                        " srun python",
                        self.tasks_path + self.task_name + ".py ",
                        self.in_path,
                        self.out_path,
                        self.delete_in,
                        self.ext,
                        f"{interval0}",
                        f"{interval1}",
                        "&",
                    ]
                )

            srun = srun + " wait"

            with open(f"./jobs/{job}", "w") as f:
                f.write(
                    t.render(
                        job_name="test1",
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

            with self.output().open("w") as outfile:
                outfile.write(f"{stderr}\n")
            return process

        debug(cmd)

        self.done = True
