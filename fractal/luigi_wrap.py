import json
import sys

import luigi

from .wrappers import compressionTaskWrap
from .wrappers import conversionTaskWrap

# TODO add task to clean old logs.
# class CleanLogs(luigi.Task):
#     pass


DICT_TASK = {
    "compression_tif": compressionTaskWrap,
    "tif_to_zarr": conversionTaskWrap,
    "yokogawa_tif_to_zarr": conversionTaskWrap,
    "3Dyokogawa_tif_to_zarr": conversionTaskWrap,
}


class WorkflowTask(luigi.Task):

    flags = luigi.parameter.DictParameter()

    _complete = False

    def run(self):

        task_names = self.flags["tasks"].keys()
        in_paths = [r_in for r_in in self.flags["arguments"]["resource_in"]]
        wf_name = self.flags["arguments"]["workflow_name"]

        out_paths = [
            r_out for r_out in self.flags["arguments"]["resource_out"]
        ]
        delete_ins = [d for d in self.flags["arguments"]["delete"]]
        sclr = self.flags["arguments"]["scheduler"]
        ext = self.flags["arguments"]["ext"]
        other_params = self.flags["arguments"]["other_params"]
        slurm_params = self.flags["arguments"]["slurm_params"]

        for i, task_name in enumerate(task_names):
            yield DICT_TASK[task_name](
                task_name=task_name,
                wf_name=wf_name,
                in_path=in_paths[i],
                out_path=out_paths[i],
                delete_in=delete_ins[i],
                sclr=sclr,
                ext=ext,
                other_param=other_params,
                slurm_param=slurm_params,
            )

        self._complete = True

    def complete(self):
        return self._complete


if __name__ == "__main__":

    luigi.build(
        [WorkflowTask(flags=json.loads(sys.argv[1]))],
        workers=1,
        parallel_scheduling=True,
    )
