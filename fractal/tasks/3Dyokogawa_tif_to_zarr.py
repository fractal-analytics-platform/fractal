import os
import sys
from glob import glob

import dask.array as da
from dask import delayed
from skimage.io import imread


# TODO use kwargs not args
def yokogawa_tif_to_zarr(
    in_path, out_path, zarrurl, delete_in, rows, cols, ext, chl, **kwargs
):

    r = zarrurl.split("/")[1]
    c = zarrurl.split("/")[2]
    filenames = sorted(glob(in_path + f"*_{r+c}_*C{chl}*." + ext))
    # print(in_path + f"*C{chl}*."+ ext)
    # assuming that all files have same shape and type
    sample = imread(filenames[0])

    lazy_imread = delayed(imread)  # lazy reader
    lazy_arrays = [lazy_imread(fn) for fn in filenames]
    dask_arrays = [
        da.from_delayed(delayed_reader, shape=sample.shape, dtype=sample.dtype)
        for delayed_reader in lazy_arrays
    ]

    stack = da.stack(dask_arrays, axis=0)

    stack.to_zarr(out_path + zarrurl, dimension_separator="/")

    if delete_in == "True":
        for f in filenames:
            try:
                os.remove(f)
            except OSError as e:
                print("Error: %s : %s" % (f, e.strerror))


if __name__ == "__main__":
    in_path = sys.argv[1]
    out_path = sys.argv[2]
    zarrurl = sys.argv[3]
    delete_in = sys.argv[4]
    rows = sys.argv[5]
    cols = sys.argv[6]
    ext = sys.argv[7]
    chl = sys.argv[8]

    yokogawa_tif_to_zarr(
        in_path, out_path, zarrurl, delete_in, rows, cols, ext, chl
    )
