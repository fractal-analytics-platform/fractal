import os
import sys
from glob import glob

import dask.array
import imagecodecs
import tifffile


def tif_to_zarr(in_path, zarrurl, delete_in, chl, **kwargs):

    chunksize = 100
    filenames = sorted([f for f in glob(in_path + f"*C{chl}*.tif")])

    def imread(filename):
        with open(filename, "rb") as fh:
            data = fh.read()
        return imagecodecs.tiff_decode(data)
        # to increase perf
        # numpy.fromfile(filename, offset=8, count=512*512,
        #                 dtype='uint16').reshape(512, 512).

    with tifffile.FileSequence(imread, filenames) as tifs:
        with tifs.aszarr() as store:
            da = dask.array.from_zarr(store)
            chunks = (chunksize,) + da.shape[1:]
            da.rechunk(chunks).to_zarr(zarrurl + f"/{chl}", **kwargs)

    if delete_in == "True":
        for f in filenames:
            try:
                os.remove(f)
            except OSError as e:
                print("Error: %s : %s" % (f, e.strerror))


if __name__ == "__main__":
    in_path = sys.argv[1]
    out_path = sys.argv[2]
    delete_in = sys.argv[3]
    chl = sys.argv[4]
    tif_to_zarr(in_path, out_path, delete_in, chl)
