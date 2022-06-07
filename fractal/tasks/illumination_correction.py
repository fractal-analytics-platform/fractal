import json

import dask.array as da
import numpy as np
from skimage.io import imread

from fractal.tasks.lib_pyramid_creation import create_pyramid


def illumination_correction(
    zarrurl,
    overwrite=False,
    newzarrurl=None,
    chl_list=None,
    coarsening_xy=2,
    background_threshold=110,
):

    """
    Perform illumination correction of the array in zarrurl
    a new zarr file.

    :param zarrurl: input zarr file, at the site level (e.g. x.zarr/B/03/0/)
    :type zarrurl: str
    :param chl_list: list of channels
    :type chl_list: list
    :param coarsening_xy: coarsening factor along X and Y
    :type coarsening_z: xy


    """

    # Check that only one output option is chosen
    if overwrite and (newzarrurl is not None):
        raise
    if newzarrurl is None and not overwrite:
        raise

    # Sanitize zarr paths
    if not zarrurl.endswith("/"):
        zarrurl += "/"
    if overwrite:
        newzarrurl = zarrurl
    else:
        if not newzarrurl.endswith("/"):
            newzarrurl += "/"

    # Hard-coded values for the image size
    img_size_y = 2160
    img_size_x = 2560

    # FIXME: this block is too specific!
    # Hard-coded choice of illumination correction matrix
    path_correction_matrices = (
        "/data/active/fractal/Liberali/"
        "FractalTesting20220124/"
        "IlluminationCorrectionMatrices-Yokogawa/"
    )
    filenames = {
        1: "220120_60xW_BP445_CH01.tif",
        2: "220120_60xW_BP525_CH02.tif",
        3: "220120_60xW_BP600_CH03.tif",
        4: "220120_60xW_BP676_CH04.tif",
    }
    corrections = {}
    for chl in chl_list:
        corrections[int(chl)] = imread(
            path_correction_matrices + filenames[int(chl)]
        )
        if corrections[int(chl)].shape != (img_size_y, img_size_x):
            raise

    # Read number of levels from .zattrs of original zarr file
    with open(zarrurl + ".zattrs", "r") as inputjson:
        zattrs = json.load(inputjson)
    num_levels = len(zattrs["multiscales"][0]["datasets"])

    # Load highest-resolution level from original zarr array
    data_czyx = da.from_zarr(zarrurl + "/0")

    # Loop over channels
    data_czyx_new = []
    for ind_chl, chl in enumerate(chl_list):
        data_zyx = data_czyx[ind_chl]

        # Check that input array is made of images (in terms of shape/chunks)
        nz, ny, nx = data_zyx.shape
        if (ny % img_size_y != 0) or (nx % img_size_x != 0):
            raise
        chunks_z, chunks_y, chunks_x = data_zyx.chunks
        if len(set(chunks_y)) != 1 or chunks_y[0] != img_size_y:
            raise
        if len(set(chunks_x)) != 1 or chunks_x[0] != img_size_x:
            raise

        # Correction function to be applied on each chunk, for each Z level
        def correct(img):

            # Background subtraction
            img[img <= background_threshold] = 0
            img[img > background_threshold] = (
                img[img > background_threshold] - background_threshold
            )

            # Select illumination-correction matrix
            # FIXME this is currently very specific!
            illum_img = corrections[int(chl)]

            # Actual illumination correction:
            ratio = img / illum_img
            img_corr = np.mean(img) * ratio / np.mean(ratio)

            # Recasting into uint16 (mostly stays in uint16, but in some edge
            # cases it doesn't and recasting is useful)
            return img_corr.astype("uint16")

        # Loop over Z levels and collect corrected arrays
        data_zyx_new = []
        for ind_z in range(nz):
            print(data_zyx[ind_z].shape)
            print(data_zyx[ind_z].chunks)
            data_zyx_new.append(
                data_zyx[ind_z]
                .map_blocks(
                    correct,
                    chunks=(img_size_y, img_size_x),
                    meta=np.array((), dtype=np.uint16),
                )
                .compute()
            )
        data_zyx_new = da.stack(data_zyx_new, axis=0)

        # Accumulate different channels
        data_czyx_new.append(data_zyx_new)

    data_czyx_new = da.stack(data_czyx_new, axis=0)

    # Construct resolution pyramid
    pyramid = create_pyramid(
        data_czyx_new,
        coarsening_z=1,
        coarsening_xy=coarsening_xy,
        num_levels=num_levels,
        chunk_size_x=img_size_x,
        chunk_size_y=img_size_y,
        chl_list=chl_list,
    )

    # Write data into output zarr
    for ind_level in range(num_levels):
        pyramid[ind_level].to_zarr(
            newzarrurl + f"{ind_level}/",
            dimension_separator="/",
            overwrite=overwrite,
        )


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser(prog="illumination_correction.py")
    parser.add_argument(
        "-z", "--zarrurl", help="zarr url, at the FOV level", required=True
    )
    parser.add_argument(
        "-o",
        "--overwrite",
        action="store_true",
        help="overwrite original zarr file",
    )
    parser.add_argument(
        "-znew",
        "--newzarrurl",
        help="path of the new zarr file",
    )

    parser.add_argument(
        "-C",
        "--chl_list",
        nargs="+",
        help="list of channels ",
    )
    parser.add_argument(
        "-cxy",
        "--coarsening_xy",
        default=2,
        type=int,
        help="coarsening factor along X and Y (optional, defaults to 2)",
    )
    parser.add_argument(
        "-bg",
        "--background_threshold",
        default=110,
        type=int,
        help=(
            "threshold for background subtraction"
            " (optional, defaults to 110)"
        ),
    )

    args = parser.parse_args()
    illumination_correction(
        args.zarrurl,
        overwrite=args.overwrite,
        newzarrurl=args.newzarrurl,
        chl_list=args.chl_list,
        coarsening_xy=args.coarsening_xy,
        background_threshold=args.background_threshold,
    )