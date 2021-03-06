"""
Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
University of Zurich

Original authors:
Tommaso Comparin <tommaso.comparin@exact-lab.it>
Marco Franzon <marco.franzon@exact-lab.it>

This file is part of Fractal and was originally developed by eXact lab S.r.l.
<exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
Institute for Biomedical Research and Pelkmans Lab from the University of
Zurich.
"""
import dask.array as da
import numpy as np

from fractal.tasks.lib_to_zarr_custom import to_zarr_custom


def create_pyramid(
    data_czyx,
    coarsening_xy=1,
    num_levels=1,
    chunk_size_x=None,
    chunk_size_y=None,
    num_channels=None,
    aggregation_function=None,
):

    """
    Take a four-dimensional array and build a pyramid of coarsened levels

    :param data_czyx: input data
    :type data_czyx: dask array
    :param coarsening_xy: coarsening factor along X and Y
    :type coarsening_xy: int
    :param num_levels: number of levels in the zarr pyramid
    :type num_levels: int
    :param chunk_size_x: chunk size along X
    :type chunk_size_x: int
    :param chunk_size_y: chunk size along Y
    :type chunk_size_y: int
    :param num_channels: number of channels
    :type num_channels: int
    :param aggregation_function: FIXME
    :type aggregation_function: FIXME
    """

    # Check that input has the right shape
    if len(data_czyx.shape) != 4:
        raise Exception("Error in create_pyramid: data_czyx has wrong shape")

    # Check rechunking options
    apply_rechunking = True
    if chunk_size_x is None or chunk_size_y is None:
        apply_rechunking = False

    # Select aggregation_function
    if aggregation_function is None:
        aggregation_function = np.mean

    # Create pyramid of XY-coarser levels
    pyramid = []
    for level in range(num_levels):
        list_data_channels = []
        for ind_chl in range(num_channels):

            # Coarsen, if needed
            if level == 0:
                zyx_new = data_czyx[ind_chl]
            else:
                if min(pyramid[level - 1][ind_chl].shape[1:]) < coarsening_xy:
                    raise Exception(
                        f"ERROR: at level={level}, "
                        f"coarsening_xy={coarsening_xy} "
                        "but level-1 shape is "
                        f"{pyramid[level - 1][ind_chl].shape}"
                    )
                zyx_new = da.coarsen(
                    aggregation_function,
                    pyramid[level - 1][ind_chl],
                    {1: coarsening_xy, 2: coarsening_xy},
                    trim_excess=True,
                )
            list_data_channels.append(zyx_new)

        # Stack several channels
        data_czyx_new = da.stack(list_data_channels, axis=0)

        # Rechunk if needed
        if apply_rechunking:
            data_czyx_final = data_czyx_new.rechunk(
                {2: chunk_size_y, 3: chunk_size_x}, balance=True
            )
        else:
            data_czyx_final = data_czyx_new

        # Append new level
        pyramid.append(data_czyx_final.astype(data_czyx.dtype))

    return pyramid


def create_pyramid_3D(
    data_zyx,
    coarsening_xy=1,
    num_levels=1,
    chunk_size_x=None,
    chunk_size_y=None,
    aggregation_function=None,
):

    """
    Take a three-dimensional array and build a pyramid of coarsened levels

    :param data_czyx: input data
    :type data_czyx: dask array
    :param coarsening_xy: coarsening factor along X and Y
    :type coarsening_xy: int
    :param num_levels: number of levels in the zarr pyramid
    :type num_levels: int
    :param chunk_size_x: chunk size along X
    :type chunk_size_x: int
    :param chunk_size_y: chunk size along Y
    :type chunk_size_y: int
    :param aggregation_function: FIXME
    :type aggregation_function: FIXME
    """

    # Check that input has the right shape
    if len(data_zyx.shape) != 3:
        raise Exception("Error in create_pyramid: data_zyx has wrong shape")

    # Check rechunking options
    apply_rechunking = True
    if chunk_size_x is None or chunk_size_y is None:
        apply_rechunking = False

    # Select aggregation_function
    if aggregation_function is None:
        aggregation_function = np.mean

    # Create pyramid of XY-coarser levels
    pyramid = [data_zyx]
    for level in range(1, num_levels):
        # Check that we can produce the next level
        if min(pyramid[level - 1].shape[1:]) < coarsening_xy:
            raise Exception(
                f"ERROR: at level={level}, "
                f"coarsening_xy={coarsening_xy} "
                "but level-1 shape is "
                f"{pyramid[level - 1].shape}"
            )
        # Coarsen (level-1)-th level into level-th level
        data_zyx_new = da.coarsen(
            aggregation_function,
            pyramid[level - 1],
            {1: coarsening_xy, 2: coarsening_xy},
            trim_excess=True,
        )
        # Rechunk if needed
        if apply_rechunking:
            data_zyx_final = data_zyx_new.rechunk(
                {1: chunk_size_y, 2: chunk_size_x}, balance=True
            )
        else:
            data_zyx_final = data_zyx_new
        # Append new level
        pyramid.append(data_zyx_final.astype(data_zyx.dtype))

    return pyramid


def write_pyramid(
    data,
    overwrite=False,
    newzarrurl=None,
    coarsening_xy=2,
    num_levels=2,
    chunk_size_x=None,
    chunk_size_y=None,
    aggregation_function=None,
):

    """
    Take a four-dimensional array and build a pyramid of coarsened levels

    :param data_czyx: input data
    :type data_czyx: dask array
    :param coarsening_xy: coarsening factor along X and Y
    :type coarsening_xy: int
    :param num_levels: number of levels in the zarr pyramid
    :type num_levels: int
    :param chunk_size_x: chunk size along X
    :type chunk_size_x: int
    :param chunk_size_y: chunk size along Y
    :type chunk_size_y: int
    :param aggregation_function: FIXME
    :type aggregation_function: FIXME
    """

    # Check the number of axes and identify YX dimensions
    ndims = len(data.shape)
    if ndims not in [2, 3, 4]:
        raise Exception(
            "ERROR: data has shape {data.shape}, ndims not in [2,3,4]"
        )
    y_axis = ndims - 2
    x_axis = ndims - 1

    # Set rechunking options, if needed
    if chunk_size_x is None or chunk_size_y is None:
        apply_rechunking = False
    else:
        apply_rechunking = True
        chunking = {y_axis: chunk_size_y, x_axis: chunk_size_x}

    # Set aggregation_function
    if aggregation_function is None:
        aggregation_function = np.mean

    # Write highest-resolution level
    level0 = to_zarr_custom(
        newzarrurl=newzarrurl, array=data, component="0", overwrite=overwrite
    )
    if apply_rechunking:
        previous_level = level0.rechunk(chunking)
    else:
        previous_level = level0

    # Compute and write lower-resolution levels
    for ind_level in range(1, num_levels):

        # Verify that coarsening is doable
        if min(previous_level.shape[-2:]) < coarsening_xy:
            raise Exception(
                f"ERROR: at {ind_level}-th level, "
                f"coarsening_xy={coarsening_xy} "
                f"but previous level has shape {previous_level.shape}"
            )

        # Apply coarsening
        newlevel = da.coarsen(
            aggregation_function,
            previous_level,
            {y_axis: coarsening_xy, x_axis: coarsening_xy},
            trim_excess=True,
        ).astype(data.dtype)

        # Apply rechunking
        if apply_rechunking:
            newlevel_rechunked = newlevel.rechunk(chunking)
        else:
            newlevel_rechunked = newlevel

        # Write zarr and store output (useful to construct next level)
        previous_level = to_zarr_custom(
            newzarrurl=newzarrurl,
            array=newlevel_rechunked,
            component=f"{ind_level}",
            overwrite=overwrite,
        )
