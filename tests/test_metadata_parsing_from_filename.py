"""
Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
University of Zurich

Original authors:
Tommaso Comparin <tommaso.comparin@exact-lab.it>
Jacopo Nespolo <jacopo.nespolo@exact-lab.it>

This file is part of Fractal and was originally developed by eXact lab S.r.l.
<exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
Institute for Biomedical Research and Pelkmans Lab from the University of
Zurich.
"""
import pytest

from fractal.tasks.lib_parse_filename_metadata import parse_metadata

f1 = (
    "20200812-CardiomyocyteDifferentiation14-Cycle1"
    "_B03_T0001F036L01A01Z18C01.png"
)
f2 = "210305NAR005AAN_210416_164828_B11_T0001F006L01A04Z14C01.tif"
f3 = "220304_172545_220304_175557_L06_T0277F004L277A04Z07C04.tif"

p1 = "20200812-CardiomyocyteDifferentiation14-Cycle1"
p2 = "210305NAR005AAN"
p3 = "RS220304172545"


@pytest.mark.parametrize(
    "filename, plate_expected", [(f1, p1), (f2, p2), (f3, p3)]
)
def test_metadata(filename, plate_expected):
    assert parse_metadata(filename)["plate"] == plate_expected
