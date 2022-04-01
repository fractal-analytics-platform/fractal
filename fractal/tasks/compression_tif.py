import glob
import os

from PIL import Image


def compress(
    in_path: str,
    out_path: str,
    start: int = 0,
    end: int = -1,
    delete_in: bool = False,
) -> None:
    """
    Compress tiff files

    :param str in_path: directory containing the input files
    :param str out_path: directory containing the output files
    :param int start: index of first file to process
    :param int end: index of last file to process
    :param bool delete_in: delete input files, and folder if empty
    """

    if not os.path.exists(out_path):
        os.makedirs(out_path)

    file_list = []

    if isinstance(in_path, str):
        # os.chdir(in_path)
        f_list = glob.glob(in_path + "*.tif")[
            int(start) : int(end)  # noqa: E203
        ]
        for filename in f_list:
            with Image.open(filename) as image:
                image.save(
                    os.path.join(
                        out_path,
                        os.path.basename(filename).strip(".tif")
                        + "_compressed.tif",
                    ),
                    compression="tiff_lzw",
                )
            file_list.append(filename)

        if delete_in == "True":
            for f in file_list:
                try:
                    os.remove(f)
                except OSError as e:
                    print("Error: %s : %s" % (f, e.strerror))


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser(prog="compression_tif")
    parser.add_argument("in_path", help="directory containing the input files")
    parser.add_argument(
        "out_path", help="directory containing the output files"
    )
    parser.add_argument(
        "-s",
        "--start-index",
        type=int,
        help="index of first file to process (default: 0)",
        default=0,
    )
    parser.add_argument(
        "-e",
        "--end-index",
        type=int,
        help="index of last file to process (default: -1)",
        default=-1,
    )
    parser.add_argument(
        "-d",
        "--delete-input",
        default=False,
        action="store_true",
        help="Delete input files and folder",
    )

    args = parser.parse_args()

    compress(
        args.in_path,
        args.start_index,
        args.end_index,
        args.out_path,
        args.delete_input,
    )
