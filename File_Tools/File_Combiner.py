"""Reconstruct a file previously split into numbered parts."""

from __future__ import annotations

import argparse
from pathlib import Path


def combine_parts(directory: Path) -> Path:
    """Combine numbered parts inside ``directory`` into a single file."""

    parts = sorted(file for file in directory.iterdir() if file.is_file())
    if not parts:
        raise FileNotFoundError(f"No files found in {directory}")

    suffix = parts[0].suffix
    output_file = directory.with_suffix(suffix or ".bin")

    with output_file.open("wb") as destination:
        for part in parts:
            destination.write(part.read_bytes())

    return output_file


def create_parser() -> argparse.ArgumentParser:
    """Return the argument parser for the combiner CLI."""

    parser = argparse.ArgumentParser(
        prog="File Combiner",
        description="Combine numbered files in a directory into a single output file.",
    )
    parser.add_argument(
        "-d",
        "--directory",
        required=True,
        help="Directory containing numbered parts (e.g. name_1.ext)",
    )
    return parser


def main() -> None:
    """Entry point for the ``file_combiner`` command line tool."""

    parser = create_parser()
    args = parser.parse_args()
    directory = Path(args.directory).expanduser().resolve()
    if not directory.exists() or not directory.is_dir():
        raise NotADirectoryError(
            f"Input directory {directory} does not exist or is not a directory",
        )

    output = combine_parts(directory)
    print(f"Combined files into {output}")


if __name__ == "__main__":
    main()
