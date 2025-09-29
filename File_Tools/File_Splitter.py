"""Utilities for splitting a large file into manageable chunks."""

from __future__ import annotations

import argparse
from pathlib import Path


DEFAULT_CHUNK_SIZE = 51_200


def split_file(path: Path, chunk_size: int = DEFAULT_CHUNK_SIZE) -> Path:
    """Split ``path`` into ``chunk_size`` byte segments."""

    if chunk_size <= 0:
        raise ValueError("chunk_size must be a positive integer")

    destination = path.with_suffix("")
    destination.mkdir(parents=True, exist_ok=True)

    with path.open("rb") as source:
        index = 1
        while True:
            chunk = source.read(chunk_size)
            if not chunk:
                break
            part_file = destination / f"{path.stem}_{index}{path.suffix}"
            with part_file.open("wb") as part:
                part.write(chunk)
            index += 1

    return destination


def create_parser() -> argparse.ArgumentParser:
    """Return the CLI argument parser for the splitter script."""

    parser = argparse.ArgumentParser(
        prog="File splitter",
        description="Split a file into equal sized chunks stored in a sibling directory.",
    )
    parser.add_argument("-f", "--file-name", required=True, help="File to split")
    parser.add_argument(
        "-s",
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help="Chunk size in bytes (default: 51,200)",
    )
    return parser


def main() -> None:
    """Entry point for the ``file_splitter`` command line tool."""

    parser = create_parser()
    args = parser.parse_args()
    source = Path(args.file_name).expanduser().resolve()
    if not source.exists():
        raise FileNotFoundError(f"Input file {source} does not exist")

    destination = split_file(source, args.chunk_size)
    print(f"Split {source} into {destination}")


if __name__ == "__main__":
    main()
