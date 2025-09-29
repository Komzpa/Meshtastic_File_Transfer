"""CLI helpers for compressing large files prior to transfer."""

from __future__ import annotations

import argparse
import zipfile
from pathlib import Path
from typing import Callable

from PIL import Image
from pydub import AudioSegment


MIN_QUALITY = 1
MAX_QUALITY = 5


def replace_ext(path: Path, extension: str) -> Path:
    """Return ``path`` with ``_compressed`` appended and ``extension`` applied."""

    stem = path.stem
    parent = path.parent
    return parent / f"{stem}_compressed.{extension}"


def compress_image(image_file: Path, quality: int) -> Path:
    """Downscale ``image_file`` and write it as a WebP image."""

    quality = max(MIN_QUALITY, min(MAX_QUALITY, quality))
    comp_filename = replace_ext(image_file, "webp")

    with Image.open(image_file) as image:
        base_width = 540
        width_percent = base_width / float(image.size[0])
        hsize = int(float(image.size[1]) * width_percent)
        resized = image.resize((base_width, hsize))
        resized.save(
            comp_filename,
            "webp",
            optimize=True,
            quality=(quality - 1) * 20 + 10,
        )
    return comp_filename


def compress_audio(audio_file: Path, quality: int) -> Path:
    """Convert ``audio_file`` to a mono MP3 with a reduced sample rate."""

    comp_filename = replace_ext(audio_file, "mp3")
    audio = AudioSegment.from_file(audio_file)
    bitrate = max(MIN_QUALITY, min(MAX_QUALITY, quality)) * 16
    audio.export(
        comp_filename,
        format="mp3",
        parameters=["-ac", "1", "-ar", "8000", "-b:a", f"{bitrate}k"],
    )
    return comp_filename


def compress_with_fallback(file_name: Path, quality_level: int) -> Path:
    """Try to compress ``file_name`` using format-specific optimisations."""

    compressors: list[tuple[Callable[[Path, int], Path], tuple[str, ...]]] = [
        (compress_audio, (".wav", ".ogg", ".flac", ".mp3", ".aac")),
        (compress_image, (".jpg", ".jpeg", ".png", ".bmp", ".gif", ".tiff", ".webp")),
    ]

    for compressor, extensions in compressors:
        if file_name.suffix.lower() in extensions:
            return compressor(file_name, quality_level)

    archive_name = replace_ext(file_name, "zip")
    with zipfile.ZipFile(archive_name, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.write(file_name, arcname=file_name.name)
    return archive_name


def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser used by the compression CLI."""

    parser = argparse.ArgumentParser(
        prog="File Compressor",
        description="Compress a file as audio, image, or zip depending on its type.",
    )
    parser.add_argument("-f", "--file-name", required=True, help="File to compress")
    parser.add_argument(
        "-q",
        "--quality-level",
        type=int,
        default=MIN_QUALITY,
        help="Compression quality from 1 (smallest) to 5 (largest)",
    )
    return parser


def main() -> None:
    """Entry point for the ``file_compression`` command line tool."""

    parser = create_parser()
    args = parser.parse_args()
    input_file = Path(args.file_name).expanduser().resolve()
    if not input_file.exists():
        raise FileNotFoundError(f"Input file {input_file} does not exist")

    new_file = compress_with_fallback(input_file, args.quality_level)
    print(f"Saved file to {new_file}")


if __name__ == "__main__":
    main()

