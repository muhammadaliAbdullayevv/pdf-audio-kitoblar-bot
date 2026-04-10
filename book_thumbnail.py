from __future__ import annotations

import logging
import os
from functools import lru_cache
from io import BytesIO
from pathlib import Path

from PIL import Image, ImageFilter, ImageOps
from telegram import InputFile

logger = logging.getLogger(__name__)

_MAX_THUMBNAIL_BYTES = 200 * 1024
_MAX_THUMBNAIL_SIZE = (320, 320)
_THUMBNAIL_BG = (9, 22, 70)
_THUMBNAIL_PAD = 10
_THUMBNAIL_QUALITY_STEPS = (98, 95, 92, 88, 84, 80, 76, 72)


def _candidate_paths() -> list[Path]:
    root = Path(__file__).resolve().parent
    candidates: list[Path] = []

    env_path = str(os.getenv("BOOK_THUMBNAIL_PATH", "") or "").strip()
    if env_path:
        candidates.append(Path(env_path).expanduser())

    candidates.extend(
        [
            Path("/home/muhammadaliabdullayev/Downloads/main logo.jpg"),
            root / "assets" / "book_thumbnail.jpg",
            root / "assets" / "book_thumbnail.jpeg",
            root / "assets" / "book_thumbnail.png",
            root / "book_thumbnail.jpg",
            root / "book_thumbnail.jpeg",
            root / "book_thumbnail.png",
        ]
    )
    return candidates


def _resolve_source_path() -> Path | None:
    for candidate in _candidate_paths():
        try:
            if candidate.is_file():
                return candidate
        except Exception:
            continue
    return None


@lru_cache(maxsize=1)
def _build_thumbnail_payload() -> tuple[bytes, str] | None:
    source = _resolve_source_path()
    if not source:
        return None

    try:
        with Image.open(source) as image:
            image = ImageOps.exif_transpose(image).convert("RGB")
            inner_max = (
                max(1, _MAX_THUMBNAIL_SIZE[0] - (_THUMBNAIL_PAD * 2)),
                max(1, _MAX_THUMBNAIL_SIZE[1] - (_THUMBNAIL_PAD * 2)),
            )
            image = ImageOps.contain(
                image,
                inner_max,
                method=Image.Resampling.LANCZOS,
            )
            image = image.filter(ImageFilter.UnsharpMask(radius=1.1, percent=120, threshold=2))

            canvas = Image.new("RGB", _MAX_THUMBNAIL_SIZE, _THUMBNAIL_BG)
            offset = (
                (_MAX_THUMBNAIL_SIZE[0] - image.width) // 2,
                (_MAX_THUMBNAIL_SIZE[1] - image.height) // 2,
            )
            canvas.paste(image, offset)
            image = canvas

            # Telegram thumbnails must be JPEG and small. Try a few qualities.
            for quality in _THUMBNAIL_QUALITY_STEPS:
                buffer = BytesIO()
                image.save(buffer, format="JPEG", quality=quality, optimize=True, subsampling=0)
                data = buffer.getvalue()
                if len(data) <= _MAX_THUMBNAIL_BYTES:
                    return data, f"{source.stem}.jpg"

            # Last resort: return the smallest attempt we made.
            buffer = BytesIO()
            image.save(buffer, format="JPEG", quality=_THUMBNAIL_QUALITY_STEPS[-1], optimize=True, subsampling=0)
            data = buffer.getvalue()
            if len(data) > _MAX_THUMBNAIL_BYTES:
                logger.warning(
                    "Book thumbnail %s is still %s bytes after compression; Telegram may reject it",
                    source,
                    len(data),
                )
            return data, f"{source.stem}.jpg"
    except Exception as e:
        logger.warning("Failed to prepare book thumbnail from %s: %s", source, e, exc_info=True)
        return None


def get_book_thumbnail_input() -> InputFile | None:
    payload = _build_thumbnail_payload()
    if not payload:
        return None
    data, filename = payload
    return InputFile(data, filename=filename)
