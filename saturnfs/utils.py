from enum import Enum


class Units(int, Enum):
    KiB = 1 << 10
    MiB = 1 << 20
    GiB = 1 << 30
    TiB = 1 << 40


def human_readable_format(size: int):
    for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if size < 1024.0 or unit == "TiB":
            break
        size /= 1024.0
    formatted = f"{size:.2f}"
    formatted = formatted.rstrip("0").rstrip(".")
    return f"{formatted} {unit}"
