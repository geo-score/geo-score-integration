"""HTTP file download with optional gzip decompression."""

import gzip
from pathlib import Path

import httpx
from rich.console import Console

console = Console()


def download_file(url: str, dest: Path, *, decompress: bool = False, label: str = "") -> Path:
    """Download a file via HTTP streaming. Optionally decompress .gz."""
    if decompress:
        out = dest / Path(url).name.removesuffix(".gz")
    else:
        out = dest / Path(url).name

    if out.exists():
        return out

    if label:
        console.print(f"  Downloading {label}...")

    gz_path = dest / Path(url).name
    with httpx.stream("GET", url, follow_redirects=True, timeout=120) as r:
        r.raise_for_status()
        with open(gz_path, "wb") as f:
            for chunk in r.iter_bytes(chunk_size=8192):
                f.write(chunk)

    if decompress and gz_path.suffix == ".gz":
        with gzip.open(gz_path, "rb") as gz_in, open(out, "wb") as f:
            f.write(gz_in.read())
        gz_path.unlink()

    return out
