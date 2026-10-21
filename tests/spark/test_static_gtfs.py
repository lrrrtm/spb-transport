import zipfile
from pathlib import Path

import pytest
from spb_transport_spark.static_gtfs import REQUIRED_COLUMNS, validate_archive


def write_feed(
    path: Path,
    *,
    omit: str | None = None,
    nested: bool = False,
    omit_route_type: bool = False,
) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        for filename, columns in REQUIRED_COLUMNS.items():
            if filename == omit:
                continue
            name = f"nested/{filename}" if nested and filename == "routes.txt" else filename
            header = (
                columns - {"route_type"}
                if omit_route_type and filename == "routes.txt"
                else columns
            )
            archive.writestr(name, ",".join(sorted(header)) + "\n")


def test_validate_archive_accepts_confirmed_static_contract(tmp_path: Path) -> None:
    feed = tmp_path / "feed.zip"
    write_feed(feed)

    validate_archive(feed, max_uncompressed_bytes=10_000)


def test_validate_archive_rejects_missing_required_file(tmp_path: Path) -> None:
    feed = tmp_path / "feed.zip"
    write_feed(feed, omit="shapes.txt")

    with pytest.raises(ValueError, match="shapes.txt"):
        validate_archive(feed, max_uncompressed_bytes=10_000)


def test_validate_archive_rejects_nested_paths(tmp_path: Path) -> None:
    feed = tmp_path / "feed.zip"
    write_feed(feed, nested=True)

    with pytest.raises(ValueError, match="archive root"):
        validate_archive(feed, max_uncompressed_bytes=10_000)


def test_validate_archive_rejects_missing_required_column(tmp_path: Path) -> None:
    feed = tmp_path / "feed.zip"
    write_feed(feed, omit_route_type=True)

    with pytest.raises(ValueError, match="route_type"):
        validate_archive(feed, max_uncompressed_bytes=10_000)
