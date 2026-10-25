"""Build a deterministic import ZIP; no third-party libraries or runtime downloads in Glue."""

from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo


def package(root: Path, output: Path):
    output.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(output, "w", compression=ZIP_DEFLATED) as archive:
        for path in sorted((root / "src" / "delivery_lakehouse").rglob("*.py")):
            info = ZipInfo(str(path.relative_to(root / "src")), date_time=(2026, 1, 1, 0, 0, 0))
            info.compress_type = ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            archive.writestr(info, path.read_bytes())


if __name__ == "__main__":
    package(Path(__file__).resolve().parents[1], Path("dist/delivery_lakehouse.zip"))
