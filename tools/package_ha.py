"""Build a local HA custom-component archive from the Core fork and client wheel."""

import argparse
import json
import shutil
import tarfile
import tempfile
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("core", type=Path)
parser.add_argument("wheel", type=Path)
parser.add_argument("output", type=Path)
args = parser.parse_args()
with tempfile.TemporaryDirectory(dir=args.output.parent) as tmp:
    target = Path(tmp) / "custom_components/xthings_cloud"
    shutil.copytree(
        args.core / "homeassistant/components/xthings_cloud",
        target,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    shutil.copy2(args.wheel, target / args.wheel.name)
    manifest_path = target / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["version"] = "2026.9.3.dev9"
    manifest["requirements"] = [
        f"ha-xthings-cloud @ file:///config/custom_components/xthings_cloud/{args.wheel.name}"
    ]
    manifest["issue_tracker"] = "https://github.com/ariofrio/ha-xthings-cloud/issues"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    with tarfile.open(args.output, "w:gz") as archive:
        archive.add(target, arcname="custom_components/xthings_cloud")
print(args.output)
