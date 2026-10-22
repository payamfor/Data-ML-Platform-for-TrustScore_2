"""Build only the recovery sidecar; NEVER overwrite gold_pipeline.zip.

No dependencies are installed and no AWS operation occurs. Existing staged
runs keep their original Gold ZIP and CODE_SHA256. Reproducible ZIP entries and
a file inventory make the small additive artifact reviewable.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo


def main():
    root = Path(__file__).resolve().parent
    files = sorted((root / "gold_recovery").glob("*.py"))
    output = root / "gold_recovery_tools.zip"
    inventory = {}
    with ZipFile(output, "w", compression=ZIP_DEFLATED) as archive:
        for file in files:
            data = file.read_bytes().replace(b"\r\n", b"\n")
            compile(data, str(file), "exec")
            name = file.relative_to(root).as_posix()
            info = ZipInfo(name, (2026, 1, 1, 0, 0, 0))
            info.compress_type = ZIP_DEFLATED
            archive.writestr(info, data)
            inventory[name] = hashlib.sha256(data).hexdigest()
    entrypoints = [root / "recover_gold_run.py", root / "rollback_gold_publish.py"]
    entrypoint_hashes = {}
    for entrypoint in entrypoints:
        compile(entrypoint.read_bytes(), str(entrypoint), "exec")
        entrypoint_hashes[entrypoint.name] = hashlib.sha256(entrypoint.read_bytes()).hexdigest()
    manifest = {"artifact": output.name, "sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
                "entrypoints": entrypoint_hashes,
                "files": inventory, "original_gold_zip_rebuilt": False}
    (root / "gold_recovery_build_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
