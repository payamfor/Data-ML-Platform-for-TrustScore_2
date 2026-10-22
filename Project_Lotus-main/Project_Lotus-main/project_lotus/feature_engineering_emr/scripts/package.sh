#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Build the feature engineering job's artifacts and (optionally) upload them.
#
#   ./scripts/package.sh                                  # build into ./dist
#   ./scripts/package.sh s3://<bucket>/ts05_features/     # build and upload
#
# Produces:
#   dist/ts05_features.zip          -> --py-files (the package)
#   dist/pydeps.zip                 -> --py-files (vendored PyYAML)
#   dist/feature_engineering_job.py -> the spark-submit entry point
#   dist/conf/*.yaml                -> --files / --config
#
# Same conventions as project_lotus/sagemaker/scripts/package.sh:
# * a zip of the package rather than a wheel - --py-files handles zips natively
#   on every EMR release and needs no pip install on the executors;
# * PyYAML vendored in pydeps.zip, because the config is read before there is a
#   SparkSession and the EMR driver Python does not ship it.
#
# The package is `ts05_features`, not `trust_score_05`, so this zip can sit
# beside the lineage/ML jobs' trust_score_05.zip without either shadowing the
# other.
# -----------------------------------------------------------------------------
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/.." && pwd)"
DIST="${DIST:-${ROOT}/dist}"
DEST="${1:-}"

PYTHON_BIN=""
for candidate in python3 python; do
  if command -v "${candidate}" >/dev/null 2>&1 && "${candidate}" -c 'import yaml' >/dev/null 2>&1; then
    PYTHON_BIN="${candidate}"
    break
  fi
done

if [[ -z "${PYTHON_BIN}" ]]; then
  echo "ERROR: PyYAML is not importable here, so it cannot be vendored into" >&2
  echo "       pydeps.zip. Install it (pip install pyyaml) and run this again." >&2
  exit 1
fi

rm -rf "${DIST}"
mkdir -p "${DIST}/conf"

# 1. the package, zipped from the folder root so the archive root is
#    `ts05_features/` and `import ts05_features.library` resolves on executors.
(
  cd "${ROOT}"
  find ts05_features -name '__pycache__' -type d -prune -exec rm -rf {} + 2>/dev/null || true
  zip -qr "${DIST}/ts05_features.zip" ts05_features -x '*.pyc' '*/docs/*'
)

# 1b. vendored PyYAML (from this build environment; no network needed).
YAML_DIR="$(${PYTHON_BIN} -c 'import os, yaml; print(os.path.dirname(yaml.__file__))' 2>/dev/null || true)"
if [[ -z "${YAML_DIR}" ]]; then
  echo "ERROR: PyYAML is not importable here, so it cannot be vendored into" >&2
  echo "       pydeps.zip. Install it (pip install pyyaml) and run this again." >&2
  exit 1
fi
(
  cd "$(dirname "${YAML_DIR}")"
  zip -qr "${DIST}/pydeps.zip" yaml -x '*.pyc' '*/__pycache__/*' '*.so' '*.pyd'
)

# 2. the entry point and the configs.
cp "${ROOT}/jobs/feature_engineering_job.py" "${DIST}/"
cp "${ROOT}/conf/"*.yaml "${DIST}/conf/"

# 3. refuse to ship a package whose feature logic drifted from the client's
#    (only where the tests are present - a deploy-only copy has no tests/).
if [[ ! -f "${ROOT}/tests/test_unchanged.py" ]]; then
  echo "NOTE: no tests/ folder here (deploy-only copy); skipped the pre-build checks" >&2
elif "${PYTHON_BIN}" -c 'import pytest' 2>/dev/null; then
  (cd "${ROOT}" && "${PYTHON_BIN}" -m pytest -q tests/test_unchanged.py tests/test_layout_and_params.py)
else
  echo "WARNING: pytest not installed; skipped tests/test_unchanged.py" >&2
fi

echo "built:"
find "${DIST}" -type f | sort | sed 's/^/  /'

if [[ -n "${DEST}" ]]; then
  echo
  echo "uploading to ${DEST}"
  aws s3 sync "${DIST}" "${DEST%/}/" --delete --no-verify-ssl
fi
