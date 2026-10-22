"""EMR spark-submit entry point; the Python package is supplied via --py-files.

Staged example (repeat with accounts, customers, validate, then publish):
  --config s3://lotus-sandbox-gold-curated-data/pipeline-code/gold/v1/lotus_sandbox.yaml
  --job lineage --stage prepare --run-id gold-20260925-001

With --stage, five separate Spark applications share one immutable S3 handoff
and publish after all DQ passes. Without --stage, the original single-job path
is retained. --dry-run validates but never publishes or consumes a watermark.
The optional --job imei-map builds the separate sixth device map after lineage.
"""

import argparse
import sys


def main(argv=None):
    # Parse just the selector here. The shared client driver handles --config,
    # --set, --first-run, watermark bounds, repairs and --dry-run.
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--job", choices=("lineage", "imei-map"), default="lineage")
    parser.add_argument("--stage", choices=("prepare", "accounts", "customers", "validate", "publish"))
    selected, remaining = parser.parse_known_args(argv)
    if selected.stage is not None:
        if selected.job != "lineage":
            parser.error("--stage is available only for --job lineage")
        from trust_score_05.lineage.jobs.staged_job import main_cli
        remaining = ["--stage", selected.stage, *remaining]
    elif selected.job == "lineage":
        from trust_score_05.lineage.jobs.lineage_job import main_cli
    else:
        from trust_score_05.lineage.jobs.imei_map_job import main_cli
    return main_cli(remaining)


if __name__ == "__main__":
    sys.exit(main())
