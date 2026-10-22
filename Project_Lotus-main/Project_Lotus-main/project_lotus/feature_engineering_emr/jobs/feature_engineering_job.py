"""spark-submit entry point for the feature engineering job.

    spark-submit --py-files ts05_features.zip,pydeps.zip feature_engineering_job.py \
        --config base.yaml --config sandbox.yaml --mode training
    ... --mode scoring --batch-date 2026-10-01

Importing the job reads its configuration (the notebook functions bind it as
default arguments, so it must be in place before they are defined); a bad
configuration exits with code 2 right here, before Spark starts.

The exit code is how EMR - and the orchestrator - learn the outcome:
0 published + completion marker written, 1 something failed (no marker),
2 configuration error.
"""

import sys

from ts05_features.feature_engineering import main

if __name__ == "__main__":
    sys.exit(main())
