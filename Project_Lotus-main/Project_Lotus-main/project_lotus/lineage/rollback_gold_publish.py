"""EMR entrypoint for exact-snapshot Gold publish rollback only."""
from gold_recovery.rollback_cli import main


if __name__ == "__main__":
    raise SystemExit(main())
