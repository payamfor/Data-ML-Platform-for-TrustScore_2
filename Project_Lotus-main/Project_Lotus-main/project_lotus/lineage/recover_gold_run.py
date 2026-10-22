"""EMR entrypoint that only appends the terminal failed control record."""
from gold_recovery.finalize_cli import main

if __name__ == "__main__":
    raise SystemExit(main())
