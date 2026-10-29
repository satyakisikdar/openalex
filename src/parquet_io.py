"""Crash-safe Parquet writes."""

import os
from pathlib import Path
from typing import Any

import pandas as pd


def atomic_write_parquet(df: pd.DataFrame, target: Path, **to_parquet_kwargs: Any) -> None:
    """
    Write `df` to `<target>.partial-<pid>`, then atomically replace `target` with it.

    `target` is only ever absent, the previous file, or the complete new file, so "target exists" means
    "target is complete". Leftover partials from a killed run are removed first.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    # ponytail: assumes one writer per target (true here: one output per input part). Concurrent writers to
    # the same target would need a pid-liveness check before unlinking.
    for stale in target.parent.glob(f'{target.name}.partial-*'):
        stale.unlink(missing_ok=True)

    partial = target.with_name(f'{target.name}.partial-{os.getpid()}')
    try:
        df.to_parquet(partial, **to_parquet_kwargs)
        partial.replace(target)
    except BaseException:  # includes KeyboardInterrupt/SystemExit (e.g. from a SIGTERM handler)
        partial.unlink(missing_ok=True)
        raise
