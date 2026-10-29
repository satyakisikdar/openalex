"""Crash-safety tests for atomic_write_parquet: the target is either absent, the old file, or the new file."""

from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from src.parquet_io import atomic_write_parquet


def _df(n: int = 2) -> pd.DataFrame:
    return pd.DataFrame({'work_id': list(range(n)), 'title': [f't{i}' for i in range(n)]})


def _partials(target: Path) -> list[Path]:
    return list(target.parent.glob(f'{target.name}.partial-*'))


def _crash_mid_write(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make to_parquet write a truncated file and then die, like a crash halfway through."""

    def fake(self: pd.DataFrame, path: Path, **_: Any) -> None:
        Path(path).write_bytes(b'PAR1 truncated')
        raise RuntimeError('simulated crash')

    monkeypatch.setattr(pd.DataFrame, 'to_parquet', fake)


def test_happy_path_writes_target_and_leaves_no_partial(tmp_path: Path) -> None:
    target = tmp_path / 'works' / '2026-03-01_part_000.parquet'
    atomic_write_parquet(_df(), target, compression='brotli')
    assert pd.read_parquet(target).shape == (2, 2)
    assert _partials(target) == []


def test_crash_leaves_no_target_and_no_partial(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    target = tmp_path / 'x.parquet'
    _crash_mid_write(monkeypatch)
    with pytest.raises(RuntimeError, match='simulated crash'):
        atomic_write_parquet(_df(), target)
    assert not target.exists()  # so the `exists()` skip check never sees a half-written file
    assert _partials(target) == []


def test_crash_never_clobbers_existing_target(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    target = tmp_path / 'x.parquet'
    atomic_write_parquet(_df(3), target)
    _crash_mid_write(monkeypatch)
    with pytest.raises(RuntimeError):
        atomic_write_parquet(_df(5), target)
    monkeypatch.undo()
    assert len(pd.read_parquet(target)) == 3


def test_rerun_after_kill_cleans_stale_partial(tmp_path: Path) -> None:
    target = tmp_path / 'x.parquet'
    stale = tmp_path / 'x.parquet.partial-424242'  # left by a SIGKILLed / OOM-killed run
    stale.write_bytes(b'PAR1 truncated')
    atomic_write_parquet(_df(), target)
    assert len(pd.read_parquet(target)) == 2
    assert _partials(target) == []
