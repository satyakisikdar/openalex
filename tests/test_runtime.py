"""SIGTERM must become a normal exit so cleanup (e.g. removing partial Parquet files) runs."""

import os
import signal
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from src.parquet_io import atomic_write_parquet
from src.runtime import exit_on_sigterm


@pytest.fixture(autouse=True)
def _restore_sigterm() -> Iterator[None]:
    old = signal.getsignal(signal.SIGTERM)
    yield
    signal.signal(signal.SIGTERM, old)


def test_sigterm_raises_system_exit_143(capsys: pytest.CaptureFixture[str]) -> None:
    exit_on_sigterm()
    with pytest.raises(SystemExit) as exc:
        os.kill(os.getpid(), signal.SIGTERM)
    assert exc.value.code == 143
    assert 'SIGTERM' in capsys.readouterr().out


def test_sigterm_mid_write_removes_partial(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    target = tmp_path / 'x.parquet'

    def killed_mid_write(self: pd.DataFrame, path: Path, **_: Any) -> None:
        Path(path).write_bytes(b'PAR1 truncated')
        os.kill(os.getpid(), signal.SIGTERM)

    exit_on_sigterm()
    monkeypatch.setattr(pd.DataFrame, 'to_parquet', killed_mid_write)
    with pytest.raises(SystemExit):
        atomic_write_parquet(pd.DataFrame({'a': [1, 2]}), target)
    assert not target.exists()
    assert list(tmp_path.iterdir()) == []
