"""Process-level helpers for long-running jobs."""

import signal
from datetime import datetime
from types import FrameType


def exit_on_sigterm() -> None:
    """
    Turn SIGTERM (kill, timeout, scheduler preemption) into SystemExit(143).

    Python's default SIGTERM action kills the process without unwinding, so `except`/`finally` cleanup never
    runs and partial files are left behind. Raising SystemExit instead lets that cleanup run.
    """

    def _handler(signum: int, frame: FrameType | None) -> None:
        print(f'{datetime.now():%c} SIGTERM received; exiting early (run is incomplete).', flush=True)
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, _handler)
