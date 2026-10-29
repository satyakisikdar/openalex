"""Skip logic in filter_dataset: one already-complete table must not stop the others."""

from pathlib import Path

import pandas as pd

from preprocessing import filter_dataset as fd

KINDS = ['authorships', 'concepts', 'referenced_works']


def _row(kind: str) -> pd.DataFrame:
    """One synthetic row (work_id=1) matching the dtypes the function casts to."""
    sample = {'int64': 1, 'Int64': 1, 'Int16': 2020, 'uint8': 1, 'category': 'a', 'string[python]': 'a'}
    row = {col: sample.get(str(dt), 0.5) for col, dt in fd.dtypes[kind].items()}
    return pd.DataFrame([row]).astype(fd.dtypes[kind])


def test_complete_first_kind_does_not_skip_later_kinds(tmp_path: Path) -> None:
    whole, filt = tmp_path / 'whole', tmp_path / 'filt'
    for kind in KINDS:
        (whole / f'works_{kind}').mkdir(parents=True)
        _row(kind).to_parquet(whole / f'works_{kind}' / 'part_000.parquet')
    (filt / 'works_authorships').mkdir(parents=True)
    _row('authorships').to_parquet(filt / 'works_authorships' / 'part_000.parquet')  # already complete

    fd.write_other_filtered_tables_v2(whole_parq_path=whole, filt_parq_path=filt, work_ids=[1])

    for kind in ('concepts', 'referenced_works'):
        assert len(pd.read_parquet(filt / f'works_{kind}' / 'part_000.parquet')) == 1
