"""Fast, side-effect-free tests for the ID and abstract parsers in src/utils.py."""

import pandas as pd

from src.utils import convert_openalex_id_to_int, convert_topic_id_to_int, reconstruct_abstract


def test_openalex_id_to_int() -> None:
    assert convert_openalex_id_to_int('https://openalex.org/W2741809807') == 2741809807
    assert convert_openalex_id_to_int(' A123 ') == 123
    assert convert_openalex_id_to_int(None) is None
    assert convert_openalex_id_to_int('https://openalex.org/Wnope') is None


def test_topic_id_to_int_handles_old_and_new_formats() -> None:
    assert convert_topic_id_to_int('https://openalex.org/T10102') == 10102
    assert convert_topic_id_to_int('https://openalex.org/topics/T10102') == 10102
    assert convert_topic_id_to_int('https://openalex.org/fields/17') == 17
    assert convert_topic_id_to_int('') is None


def test_reconstruct_abstract_inverts_index() -> None:
    inv = {'world': [1], 'hello': [0, 2]}
    assert reconstruct_abstract(inv) == 'hello world hello'
    assert reconstruct_abstract('{"b": [1], "a": [0]}') == 'a b'
    assert reconstruct_abstract(b'{"x": [0]}') == 'x'
    assert reconstruct_abstract('') is pd.NA
    assert reconstruct_abstract(None) is pd.NA


def test_peek_parquet_dir_with_manifest_outside_jupyter(tmp_path, capsys) -> None:
    from src.utils import peek_parquet

    part = tmp_path / 'authors' / 'updated_date=2026-04-08'
    part.mkdir(parents=True)
    pd.DataFrame({'id': ['https://openalex.org/A1', 'https://openalex.org/A2']}).to_parquet(part / 'part_0000.parquet')
    pd.DataFrame({'id': ['https://openalex.org/A3']}).to_parquet(part / 'part_0001.parquet')
    (tmp_path / 'authors' / 'manifest.json').write_text('{}')  # non-parquet file, as in the OpenAlex snapshot

    peek_parquet(tmp_path / 'authors')
    out = capsys.readouterr().out
    assert 'Files: 2' in out
    assert 'Rows: 3' in out
    assert 'https://openalex.org/A1' in out  # sample rows printed, not display()-ed
