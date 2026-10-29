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
