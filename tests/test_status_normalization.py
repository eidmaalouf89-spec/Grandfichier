"""
Tests for status normalization (processing/statuses.py)
"""
import json
import sys
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from processing.statuses import load_status_map, resolve_status, get_normalized_code

STATUS_MAP_PATH = Path(__file__).parent.parent / "data" / "status_map.json"


def _load():
    return load_status_map(STATUS_MAP_PATH)


def test_load_status_map():
    sm = _load()
    assert isinstance(sm, dict)
    assert "_NONE_" in sm
    assert "Validé avec observation" in sm


def test_vso():
    sm = _load()
    code = get_normalized_code("Validé sans observation", sm)
    assert code == "VSO"


def test_vao():
    sm = _load()
    code = get_normalized_code("Validé avec observation", sm)
    assert code == "VAO"


def test_ref():
    sm = _load()
    code = get_normalized_code("Refusé", sm)
    assert code == "REF"


def test_def():
    sm = _load()
    code = get_normalized_code("Défavorable", sm)
    assert code == "DEF"


def test_hm():
    sm = _load()
    code = get_normalized_code("Hors Mission", sm)
    assert code == "HM"


def test_hm_en_retard():
    """GED sometimes appends ' - En retard' suffix — must still normalize to HM."""
    sm = _load()
    code = get_normalized_code("Hors Mission - En retard", sm)
    assert code == "HM"


def test_sus():
    sm = _load()
    code = get_normalized_code("Suspendu", sm)
    assert code == "SUS"


def test_ann():
    sm = _load()
    code = get_normalized_code("Annulé", sm)
    assert code == "ANN"


def test_unknown_falls_back_to_none():
    sm = _load()
    entry, found = resolve_status("CompletelyUnknownValue", sm)
    assert not found
    assert entry["code"] == "NONE"


def test_none_input():
    sm = _load()
    code = get_normalized_code(None, sm)
    assert code == "NONE"


def test_empty_string():
    sm = _load()
    entry, found = resolve_status("", sm)
    assert entry["code"] == "NONE"
