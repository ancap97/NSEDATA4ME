"""Golden outputs of the read API against a real data-branch snapshot (see tests/golden/golden.py).

Skipped unless NSE_GOLDEN_DATA points at a `data/` folder from the `data` branch, because the
snapshot (~430 MB) is not part of the repo and reading it takes a few minutes."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from tests.golden import golden

DATA = os.environ.get("NSE_GOLDEN_DATA")


@pytest.mark.skipif(not DATA, reason="set NSE_GOLDEN_DATA=<data branch checkout>/data to run")
def test_golden_outputs_unchanged():
    expected = json.loads(golden.HASH_FILE.read_text())
    actual, _ = golden.snapshot(Path(DATA))
    assert golden.compare(expected, actual) == []


def test_digest_is_sensitive_to_values_and_dtypes():
    import pandas as pd

    a = pd.DataFrame({"x": [1.0, 2.0]})
    assert golden.digest(a) == golden.digest(a.copy())
    assert golden.digest(a) != golden.digest(a.assign(x=[1.0, 2.0000001]))
    assert golden.digest(a) != golden.digest(a.astype("float32"))
    assert golden.digest(a) != golden.digest(a.iloc[::-1])
