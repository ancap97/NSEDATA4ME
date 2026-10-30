from __future__ import annotations

import numpy as np
import pandas as pd

import breadth
from tests import factory


def _assert_wide_equal(store, keys):
    new = breadth._load_wide(store, keys)
    old = breadth._load_wide_by_file(store, keys)
    for a, b in zip(new, old):
        # the old dict-of-Series union can infer a freq on a perfectly regular calendar; it is
        # not used downstream (test_compute_breadth_unchanged_by_scan compares the output)
        pd.testing.assert_frame_equal(a, b, check_exact=True, check_freq=False)
    return new


def test_load_wide_scan_matches_file_by_file(data_dir):
    from storage import Store

    factory.build_db()
    s = Store()
    keys = ["NEWNAME", "AAA", "DEAD", "NOFILE", "BBB", "MIG", "DUP"]
    _, _, close = _assert_wide_equal(s, keys)
    assert list(close.columns) == ["NEWNAME", "AAA", "DEAD", "BBB", "MIG", "DUP"]
    assert close.dtypes.unique().tolist() == [np.dtype("float32")] and close.index.name == "date"
    # adjusted: the AAA split ex 2024-01-08 leaves no gap in the adjusted close
    assert close["AAA"].diff().dropna().between(0.4, 0.6).all()


def test_load_wide_unsorted_file_falls_back_to_pandas(data_dir):
    from storage import Store, _atomic_parquet

    factory.build_db()
    s = Store()
    df = s.read("BBB")
    # a file not written by Store.write: unsorted, with a duplicated date (first row must win)
    bad = pd.concat([df.iloc[[3]].assign(close=999.0), df.iloc[::-1]], ignore_index=True)
    _atomic_parquet(bad, s.path("BBB"))
    _, _, close = _assert_wide_equal(s, ["AAA", "BBB"])
    assert close.loc[df["date"].iloc[3], "BBB"] == np.float32(999.0 / df["adj_factor"].iloc[3])


def test_compute_breadth_unchanged_by_scan(data_dir, monkeypatch):
    from storage import Store

    factory.build_db()
    fast = breadth.compute_breadth(Store())
    monkeypatch.setattr(breadth, "_load_wide", breadth._load_wide_by_file)
    pd.testing.assert_frame_equal(fast, breadth.compute_breadth(Store()), check_exact=True)
    assert list(fast.columns) == breadth.BREADTH_COLUMNS


def test_empty_store(data_dir):
    from storage import Store

    out = breadth.compute_breadth(Store())
    assert out.empty and list(out.columns) == breadth.BREADTH_COLUMNS
