"""Point every data path of the project at another directory, for tests and golden checks.

config.py builds all paths from DATA_DIR at import time and several modules copy them into
their own globals (`from config import STORE_DIR`) or into default arguments
(`Store(directory=STORE_DIR)`), so patching config alone is not enough. `repoint` walks the
project modules and replaces every Path under the original DATA_DIR - module globals, function
defaults and method defaults - with the same relative path under `new_dir`. Production code is
not modified; everything goes through the given pytest MonkeyPatch and is undone with it.
"""

from __future__ import annotations

import importlib
import inspect
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

PROJECT_MODULES = (
    "config", "parsers", "storage", "symbol_master", "scraper", "adjuster", "breadth",
    "pipeline", "loader", "bootstrap", "sync", "healthcheck",
)


def _mapper(old: Path, new: Path):
    def f(v):
        if isinstance(v, Path) and (v == old or old in v.parents):
            return new / v.relative_to(old)
        return None
    return f


def _patch_defaults(mp, func, remap) -> None:
    if not inspect.isfunction(func):
        return
    if func.__defaults__:
        vals = [remap(v) or v for v in func.__defaults__]
        if any(a is not b for a, b in zip(vals, func.__defaults__)):
            mp.setattr(func, "__defaults__", tuple(vals))
    if func.__kwdefaults__:
        kw = {k: remap(v) or v for k, v in func.__kwdefaults__.items()}
        if any(kw[k] is not func.__kwdefaults__[k] for k in kw):
            mp.setattr(func, "__kwdefaults__", kw)


def repoint(new_dir: Path, mp) -> Path:
    """Redirect all project data paths to `new_dir` (a stand-in for data/). Returns new_dir."""
    new_dir = Path(new_dir)
    config = importlib.import_module("config")
    remap = _mapper(config.DATA_DIR, new_dir)
    mods = [importlib.import_module(m) for m in PROJECT_MODULES]
    for mod in mods:
        for name, val in list(vars(mod).items()):
            p = remap(val)
            if p is not None:
                mp.setattr(mod, name, p)
            elif inspect.isfunction(val) and val.__module__ == mod.__name__:
                _patch_defaults(mp, val, remap)
            elif inspect.isclass(val) and val.__module__ == mod.__name__:
                for attr in vars(val).values():
                    if isinstance(attr, (classmethod, staticmethod)):
                        attr = attr.__func__
                    _patch_defaults(mp, attr, remap)
    return new_dir
