"""Corporate actions table and price adjustment factors.

Sources merged into data/actions/actions.parquet (priority high -> low):
  manual  data/actions/manual_overrides.csv  (user corrections)
  api     NSE corporate-actions API           (forward daily sync)
  pr      bc*.csv inside NSE PR bhavcopy zips (2011+, includes delisted names)

Purpose text parsing (split/bonus/consolidation regexes, exclusions for
debenture/preference/NCRPS/DVR bonuses, PR-text normalisation) is copied from
BennyThadikaran/eod2 and eod2_utils (GPL-3).

Adjustment model
  factor  SPLIT/CONSOLIDATION: old_face_value / new_face_value
          BONUS a:b           : 1 + a/b
  adj_factor(row) = product of factors of all actions with ex_date > row.date
  adjusted price  = raw price / adj_factor,  adjusted volume = raw * adj_factor
Rights issues and dividends are recorded (type RIGHTS / DIVIDEND) but never
applied automatically; a diagnostic flags large unexplained price gaps so
they can be reviewed and added as manual overrides.
"""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

from config import (
    ACTIONS_CSV,
    ACTIONS_DIR,
    ACTIONS_FILE,
    ADJ_CHECK_HIGH,
    ADJ_CHECK_LOW,
    ADJ_WARNINGS_FILE,
    MANUAL_OVERRIDES_FILE,
    SME_SERIES,
)
from parsers import parse_pr_actions
from storage import Store
from symbol_master import EntityResolver

logger = logging.getLogger("adjuster")

ACTION_COLUMNS = [
    "key",
    "symbol",
    "ex_date",
    "rec_date",
    "type",
    "factor",
    "amount",
    "purpose",
    "source",
    "hash",
]
ADJUSTING_TYPES = ("SPLIT", "BONUS", "CONSOLIDATION")
# PR purpose text is a truncated 25-char field, so the API-derived seed wins over it
SOURCE_PRIORITY = {"manual": 0, "api": 1, "seed": 2, "ref": 3, "pr": 4, "price": 5}

SPLIT_RE = re.compile(r"(\d+\.?\d*)[\/\- a-z\.]+(\d+\.?\d*)")
BONUS_RE = re.compile(r"(\d+\.?\d*) ?: ?(\d+\.?\d*)")
DIVIDEND_RE = re.compile(r"(?<!\d)(\d+(?:\.\d+)?)(?!\d|st\b|nd\b|rd\b|th\b)")
BONUS_EXCLUDE = ("deb", "pref", "ncrps", "dvr")

# ------------------------------------------------------------------ PR text normalisation (eod2_utils)
_REPLACE_MAP = {
    "RHTS": "RIGHTS", "GENREAL": "GENERAL", "FGENERAL": "GENERAL", "RGHTS": "RIGHTS", "RGTS": "RIGHTS",
    "RIGTS": "RIGHTS", "ARRNGMT": "ARRANGEMENT", "ARGMT": "ARRANGEMENT", "AGMT": "ARRANGEMENT",
    "ARRANGMNT": "ARRANGEMENT", "ARRNGMNT": "ARRANGEMENT", "ARNGMNT": "ARRANGEMENT", "ARNGMT": "ARRANGEMENT",
    "SCH": "SCHEME", "SCHM": "SCHEME", "CNSLDATN": "CONSOLIDATION", "CONSOLIDATIN": "CONSOLIDATION",
    "CONSO": "CONSOLIDATION", "AMLGMTN": "AMALGAMATION", "AMALGATION": "AMALGAMATION",
    "AMALAGMATION": "AMALGAMATION", "GENRAL": "GENERAL", "ANUUAL": "ANNUAL", "ANNAL": "ANNUAL",
    "ANNUL": "ANNUAL", "ANNAUL": "ANNUAL", "ANUAL": "ANNUAL", "MEEING": "MEETING", "MEETNG": "MEETING",
    "MEETINGQ": "MEETING", "MEETIG": "MEETING", "MEETIN": "MEETING", "MEEETING": "MEETING", "METING": "MEETING",
    "EETING": "MEETING", "SHAR": "SHARE", "SHR": "SHARE", "SHRE": "SHARE", "SAHR": "SHAREHOLDER", "SHAE": "SHARE",
    "SH": "SHARE", "SHA": "SHARE", "SPLDV": " DIVIDEND ", "SPLDIV": " DIVIDEND ", "DIVRS": " DIVIDEND ",
    "DIVRE": " DIVIDEND ", "SPDV": " DIVIDEND ", "AGMDIV": " DIVIDEND ", "DIVD": " DIVIDEND ",
    "SPDIV": " DIVIDEND ", "DIVDEND": " DIVIDEND ", "DIVINDEND": " DIVIDEND ", "FINDIV": " DIVIDEND ",
    "INTDV": " DIVIDEND ", "SPLRS": " DIVIDEND ", "DVSPDV": " DIVIDEND ", "IDV": " DIVIDEND ",
    "DIV-FINRS": " DIVIDEND ", "SPLDIVRS": " DIVIDEND ", "SPDIVRS": " DIVIDEND ", "DIVSPDV": " DIVIDEND ",
    "FDIV": " DIVIDEND ", "DIVIDNED": " DIVIDEND ", "SPLINTDIV": " DIVIDEND ", "DIVIVEND": " DIVIDEND ",
    "DIVIDND": " DIVIDEND ", "SPECIALDIV": " DIVIDEND ", "INTERIMD": " DIVIDEND ", "INTDIV": " DIVIDEND ",
    "DIV": " DIVIDEND ", "DIVI": " DIVIDEND ", "DIVID": " DIVIDEND ", "DIVIDEN": " DIVIDEN ", "RED": "CONSOLIDATION",
}
_WORD_BOUND = {"SH", "SHA", "SHR", "SHRE", "SHAR", "SCH", "SCHM", "CONSO", "MEETIN", "EETING", "DIV", "DIVI",
               "DIVID", "DIVIDEN", "FGENERAL", "RED"}
_PATTERN_RE = re.compile("(" + "|".join((rf"\b{k}\b" if k in _WORD_BOUND else k) for k in _REPLACE_MAP) + ")")
_DROP_PURPOSES = {
    "EGM", "EOGM", "EXTRA GENERAL MEETING", "EXTR-ORDNRY GNRL MEETING", "CAPITAL REDUCTION",
    "CAP. CONSOLIDATION /CONSOLIDATION", "CAP CONSOLIDATION /CONSOLIDATION", "CAP CONSOLIDATION/ CONSOLIDATION",
    "CAP CONSOLIDATION", "CONSOLIDATION/CAP CONSOLIDATION",
}


def normalise_pr_purpose(text: str) -> str:
    s = str(text).upper()
    s = s.replace("DIVISION", " ").replace("DE-MERGER", " DEMERGER ").replace("/-", " ")
    for a, b in (("FVSPLT", " FV SPLIT "), ("FVSPLIT", " FV SPLIT "), ("FVSPL", " FV SPLIT "), ("FVS ", " FV SPLIT "),
                 ("SPLT", " SPLIT "), ("BUY BACK", " BUYBACK "), ("BUY-BACK", " BUYBACK ")):
        s = s.replace(a, b)
    s = re.sub("REDTN|REDUCTN|REDN|REDCTN", " CONSOLIDATION ", s)
    s = re.sub("BON(?!US)", " BONUS ", s)
    s = _PATTERN_RE.sub(lambda m: _REPLACE_MAP[m.group()], s)
    s = s.replace("NCRPS", " NCRPS ")
    s = re.sub(r"\b\d+\s*(ST|ND|RD|TH)\b", " ", s)
    s = re.sub(r"(RE|RS)\.?(?=\d+)", " RS ", s)
    s = re.sub(r"FIN(?:-|\s)?(?:RS|RE)", " ", s)
    s = re.sub(r"\bFV SPL\b", " FV SPLIT ", s)
    s = re.sub(r"(?:DV|DI|FIN|SPL|INT)(?:-|\s)?(?:RS|RE)", " DIVIDEND RS ", s)
    return re.sub(r"\s+", " ", s).strip()


def split_combined_purpose(text: str) -> List[str]:
    """'BONUS 1:1 / FV SPLIT RS 10 TO RS 2' -> two purposes."""
    hits = sum(1 for k in ("BONUS", "SPLIT", "DIV") if k in text)
    if hits < 2:
        return [text]
    sep = "/" if "/" in text else "+" if "+" in text else None
    if sep is None:
        return [text]
    return [p.strip() for p in text.split(sep) if p.strip() and p.strip() not in ("AGM",)]


# ------------------------------------------------------------------ purpose classification (eod2)


def _split_factor(s: str) -> Optional[float]:
    m = SPLIT_RE.search(s)
    if not m:
        return None
    a, b = float(m.group(1)), float(m.group(2))
    if a <= 0 or b <= 0:
        return None
    return a / b


def _bonus_factor(s: str) -> Optional[float]:
    m = BONUS_RE.search(s)
    if not m:
        return None
    a, b = float(m.group(1)), float(m.group(2))
    if a <= 0 or b <= 0:
        return None
    return 1 + a / b


def classify(purpose: str) -> List[Tuple[str, Optional[float], Optional[float]]]:
    """Return list of (type, factor, amount) for a purpose string."""
    s = str(purpose).lower().strip()
    out: List[Tuple[str, Optional[float], Optional[float]]] = []
    if not s or s == "nan":
        return out
    if "[truncated]" in s:
        s = s.replace("[truncated]", "")
        is_div = "div" in s or "dv" in s
        if "consolidation" in s:
            typ = "CONSOLIDATION"
        elif "split" in s or "sub-division" in s or ("spl" in s and not is_div):
            typ = "SPLIT"
        elif "bonus" in s and not any(x in s for x in BONUS_EXCLUDE):
            typ = "BONUS"
        else:
            typ = None
        if typ:
            return [(f"{typ}_UNPARSED", None, None)]
        return classify(s)

    if "split" in s or "splt" in s or "consolidation" in s or "sub-division" in s or "subdivision" in s:
        if "consolidation" in s:
            i = s.index("consolidation")
            typ = "CONSOLIDATION"
        elif "spl" in s:
            i = s.index("spl")
            typ = "SPLIT"
        else:
            i = s.index("sub")
            typ = "SPLIT"
        f = _split_factor(s[i:])
        if f is not None:
            out.append((typ, f, None))
        else:
            out.append((f"{typ}_UNPARSED", None, None))

    # pre-2011 NSE abbreviations: "Spl-Rs10 To Rs2", "Bon 1:1" (eod2 make_adjustments fallbacks).
    # "SPL 7.5" next to a dividend is a *special* dividend, so require an explicit "X to Y".
    if (
        not any(t[0].startswith(("SPLIT", "CONSOLIDATION")) for t in out)
        and "spl" in s and "div" not in s and "dv" not in s
        and re.search(r"spl[^\d]*\d+\.?\d*\s*/?-?\s*(?:to|-)\s*(?:rs|re)?\.?\s*\d", s)
    ):
        f = _split_factor(s[s.index("spl"):])
        if f is not None:
            out.append(("SPLIT" if f >= 1 else "CONSOLIDATION", f, None))

    if "bonus" in s and not any(x in s for x in BONUS_EXCLUDE):
        f = _bonus_factor(s)
        out.append(("BONUS", f, None) if f is not None else ("BONUS_UNPARSED", None, None))
    elif "bonus" not in s and re.search(r"\bbon\b|bon-|bon ?\d", s) and not any(x in s for x in BONUS_EXCLUDE):
        f = _bonus_factor(s)
        if f is not None:
            out.append(("BONUS", f, None))

    if "right" in s or "rhts" in s:
        out.append(("RIGHTS", None, None))

    if not out and ("div" in s):
        nums = [float(x) for x in DIVIDEND_RE.findall(s)]
        out.append(("DIVIDEND", None, sum(nums) if nums else None))

    if not out and ("demerger" in s or "amalgamation" in s or "scheme" in s or "arrangement" in s):
        out.append(("SCHEME", None, None))

    if not out and ("buyback" in s or "buy back" in s or "buy-back" in s):
        out.append(("BUYBACK", None, None))

    return out


def _hash(key: str, ex_date: str, typ: str, factor) -> str:
    return hashlib.sha1(f"{key}|{ex_date}|{typ}|{factor}".encode()).hexdigest()[:16]


# ------------------------------------------------------------------ sources


def _pr_zip_date(p: Path):
    try:
        return pd.Timestamp(datetime.strptime(p.name[2:8], "%d%m%y").date())
    except ValueError:
        return pd.NaT


def load_pr_actions(zip_paths: Iterable[Path]) -> pd.DataFrame:
    """NSE lists forthcoming actions daily and corrects them over time, so the
    bc file published on the ex-date itself is the final version (eod2_utils
    rule). When no PR zip exists for an ex-date, the latest earlier listing is used."""
    frames = []
    for p in zip_paths:
        df = parse_pr_actions(p)
        if not df.empty:
            frames.append(df.assign(zip_date=_pr_zip_date(p)))
    if not frames:
        return pd.DataFrame(columns=["symbol", "series", "ex_date", "rec_date", "purpose", "source"])
    df = pd.concat(frames, ignore_index=True)
    df = df[df["zip_date"].notna() & (df["zip_date"] <= df["ex_date"])]
    # final listing = the latest daily file on/before the ex-date that carries the row
    # (the ex-date file itself sometimes omits it; earlier files may hold superseded text)
    last_zip = df.groupby(["symbol", "series", "ex_date"])["zip_date"].transform("max")
    df = df[df["zip_date"] == last_zip].copy()
    # withdrawn: dropped from the listings more than 3 days before the ex-date while later files exist
    zips = np.array(sorted(df["zip_date"].unique()), dtype="datetime64[ns]")
    ex = df["ex_date"].to_numpy(dtype="datetime64[ns]")
    lz = df["zip_date"].to_numpy(dtype="datetime64[ns]")
    later_file_exists = np.searchsorted(zips, ex, side="right") > np.searchsorted(zips, lz, side="right")
    stale = ((df["ex_date"] - df["zip_date"]).dt.days > 3).to_numpy() & later_file_exists
    if stale.any():
        logger.info("%d PR corporate-action listings withdrawn before their ex-date ignored", int(stale.sum()))
    df = df[~stale].drop(columns="zip_date")
    # PURPOSE is a fixed 25-char field: text that fills it and does not end in a
    # digit was cut off, so any ratio parsed from it is unreliable. Judge the
    # original NSE text, not the hard-coded corrections applied below.
    orig = df["purpose"].astype(str)
    truncated = orig.str.len().between(24, 25) & ~orig.str.rstrip().str[-1:].str.isdigit()
    df = _apply_pr_corrections(df)
    truncated &= df["purpose"].astype(str) == orig
    df.loc[truncated, "purpose"] = df.loc[truncated, "purpose"].astype(str) + " [TRUNCATED]"
    df["purpose"] = df["purpose"].map(normalise_pr_purpose)
    df = df[~df["purpose"].isin(_DROP_PURPOSES)]
    df = df.drop_duplicates(subset=["symbol", "series", "ex_date", "purpose"])
    # split combined purposes into separate rows
    df["purpose"] = df["purpose"].map(split_combined_purpose)
    df = df.explode("purpose", ignore_index=True)
    df["source"] = "pr"
    return df[["symbol", "series", "ex_date", "rec_date", "purpose", "source"]]


def _apply_pr_corrections(df: pd.DataFrame) -> pd.DataFrame:
    """Hard-coded fixes for truncated/incorrect PR purpose text (from eod2_utils)."""
    df = df.copy()
    fixes = {
        ("KSCL", "2014-01-27"): "Face Value Split From Rs 10/- To Rs 2/-",
        ("CONCOR", "2018-06-26"): "Face Value Split (Sub-Division) - From Rs 10/- Per Share To Rs 5/- Per Share",
        ("HDFCPVTBAN", "2024-02-02"): "Face Value Split (Sub-Division) - From Rs 216.75/- Per Share To Rs 21.675/- Per Share",
        ("HDFCNIFIT", "2024-02-02"): "Face Value Split (Sub-Division) - From Rs 299.92/- Per Share To Rs 29.992/- Per Share",
    }
    ex_str = df["ex_date"].dt.strftime("%Y-%m-%d")
    for (sym, ex), text in fixes.items():
        df.loc[(df["symbol"] == sym) & (ex_str == ex), "purpose"] = text
    rec_str = df["rec_date"].dt.strftime("%Y-%m-%d")
    for sym in ("HDFCNIFIT", "KRITIKA"):
        df.loc[(df["symbol"] == sym) & (rec_str == "2022-08-30"), "ex_date"] = pd.Timestamp("2022-08-30")
    return df


def api_rows_to_frame(rows: Iterable[dict]) -> pd.DataFrame:
    """NSE corporate-actions API rows -> raw action frame.

    Expected row shape (2024-2026): {"symbol", "series", "subject", "exDate": "05-Sep-2026",
    "recDate": "05-Sep-2026" | "-", "isin", ...}. If NSE changes the date format every
    row is skipped, so that case is logged loudly instead of silently returning nothing."""
    out = []
    rows = list(rows)
    unparsed = 0
    for r in rows:
        ex = r.get("exDate")
        if not ex or ex == "-":
            continue
        try:
            ex_dt = pd.Timestamp(datetime.strptime(ex, "%d-%b-%Y").date())
        except ValueError:
            unparsed += 1
            continue
        rec = r.get("recDate")
        try:
            rec_dt = pd.Timestamp(datetime.strptime(rec, "%d-%b-%Y").date()) if rec and rec != "-" else pd.NaT
        except ValueError:
            rec_dt = pd.NaT
        out.append(
            {
                "symbol": str(r.get("symbol", "")).strip(),
                "series": str(r.get("series", "EQ")).strip(),
                "ex_date": ex_dt,
                "rec_date": rec_dt,
                "purpose": str(r.get("subject", "")).strip(),
                "source": "api",
                "isin": r.get("isin"),
            }
        )
    if unparsed and not out:
        logger.error(
            "NSE actions API: %d rows but no exDate could be parsed (sample: %r) - has the API date format "
            "changed? Fix api_rows_to_frame in adjuster.py", unparsed, rows[0].get("exDate") if rows else None,
        )
    elif unparsed:
        logger.warning("NSE actions API: %d of %d rows had an unparsable exDate", unparsed, len(rows))
    return pd.DataFrame(out, columns=["symbol", "series", "ex_date", "rec_date", "purpose", "source", "isin"])


def load_manual_overrides() -> pd.DataFrame:
    """CSV columns: symbol, ex_date, type, factor, action(add|ignore), note."""
    if not MANUAL_OVERRIDES_FILE.exists():
        return pd.DataFrame(columns=["symbol", "ex_date", "type", "factor", "action", "note"])
    df = pd.read_csv(MANUAL_OVERRIDES_FILE, dtype=str).fillna("")
    df.columns = [c.strip().lower() for c in df.columns]
    df["symbol"] = df["symbol"].str.upper().str.strip()
    df["ex_date"] = pd.to_datetime(df["ex_date"], errors="coerce")
    df["type"] = df["type"].str.upper().str.strip()
    df["factor"] = pd.to_numeric(df["factor"], errors="coerce")
    df["action"] = df["action"].str.lower().str.strip().replace("", "add")
    return df[df["ex_date"].notna()]


# ------------------------------------------------------------------ table build


def _resolve_keys(df: pd.DataFrame, resolver: EntityResolver) -> pd.Series:
    cache: Dict[Tuple[str, bool, str], Optional[str]] = {}
    keys = []
    for sym, series, ex, src in zip(df["symbol"], df["series"], df["ex_date"], df["source"]):
        sme = series in SME_SERIES
        on = None if src == "seed" else (ex.date() if pd.notna(ex) else None)
        ck = (sym, sme, on.isoformat() if on else "")
        if ck not in cache:
            k = resolver.key_for_symbol(sym, on=on, sme=sme)
            if k is None and on is not None:
                k = resolver.key_for_symbol(sym, on=None, sme=sme)
            cache[ck] = k
        keys.append(cache[ck])
    return pd.Series(keys, index=df.index, dtype="object")




def _candidate_ratios() -> List[Tuple[float, int, str]]:
    """(ratio, complexity, label) for plausible actions. Lower complexity =
    more common (pure splits, bonus 1:1 / 1:2 ...); combos = split x bonus."""
    out: Dict[float, Tuple[int, str]] = {}

    def add(f: float, cx: int, label: str) -> None:
        f = round(f, 9)
        if f == 1.0:
            return
        if f not in out or cx < out[f][0]:
            out[f] = (cx, label)

    fvs = (1, 2, 4, 5, 10, 100)
    splits = [(a / b, f"split {a}->{b}") for a in fvs for b in fvs if a != b]
    for f, lab in splits:
        add(f, 1, lab)
    bonuses = [(1 + a / b, a + b, f"bonus {a}:{b}") for a in range(1, 6) for b in range(1, 6)]
    for f, cx, lab in bonuses:
        add(f, cx, lab)
    common_splits = [(sf, slab) for sf, slab in splits if sf in (2.0, 2.5, 5.0, 10.0)]
    for sf, slab in common_splits:
        for bf, bcx, blab in bonuses:
            if bf <= 3 and bcx <= 5:
                add(sf * bf, 10 + bcx, f"{slab} + {blab}")
    return sorted((f, cx, lab) for f, (cx, lab) in out.items())


def _snap_inferred(r_est: float, disagreement: float) -> Tuple[float, str, str]:
    """Return (factor, confidence, label). Tolerance tiers: >=4x 8%, 2-4x 5%,
    <2x 4%. A pure split or bonus within tolerance always wins (closest
    fit); combined split+bonus ratios are only reported as labels and the
    measured ratio is kept, marked low confidence for review."""
    mag = max(r_est, 1 / r_est)
    tol = 0.08 if mag >= 4 else 0.05 if mag >= 2 else 0.04
    pure, combo = [], []
    for f, cx, lab in _candidate_ratios():
        dev = abs(np.log(f / r_est))
        if dev <= tol:
            (combo if cx >= 10 else pure).append((dev, cx, f, lab))
    if pure:
        pure.sort()
        dev, cx, f, lab = pure[0]
        conf = "high" if (dev <= 0.03 and disagreement <= 0.035) else "medium"
        return f, conf, lab
    if combo:
        combo.sort()
        alts = ", ".join(f"{w[3]} ({w[2]:.4g})" for w in combo[:3])
        return round(r_est, 4), "low", f"combo candidates: {alts}"
    return round(r_est, 4), "low", "unsnapped (no simple ratio within tolerance)"


def reconcile_ex_dates(actions: pd.DataFrame, store: Store, window: int = 25, tol: float = 0.15) -> pd.DataFrame:
    """Move an adjusting action's ex_date to the trading day where the raw
    close actually dropped by ~1/factor, if the recorded date does not show it.

    Sources disagree on ex-dates by a day or two surprisingly often (record
    date vs ex date, postponed actions). Applying the factor from the wrong day
    corrupts every price in between, so the price series is the arbiter.
    Rows are re-dated in place; `purpose` gets a note and the original date is
    kept in `orig_ex_date`.
    """
    actions = actions.copy()
    if "orig_ex_date" not in actions:
        actions["orig_ex_date"] = actions["ex_date"]
    adj = actions[actions["type"].isin(ADJUSTING_TYPES) & actions["factor"].notna() & actions["key"].notna()]
    shifted = 0
    unconfirmed: List[int] = []
    unconfirmed_rows: List[dict] = []
    for key, grp in adj.groupby("key"):
        if not store.exists(key):
            continue
        df = store.read(key, columns=["date", "close"])
        if len(df) < 3:
            continue
        dates = df["date"].to_numpy(dtype="datetime64[ns]")
        close = df["close"].to_numpy(dtype="float64")
        with np.errstate(divide="ignore", invalid="ignore"):
            logr = np.log(close[1:] / close[:-1])  # logr[i] = move into row i+1
        for idx, r in grp.iterrows():
            ex = np.datetime64(r.ex_date, "ns")
            if ex <= dates[0] or ex > dates[-1]:
                continue
            # combined expected move on this date (several actions may share it)
            same_day = grp[grp["ex_date"] == r.ex_date]
            expected = -np.log(float(same_day["factor"].prod()))
            if abs(expected) < 0.15:
                continue  # <~16% moves are indistinguishable from normal volatility
            pos = int(np.searchsorted(dates, ex, side="left"))  # first row on/after ex
            if pos == 0:
                continue
            here = logr[pos - 1]
            # ex-day returns of +-12% on top of the adjustment are ordinary volatility
            slack = abs(expected) * tol + 0.12
            if np.isfinite(here) and abs(here - expected) <= slack:
                continue  # recorded date agrees with prices
            lo, hi = max(1, pos - window), min(len(dates) - 1, pos + window)
            cand = np.arange(lo, hi + 1)
            err = np.abs(logr[cand - 1] - expected)
            err[~np.isfinite(err)] = np.inf
            best = cand[int(np.argmin(err))]
            if err.min() <= slack:
                new_ex = pd.Timestamp(dates[best])
                actions.at[idx, "ex_date"] = new_ex
                actions.at[idx, "purpose"] = f"{r.purpose} [ex-date {pd.Timestamp(r.ex_date).date()} -> {new_ex.date()} by price check]"
                shifted += 1
                logger.info("%s %s: ex-date %s -> %s (price drop matches factor %.4g)", key, r.type, pd.Timestamp(r.ex_date).date(), new_ex.date(), r.factor)
            elif r.source != "manual" and np.isfinite(here) and abs(here) >= 0.2 and len(same_day) == 1:
                # the recorded date shows a large clean move of a *different* size:
                # the announced ratio is wrong (e.g. "1:2" for a 1:1 bonus) - adopt the price ratio
                f_obs, conf, label = _snap_inferred(float(np.exp(-here)), 0.0)
                if conf != "low":
                    actions.at[idx, "factor"] = f_obs
                    actions.at[idx, "purpose"] = f"{r.purpose} [factor {r.factor:g} -> {f_obs:g} by price check: {label}]"
                    actions.at[idx, "hash"] = _hash(key, pd.Timestamp(r.ex_date).strftime("%Y-%m-%d"), r.type, round(f_obs, 6))
                    logger.info("%s %s %s: factor %g -> %g (%s)", key, r.type, pd.Timestamp(r.ex_date).date(), r.factor, f_obs, label)
                    shifted += 1
                else:
                    unconfirmed.append(idx)
                    unconfirmed_rows.append(
                        {
                            "key": key, "ex_date": pd.Timestamp(r.ex_date).date().isoformat(), "type": r.type,
                            "factor": r.factor, "source": r.source, "purpose": r.purpose,
                            "observed_move_on_date": round(float(np.exp(here)), 4), "expected_move": round(float(np.exp(expected)), 4),
                        }
                    )
            elif r.source != "manual":
                # an announced action of this size that leaves no trace in prices within
                # +-window trading days was postponed/withdrawn (or is a source error)
                unconfirmed.append(idx)
                unconfirmed_rows.append(
                    {
                        "key": key, "ex_date": pd.Timestamp(r.ex_date).date().isoformat(), "type": r.type,
                        "factor": r.factor, "source": r.source, "purpose": r.purpose,
                        "observed_move_on_date": round(float(np.exp(here)), 4) if np.isfinite(here) else None,
                        "expected_move": round(float(np.exp(expected)), 4),
                    }
                )
    if shifted:
        logger.warning("%d corporate action ex-dates re-aligned to the price series", shifted)
    if unconfirmed:
        ACTIONS_DIR.mkdir(parents=True, exist_ok=True)
        rep = ACTIONS_DIR / "unconfirmed_actions.csv"
        prev = pd.read_csv(rep) if rep.exists() else pd.DataFrame()
        pd.concat([prev, pd.DataFrame(unconfirmed_rows)], ignore_index=True).drop_duplicates(
            subset=["key", "ex_date", "type", "factor"]
        ).to_csv(rep, index=False)
        logger.warning("%d announced actions not confirmed by any price move dropped (actions/unconfirmed_actions.csv)", len(unconfirmed))
        actions = actions.drop(index=unconfirmed)
    return actions


def build_actions_table(
    resolver: EntityResolver,
    raw_sources: List[pd.DataFrame],
    existing: Optional[pd.DataFrame] = None,
    store: Optional[Store] = None,
) -> pd.DataFrame:
    """Classify raw purpose rows, resolve entity keys, merge & dedupe."""
    frames = [f for f in raw_sources if f is not None and not f.empty]
    if existing is not None and not existing.empty:
        keep = ACTION_COLUMNS + (["orig_ex_date"] if "orig_ex_date" in existing.columns else [])
        frames.append(existing.loc[:, keep])

    typed = []
    for f in frames:
        if "type" in f.columns:  # already classified table
            f = f.copy()
            if "orig_ex_date" in f:  # undo earlier price re-alignment; it is redone below
                f["ex_date"] = f["orig_ex_date"].fillna(f["ex_date"])
                f["purpose"] = f["purpose"].astype(str).str.replace(r" \[ex-date .*? by price check\]", "", regex=True)
            typed.append(f.loc[:, ACTION_COLUMNS])
            continue
        f = f.copy()
        f["symbol"] = f["symbol"].astype(str).str.upper().str.strip()
        f = f[f["ex_date"].notna()]
        f["key"] = _resolve_keys(f, resolver)
        f["parsed"] = f["purpose"].map(classify)
        f = f[f["parsed"].map(len) > 0]
        f = f.explode("parsed", ignore_index=True)
        f["type"] = f["parsed"].map(lambda t: t[0])
        f["factor"] = f["parsed"].map(lambda t: t[1]).astype("float64")
        f["amount"] = f["parsed"].map(lambda t: t[2]).astype("float64")
        f["rec_date"] = pd.to_datetime(f["rec_date"], errors="coerce")
        f["hash"] = [
            _hash(k or s, ex.strftime("%Y-%m-%d"), t, fa)
            for k, s, ex, t, fa in zip(f["key"], f["symbol"], f["ex_date"], f["type"], f["factor"].round(6))
        ]
        typed.append(f.loc[:, ACTION_COLUMNS])

    if not typed:
        return pd.DataFrame(columns=ACTION_COLUMNS)

    # manual overrides take part from the start so price checks see the full picture
    manual = load_manual_overrides()
    if not manual.empty:
        manual["key"] = [resolver.key_for_symbol(s) or s for s in manual["symbol"]]
        add = manual[manual["action"] == "add"]
        if len(add):
            add_rows = pd.DataFrame(
                {
                    "key": add["key"], "symbol": add["symbol"], "ex_date": add["ex_date"], "rec_date": pd.NaT,
                    "type": add["type"], "factor": add["factor"], "amount": np.nan,
                    "purpose": "MANUAL: " + add["note"].astype(str), "source": "manual",
                }
            )
            add_rows["hash"] = [
                _hash(k, e.strftime("%Y-%m-%d"), t, f) for k, e, t, f in zip(add_rows["key"], add_rows["ex_date"], add_rows["type"], add_rows["factor"])
            ]
            typed.append(add_rows.loc[:, ACTION_COLUMNS])

    df = pd.concat(typed, ignore_index=True)
    df["ex_date"] = pd.to_datetime(df["ex_date"]).astype("datetime64[ns]")
    df["rec_date"] = pd.to_datetime(df["rec_date"], errors="coerce").astype("datetime64[ns]")
    df["_prio"] = df["source"].map(SOURCE_PRIORITY).fillna(9)
    df["_ident"] = df["key"].fillna("?" + df["symbol"].astype(str))
    if not manual.empty:
        ign = manual[manual["action"] == "ignore"]
        if len(ign):
            ign_set = set(zip(ign["key"], ign["ex_date"], ign["type"]))
            mask = np.array([(k, e, t) in ign_set for k, e, t in zip(df["_ident"], df["ex_date"], df["type"])], dtype=bool)
            df = df[~mask]
    df = df.sort_values(["_ident", "ex_date", "type", "_prio"], kind="stable")

    # one row per (entity, ex_date, type); keep the highest-priority source
    dedup = df.drop_duplicates(subset=["_ident", "ex_date", "type"], keep="first")
    if store is not None:
        dedup = fill_unparsed_from_prices(dedup, store)
        # filling may turn an UNPARSED row into a duplicate of a parsed one on the same date
        dedup = dedup.sort_values(["_ident", "ex_date", "type", "_prio"], kind="stable")
        dedup = dedup.drop_duplicates(subset=["_ident", "ex_date", "type"], keep="first")
        dedup = reconcile_ex_dates(dedup, store)
        dedup = dedup.sort_values(["_ident", "ex_date", "type", "_prio"], kind="stable")
        dedup = dedup.drop_duplicates(subset=["_ident", "ex_date", "type"], keep="first")
    dedup = _drop_near_duplicates(dedup)

    # detect factor disagreements between sources (reported, best source wins)
    adj = df[df["type"].isin(ADJUSTING_TYPES) & df["factor"].notna()]
    grp = adj.groupby(["_ident", "ex_date", "type"])["factor"].agg(["min", "max", "count"])
    conflicts = grp[(grp["count"] > 1) & ((grp["max"] / grp["min"] - 1).abs() > 1e-6)]
    if len(conflicts):
        logger.warning("%d corporate actions have conflicting factors across sources (see actions_conflicts.csv)", len(conflicts))
        ACTIONS_DIR.mkdir(parents=True, exist_ok=True)
        conflicts.reset_index().to_csv(ACTIONS_DIR / "actions_conflicts.csv", index=False)

    dedup = dedup.drop(columns=["_prio", "_ident"]).sort_values(["ex_date", "symbol"]).reset_index(drop=True)
    if "orig_ex_date" not in dedup:
        dedup["orig_ex_date"] = dedup["ex_date"]
    return dedup.loc[:, ACTION_COLUMNS + ["orig_ex_date"]]


def _drop_near_duplicates(df: pd.DataFrame, window_days: int = 35, factor_tol: float = 0.015) -> pd.DataFrame:
    """Sources sometimes record the same split/bonus with ex-dates days or
    weeks apart (record date vs ex date, postponements) or under a different
    label (a reference step typed SPLIT vs the announced BONUS). Applying both
    would double-adjust, so keep only the highest-priority row per (entity,
    factor) within the window. Rows on the *same* ex-date are never merged:
    "Bonus 1:1 and FV split 10->5" are two genuine x2 actions."""
    adj = df["type"].isin(ADJUSTING_TYPES) & df["factor"].notna()
    keep = np.ones(len(df), dtype=bool)
    # highest priority first; within a source prefer the later listing (NSE corrections postpone dates)
    sub = df[adj].sort_values(["_ident", "_prio", "ex_date"], ascending=[True, True, False], kind="stable")
    kept: Dict[str, List[Tuple[pd.Timestamp, float, int]]] = {}
    for pos, (ident, typ, ex, factor) in zip(
        sub.index, zip(sub["_ident"], sub["type"], sub["ex_date"], sub["factor"])
    ):
        lst = kept.setdefault(ident, [])
        dup = any(
            ex != kex and abs((ex - kex).days) <= window_days and abs(np.log(kf / factor)) < factor_tol
            for kex, kf, _ in lst
        )
        if dup:
            keep[df.index.get_loc(pos)] = False
            logger.info("near-duplicate action dropped: %s %s %s x%s", ident, typ, ex.date(), factor)
        else:
            lst.append((ex, factor, pos))
    return df[keep]


# ratios real corporate actions produce (splits, consolidations, bonuses)
_FACE_VALUES = (1, 2, 4, 5, 10, 100)
_SPLIT_RATIOS = np.array(sorted({a / b for a in _FACE_VALUES for b in _FACE_VALUES if a != b}))
_BONUS_RATIOS = np.array(sorted({1 + a / b for a in range(1, 11) for b in range(1, 11)}))


def _snap_typed(implied: float, base: str) -> Optional[float]:
    """Snap to a plausible ratio for the action type. Large factors (>=4) have
    widely spaced candidates so a 6% same-day market move is tolerated;
    smaller ones must match within 2.5%."""
    cands = _SPLIT_RATIOS if base in ("SPLIT", "CONSOLIDATION") else _BONUS_RATIOS
    i = int(np.argmin(np.abs(np.log(cands) - np.log(implied))))
    c = float(cands[i])
    tol = 0.06 if max(c, 1 / c) >= 4 else 0.025
    return c if abs(np.log(c / implied)) <= tol else None


def fill_unparsed_from_prices(actions: pd.DataFrame, store: Store, min_move: float = 0.15) -> pd.DataFrame:
    """Truncated PR text leaves *_UNPARSED actions without a factor. When the
    raw close moves by an unambiguous clean ratio on the ex-date, use it."""
    actions = actions.copy()
    unp = actions["type"].str.endswith("_UNPARSED") & actions["key"].notna()
    for key, grp in actions[unp].groupby("key"):
        if not store.exists(key):
            continue
        df = store.read(key, columns=["date", "close"])
        dates = df["date"].to_numpy(dtype="datetime64[ns]")
        close = df["close"].to_numpy(dtype="float64")
        for idx, r in grp.iterrows():
            ex = np.datetime64(r.ex_date, "ns")
            pos = int(np.searchsorted(dates, ex, side="left"))
            if pos <= 0 or pos >= len(dates) or (dates[pos] - ex) > np.timedelta64(7, "D"):
                continue
            if close[pos - 1] <= 0 or close[pos] <= 0:
                continue
            implied = close[pos - 1] / close[pos]
            if abs(np.log(implied)) < min_move:
                continue
            base = r.type.replace("_UNPARSED", "")
            f = _snap_typed(implied, base)
            if f is None:
                continue
            if base == "SPLIT" and f < 1:
                base = "CONSOLIDATION"
            actions.at[idx, "type"] = base
            actions.at[idx, "factor"] = f
            actions.at[idx, "purpose"] = f"{r.purpose} [factor {f:g} from price move {implied:.3f}]"
            actions.at[idx, "hash"] = _hash(key, pd.Timestamp(r.ex_date).strftime("%Y-%m-%d"), base, round(f, 6))
            logger.info("%s %s: factor %g taken from price move on %s", key, base, f, pd.Timestamp(r.ex_date).date())
    return actions


def save_actions(df: pd.DataFrame) -> None:
    ACTIONS_DIR.mkdir(parents=True, exist_ok=True)
    tmp = ACTIONS_FILE.with_suffix(".tmp")
    df.to_parquet(tmp, index=False)
    tmp.replace(ACTIONS_FILE)
    df.to_csv(ACTIONS_CSV, index=False)


def load_actions() -> pd.DataFrame:
    if not ACTIONS_FILE.exists():
        return pd.DataFrame(columns=ACTION_COLUMNS)
    return pd.read_parquet(ACTIONS_FILE)


# ------------------------------------------------------------------ factor computation


def compute_adj_factor(dates: pd.Series, actions: pd.DataFrame) -> np.ndarray:
    """adj_factor per row = product of factors with ex_date strictly after the row date."""
    n = len(dates)
    if actions is None or actions.empty:
        return np.ones(n)
    acts = actions[actions["type"].isin(ADJUSTING_TYPES) & actions["factor"].notna() & (actions["factor"] > 0)]
    if acts.empty:
        return np.ones(n)
    acts = acts.sort_values("ex_date")
    ex = acts["ex_date"].to_numpy(dtype="datetime64[ns]")
    f = acts["factor"].to_numpy(dtype="float64")
    suffix = np.concatenate([np.cumprod(f[::-1])[::-1], [1.0]])
    pos = np.searchsorted(ex, dates.to_numpy(dtype="datetime64[ns]"), side="right")
    return suffix[pos]


def apply_adjustments(
    store: Store,
    actions: pd.DataFrame,
    keys: Optional[Iterable[str]] = None,
    progress=None,
) -> Tuple[int, pd.DataFrame]:
    """Recompute adj_factor for the given keys (default: all). Returns
    (files rewritten, verification warnings DataFrame)."""
    all_keys = list(keys) if keys is not None else store.list_keys()
    by_key = {k: g for k, g in actions[actions["key"].notna()].groupby("key")}
    rewritten = 0
    warnings: List[dict] = []

    for i, key in enumerate(all_keys, 1):
        if not store.exists(key):
            continue
        df = store.read(key)
        if df.empty:
            continue
        acts = by_key.get(key)
        new = compute_adj_factor(df["date"], acts)
        old = df["adj_factor"].to_numpy(dtype="float64")
        if not np.allclose(new, old, rtol=1e-9, atol=0, equal_nan=False):
            df["adj_factor"] = new
            store.write(key, df)
            rewritten += 1
        if acts is not None:
            warnings.extend(verify_key(key, df, acts))
        if progress and (i % 200 == 0 or i == len(all_keys)):
            progress(i, len(all_keys), key)

    warn_df = pd.DataFrame(
        warnings,
        columns=["key", "ex_date", "type", "factor", "source", "ratio", "adj_close_ex", "adj_close_prev", "purpose", "issue"],
    )
    return rewritten, warn_df


def verify_key(key: str, df: pd.DataFrame, acts: pd.DataFrame) -> List[dict]:
    """Check adjusted close continuity across each adjusting ex-date."""
    out = []
    adj_close = (df["close"] / df["adj_factor"]).to_numpy()
    dates = df["date"].to_numpy(dtype="datetime64[ns]")
    for r in acts[acts["type"].isin(ADJUSTING_TYPES)].itertuples():
        if pd.isna(r.factor):
            out.append(_warn(key, r, np.nan, np.nan, np.nan, "factor not parsed"))
            continue
        ex = np.datetime64(r.ex_date, "ns")
        if ex <= dates[0] or ex > dates[-1]:
            continue  # outside history -> nothing to verify (no effect on stored rows)
        pos = int(np.searchsorted(dates, ex, side="left"))
        if pos == 0:
            continue
        ratio = adj_close[pos] / adj_close[pos - 1]
        if not (ADJ_CHECK_LOW < ratio < ADJ_CHECK_HIGH):
            out.append(_warn(key, r, ratio, adj_close[pos], adj_close[pos - 1], "adjusted close jump across ex-date"))
    return out


def _warn(key, r, ratio, c1, c0, issue) -> dict:
    return {
        "key": key,
        "ex_date": pd.Timestamp(r.ex_date).date().isoformat(),
        "type": r.type,
        "factor": r.factor,
        "source": r.source,
        "ratio": None if pd.isna(ratio) else round(float(ratio), 4),
        "adj_close_ex": None if pd.isna(c1) else round(float(c1), 2),
        "adj_close_prev": None if pd.isna(c0) else round(float(c0), 2),
        "purpose": r.purpose,
        "issue": issue,
    }


def save_warnings(warn_df: pd.DataFrame) -> None:
    ADJ_WARNINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    warn_df.to_csv(ADJ_WARNINGS_FILE, index=False)
