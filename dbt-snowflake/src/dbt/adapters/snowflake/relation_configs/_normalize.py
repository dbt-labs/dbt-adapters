from typing import Optional

# Readback spellings that mean "this is not set".
ABSENT = {"", "none"}


def absent_to_none(value: Optional[str]) -> Optional[str]:
    """Collapse the wire spellings of "not set" to None, at LOAD time.

    Deliberately does NOT casefold: that's a comparison concern owned by
    `normalize_warehouse`. This is only for values that are the wire
    spelling of ABSENCE -- Snowflake reads back `''` for an unset
    initialization warehouse, and that must become `None` at load so
    absence detection works. Anything else is stored byte-faithful to what
    Snowflake reported.
    """
    if value is None:
        return None
    stripped = value.strip()
    if stripped.casefold() in ABSENT:
        return None
    return stripped


def normalize_warehouse(value: Optional[str]) -> Optional[str]:
    """Snowflake folds unquoted identifiers to upper case, so warehouse names
    must compare case-insensitively."""
    if value is None:
        return None
    stripped = value.strip()
    if stripped.casefold() in ABSENT:
        return None
    return stripped.casefold()


def has_balanced_outer_parens(text: str) -> bool:
    """True when the `(` at index 0 is the one closed by the `)` at the final
    index -- i.e. nesting depth first returns to 0 exactly at the last character.

    A mere `startswith("(") and endswith(")")` check is NOT a balance check: the
    leading and trailing parens can belong to unrelated groups, e.g.
    `(a), to_date(ts)`.
    """
    if not (text.startswith("(") and text.endswith(")")):
        return False
    depth = 0
    for index, char in enumerate(text):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return index == len(text) - 1
    return False


def normalize_cluster_by(value: Optional[str]) -> Optional[str]:
    """`SHOW` returns clustering keys parenthesized -- `(id, name)` -- while the
    model config yields a bare `id, name`. Strip ONE balanced outer paren pair so
    the two compare equal, and collapse whitespace after commas.

    Deliberately NOT a strip-to-first-paren: that would corrupt an expression
    like `to_date(ts)`, which legitimately contains parens.

    `SHOW DYNAMIC TABLES` also prefixes that parenthesized list with `LINEAR` --
    `LINEAR(ID, VAL)` -- on readback; `SHOW INTERACTIVE TABLES` does not (bare
    parens, no `LINEAR`). Both confirmed live against a real warehouse (account
    `ktb38830`, 2026-08-27/31) -- see the fs (dbt-fusion) wiki page
    `pr-12664-review-followups` for the probe detail. Shared here so both object
    types tolerate the `LINEAR` spelling: it's real and necessary for dynamic
    tables, and an inert no-op for interactive tables, which never emit it.

    A leading, case-insensitive `LINEAR` is stripped ONLY when the remainder
    (after skipping whitespace) is itself a balanced parenthesized group
    closing at the end of the string -- reusing `has_balanced_outer_parens`
    on that remainder. This leaves a column or expression literally named
    `linear` alone: bare `linear` has no following paren group to satisfy that
    check, and `linear(a), b` is a multi-key list whose leading group closes
    before the final character, not at it. The single-key case `LINEAR(ts)`
    is genuinely ambiguous -- it could be Snowflake's wrapper around key `ts`,
    or a call to a function named `linear` -- and is deliberately treated as
    the wrapper, consistent with how a bare `(ts)` is already unwrapped.
    """
    if value is None:
        return None
    text = value.strip()
    if text.casefold() in ABSENT:
        return None
    if text.casefold().startswith("linear"):
        remainder = text[len("linear") :].lstrip()
        if has_balanced_outer_parens(remainder):
            text = remainder
    if has_balanced_outer_parens(text):
        text = text[1:-1].strip()
    parts = [part.strip() for part in text.split(",")]
    return ", ".join(part for part in parts if part).casefold()


def normalize_target_lag(value: Optional[str]) -> Optional[str]:
    """Snowflake canonicalizes lag units on readback (`60 seconds` -> `1 minute`).
    Convert both sides to a comparable count of seconds where possible; fall back
    to a casefolded string so unrecognized forms still compare sanely. The
    fallback also collapses internal whitespace runs to a single space, so
    e.g. `2 weeks` and `2  weeks` still compare equal.

    `DOWNSTREAM` is a legal value and is not a duration.
    """
    if value is None:
        return None
    text = value.strip().casefold()
    if text in ABSENT:
        return None
    if text == "downstream":
        return "downstream"

    units = {
        "second": 1,
        "seconds": 1,
        "minute": 60,
        "minutes": 60,
        "hour": 3600,
        "hours": 3600,
        "day": 86400,
        "days": 86400,
    }
    parts = text.split()
    if len(parts) == 2 and parts[1] in units:
        try:
            return str(int(parts[0]) * units[parts[1]])
        except ValueError:
            return " ".join(parts)
    return " ".join(parts)
