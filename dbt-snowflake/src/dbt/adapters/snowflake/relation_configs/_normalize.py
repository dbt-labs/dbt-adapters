from typing import List, Optional

# Readback spellings that mean "this is not set".
ABSENT = {"", "none"}


def absent_to_none(value: Optional[str]) -> Optional[str]:
    """Snowflake reads back '' for an unset initialization warehouse; collapse
    that to None so absence detection works. No casefolding -- that's a
    comparison concern (normalize_warehouse)."""
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
    """Not the same as startswith('(') and endswith(')') -- those parens can
    belong to unrelated groups, e.g. `(a), to_date(ts)`. Parens inside a quoted
    identifier are not structural and don't count."""
    if not (text.startswith("(") and text.endswith(")")):
        return False
    depth = 0
    in_quotes = False
    index = 0
    while index < len(text):
        char = text[index]
        if char == '"':
            if in_quotes and text[index + 1 : index + 2] == '"':
                index += 1  # doubled `""` escape for a literal quote
            else:
                in_quotes = not in_quotes
        elif not in_quotes:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0:
                    return index == len(text) - 1
        index += 1
    return False


def split_keys(text: str) -> List[str]:
    """Split on commas outside both parentheses and quoted identifiers, so a
    clustering expression like `coalesce(a, b)` stays one key and a quoted
    identifier like `"a,b"` isn't split on its embedded comma."""
    keys = []
    depth = 0
    in_quotes = False
    start = 0
    index = 0
    while index < len(text):
        char = text[index]
        if char == '"':
            if in_quotes and text[index + 1 : index + 2] == '"':
                index += 1  # doubled `""` escape for a literal quote
            else:
                in_quotes = not in_quotes
        elif not in_quotes:
            if char == "(":
                depth += 1
            elif char == ")":
                depth = max(depth - 1, 0)
            elif char == "," and depth == 0:
                keys.append(text[start:index])
                start = index + 1
        index += 1
    keys.append(text[start:])
    return keys


def normalize_key(key: str) -> str:
    """A quoted identifier is case-SENSITIVE in Snowflake and kept exactly as
    written; anything unquoted is folded, matching how Snowflake folds it."""
    if len(key) >= 2 and key.startswith('"') and key.endswith('"'):
        return key
    return key.casefold()


def normalize_cluster_by(value: Optional[str]) -> Optional[str]:
    """`SHOW` parenthesizes clustering keys -- `(id, name)` -- while the model
    config yields a bare `id, name`; `SHOW DYNAMIC TABLES` additionally prefixes
    `LINEAR`. Strip one balanced outer pair (and a `LINEAR` wrapper) so the two
    compare equal. Not a strip-to-first-paren: that corrupts `to_date(ts)`.
    `LINEAR` is only stripped when followed by a group closing at end-of-string,
    so a key literally named `linear` survives.
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
    parts = [part.strip() for part in split_keys(text)]
    return ", ".join(normalize_key(part) for part in parts if part)


def normalize_target_lag(value: Optional[str]) -> Optional[str]:
    """Snowflake canonicalizes lag units on readback (`60 seconds` -> `1 minute`).
    Convert both sides to a comparable count of seconds where possible; fall back
    to a casefolded string so unrecognized forms still compare sanely.

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
