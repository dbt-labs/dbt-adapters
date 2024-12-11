from contextlib import contextmanager
from os import chdir
from os.path import normcase, normpath
from pathlib import Path
from typing import List, Optional

from dbt.tests.util import (
    run_dbt as _run_dbt,
    run_dbt_and_capture as _run_dbt_and_capture,
)


@contextmanager
def up_one(return_path: Optional[Path] = None):
    current_path = Path.cwd()
    chdir("../")
    try:
        yield
    finally:
        chdir(return_path or current_path)


def normalize(path):
    """On windows, neither is enough on its own:

    >>> normcase('C:\\documents/ALL CAPS/subdir\\..')
    'c:\\documents\\all caps\\subdir\\..'
    >>> normpath('C:\\documents/ALL CAPS/subdir\\..')
    'C:\\documents\\ALL CAPS'
    >>> normpath(normcase('C:\\documents/ALL CAPS/subdir\\..'))
    'c:\\documents\\all caps'
    """
    return normcase(normpath(path))


def run_dbt(args: Optional[List[str]] = None, expect_pass: bool = True):
    _set_flags()
    return _run_dbt(args, expect_pass)


def run_dbt_and_capture(args: Optional[List[str]] = None, expect_pass: bool = True):
    _set_flags()
    return _run_dbt_and_capture(args, expect_pass)


def _set_flags():
    # in order to call dbt's internal profile rendering, we need to set the
    # flags global. This is a bit of a hack, but it's the best way to do it.
    from dbt.flags import set_from_args
    from argparse import Namespace

    set_from_args(Namespace(), None)
