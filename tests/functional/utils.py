from contextlib import contextmanager
from os import chdir
from os.path import normcase, normpath
from pathlib import Path
from typing import Optional


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
