import sys
import contextlib


# credit to https://stackoverflow.com/a/17603000
@contextlib.contextmanager
def smart_open(filename=None):
    """
    Open a file handle to a file or stdout, as appropriate.

    :param filename: the name of the file. Omitting this, passing ``None``, or passing ``"-"`` will return the handle to
        sys.stdout.
    :type filename: None or str

    :return: a file handle object.

    This may be used in a ``with smart_open(...) as`` block.
    """
    if filename and filename != '-':
        fh = open(filename, 'w')
    else:
        fh = sys.stdout

    try:
        yield fh
    finally:
        if fh is not sys.stdout:
            fh.close()