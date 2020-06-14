"""os_random

Duplicates methods of the random module, but using the more
cryptographically secure os.urandom (implemented by the operating system),
rather than a pseudo-random number generator.

Google App Engine has thoughtfully implemented this, drawing on each instance's
/dev/urandom: https://code.google.com/p/googleappengine/issues/detail?id=1055
"""

from __future__ import division
from math import floor
import os


def random():
    """Return float in the range [0.0, 1.0)."""
    # os.urandom returns a byte string. Each byte becomes two hexidecimal
    # digits, so five bytes becomes a 10-digit base 16 number, so the maximum
    # value is 16^10
    return int(os.urandom(5).encode('hex'), 16) / 16 ** 10


def choice(seq):
    """Return a random element from the non-empty sequence seq.

    Raises: IndexError, if seq is empty.
    """
    if len(seq) == 0:
        raise IndexError

    return seq[int(floor(len(seq) * random()))]
