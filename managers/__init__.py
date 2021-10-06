import math
from datetime import datetime
from decimal import Decimal


def to_date(timestamp):
    if timestamp is None:
        return None

    return datetime.fromtimestamp(timestamp).isoformat()


def get_total_seconds(started):
    now = datetime.now()
    delta = now - started
    total_seconds = delta.total_seconds()

    return total_seconds


def remove_exponent(d):
    """Remove exponent."""
    return d.quantize(Decimal(1)) if d == d.to_integral() else d.normalize()


def millify(n, precision=0, drop_nulls=True, prefixes=None):
    """Humanize number."""
    if prefixes is None:
        prefixes = []

    millnames = ['', 'k', 'm', 'b']

    if prefixes:
        millnames = ['']
        millnames.extend(prefixes)
    n = float(n)
    millidx = max(0, min(len(millnames) - 1,
                         int(math.floor(0 if n == 0 else math.log10(abs(n)) / 3))))
    result = '{:.{precision}f}'.format(n / 10**(3 * millidx), precision=precision)
    if drop_nulls:
        result = remove_exponent(Decimal(result))
    return '{0}{dx}'.format(result, dx=millnames[millidx])
