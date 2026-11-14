"""Exact, epoch-scoped L2 state shared by public venue adapters.

Receive order is local provenance, never a fabricated exchange sequence.
Snapshots without a comparable source version have an uncertain delta boundary.
"""
from decimal import Decimal, localcontext
import hashlib
import re

from .core import dumps


def decimal_text(value):
    if isinstance(value, (bool, float)) or not isinstance(value, (str, int, Decimal)):
        raise ValueError('exact_decimal_required')
    number = Decimal(value)
    if not number.is_finite():
        raise ValueError('finite_decimal_required')
    return format(number, 'f')


def scaled_integer(value, decimals):
    if type(decimals) is not int or not 0 <= decimals <= 36:
        raise ValueError('invalid_native_scale')
    if isinstance(value, bool) or not isinstance(value, (str, int)) or not re.fullmatch('[0-9]{1,100}', str(value)):
        raise ValueError('unsigned_native_integer_required')
    with localcontext() as context:
        context.prec = 140
        return format(Decimal(value).scaleb(-decimals), 'f')


def level(price, size):
    price, size = decimal_text(price), decimal_text(size)
    if not 0 <= Decimal(price) <= 1 or Decimal(size) < 0:
        raise ValueError('invalid_probability_level')
    return {'price': price, 'size': size}


def level_map(rows):
    result = {}
    for row in rows:
        item = level(row['price'], row['size'])
        price, size = Decimal(item['price']), Decimal(item['size'])
        if price in result:
            raise ValueError('duplicate_snapshot_price')
        result[price] = size
    return {p: q for p, q in result.items() if q}


class ExactL2:
    def __init__(self):
        self.books = {}
        self.revisions = {}

    def invalidate(self, instruments=None):
        for key in list(self.books) if instruments is None else instruments:
            self.books.pop(key, None)
            self.revisions[key] = self.revisions.get(key, 0) + 1

    def snapshot(self, instrument, epoch, bids, asks):
        book = {'epoch': epoch, 'bids': level_map(bids), 'asks': level_map(asks)}
        self.books[instrument] = book
        self.revisions[instrument] = self.revisions.get(instrument, 0) + 1

    def delta(self, instrument, epoch, bids, asks):
        # Validate the whole update before changing either side.
        changes = {side: [level(x['price'], x['size']) for x in values]
                   for side, values in (('bids', bids), ('asks', asks))}
        self.revisions[instrument] = self.revisions.get(instrument, 0) + 1
        book = self.books.get(instrument)
        if book is None or book['epoch'] != epoch:
            return False
        for side, values in changes.items():
            for item in values:
                price, size = Decimal(item['price']), Decimal(item['size'])
                if size:
                    book[side][price] = size
                else:
                    book[side].pop(price, None)
        return True

    def levels(self, instrument):
        book = self.books[instrument]
        return {side: [{'price': format(p, 'f'), 'size': format(q, 'f')}
                       for p, q in sorted(book[side].items(), reverse=side == 'bids')]
                for side in ('bids', 'asks')}

    def matches(self, instrument, bids, asks):
        book = self.books[instrument]
        return book['bids'] == level_map(bids) and book['asks'] == level_map(asks)

    def digest(self, instrument):
        # Canonicalize Decimal spellings only for local comparison hashes.
        book = self.books[instrument]
        def canonical(number):
            text = format(number, 'f')
            return text.rstrip('0').rstrip('.') if '.' in text else text
        values = [[(canonical(p), canonical(q))
                   for p, q in sorted(book[side].items())] for side in ('bids', 'asks')]
        return hashlib.sha256(dumps(values).encode()).hexdigest()
