"""Streaming synthetic-data generator for the 10M-row bulk load.

No Faker on purpose: at 10M+ rows, Faker's per-field provider overhead turns
data generation itself into the bottleneck. Sampling from small fixed pools
is 1-2 orders of magnitude faster and keeps wall-clock time dominated by the
actual database write, which is what this exercise measures.
"""
from __future__ import annotations

import datetime as dt
import random
from collections.abc import Iterator

REGIONS = ["N", "NE", "CO", "SE", "S"]
CHANNELS = ["WEB", "MOBILE_APP", "MARKETPLACE", "PHONE"]
STATUSES = ["DELIVERED", "CANCELLED", "CLOSED"]  # historical orders are always terminal

_START_DATE = dt.date(2019, 1, 1)
_DATE_RANGE_DAYS = (dt.date(2025, 12, 31) - _START_DATE).days


def _random_date(rng: random.Random) -> dt.date:
    return _START_DATE + dt.timedelta(days=rng.randint(0, _DATE_RANGE_DAYS))


def generate_rows(
    total_rows: int, batch_size: int, load_batch_id: str, seed: int | None = None
) -> Iterator[list[tuple]]:
    """Yields lists of row-tuples of length <= batch_size. Memory usage is
    O(batch_size), never O(total_rows): nothing from a previous batch is
    retained, and the caller is expected to write/discard each batch before
    the next one is produced."""
    rng = random.Random(seed)
    produced = 0
    while produced < total_rows:
        n = min(batch_size, total_rows - produced)
        batch = []
        for i in range(n):
            seq = produced + i
            batch.append(
                (
                    f"HIST-{seq:010d}",
                    f"CLIENT-{rng.randint(1, 2_000_000):09d}",
                    _random_date(rng),
                    rng.choice(STATUSES),
                    round(rng.uniform(15.0, 4500.0), 2),
                    rng.choice(REGIONS),
                    rng.choice(CHANNELS),
                    rng.randint(1, 12),
                    load_batch_id,
                )
            )
        produced += n
        yield batch
