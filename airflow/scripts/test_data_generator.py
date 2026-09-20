"""Unit tests for data_generator.py — no DB, no Airflow, just the generator."""
from data_generator import generate_rows


def test_total_row_count():
    rows = [row for batch in generate_rows(2_500, 1_000, load_batch_id="test") for row in batch]
    assert len(rows) == 2_500


def test_batches_respect_batch_size():
    batches = list(generate_rows(2_500, 1_000, load_batch_id="test"))
    assert [len(b) for b in batches] == [1_000, 1_000, 500]


def test_same_seed_is_deterministic():
    first = list(generate_rows(100, 50, load_batch_id="test", seed=42))
    second = list(generate_rows(100, 50, load_batch_id="test", seed=42))
    assert first == second


def test_rows_carry_the_load_batch_id():
    (batch,) = generate_rows(10, 10, load_batch_id="batch-xyz")
    assert all(row[-1] == "batch-xyz" for row in batch)
