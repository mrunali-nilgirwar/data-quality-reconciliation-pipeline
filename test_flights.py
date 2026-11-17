"""Data quality tests for the June 2015 flight data (source_flights.csv).

Run with:  pytest -v test_flights.py
Each test encodes one finding from profiling, so any future change to the
data that breaks an assumption fails loudly instead of slipping through.
"""
import pandas as pd
import pytest

EXPECTED_ROWS = 285_368          # data rows only (CSV has 285,369 lines incl. header)
EXPECTED_AIRLINES = {"AA", "DL", "F9", "UA", "WN"}
DELAY_REASON_COLS = [
    "AIR_SYSTEM_DELAY", "SECURITY_DELAY", "AIRLINE_DELAY",
    "LATE_AIRCRAFT_DELAY", "WEATHER_DELAY",
]
EXPECTED_DELAY_NULLS = 217_877
# A flight number can fly several legs a day (e.g. AA 23 LAX->LAS and LAS->LAX),
# so origin and destination must be part of the uniqueness key.
FLIGHT_KEY = [
    "YEAR", "MONTH", "DAY", "AIRLINE", "FLIGHT_NUMBER",
    "ORIGIN_AIRPORT", "DESTINATION_AIRPORT",
]


@pytest.fixture(scope="module")
def df():
    # Loaded once and shared by every test in this file.
    return pd.read_csv("source_flights.csv", low_memory=False)


def test_row_count(df):
    assert len(df) == EXPECTED_ROWS


def test_filter_is_june_2015_and_five_carriers(df):
    assert set(df["YEAR"].unique()) == {2015}
    assert set(df["MONTH"].unique()) == {6}
    assert set(df["AIRLINE"].unique()) == EXPECTED_AIRLINES


def test_airport_codes_are_3_letter_iata(df):
    # Schema drift check: October 2015 used numeric IDs like 12892.
    for col in ["ORIGIN_AIRPORT", "DESTINATION_AIRPORT"]:
        bad = df[~df[col].astype(str).str.fullmatch(r"[A-Z]{3}")]
        assert bad.empty, f"{len(bad)} non-IATA codes in {col}, e.g. {bad[col].head(3).tolist()}"


def test_no_duplicate_flights(df):
    dupes = df[df.duplicated(subset=FLIGHT_KEY, keep=False)]
    assert dupes.empty, f"{len(dupes)} rows share a flight key"


def test_delay_reason_nulls_are_expected(df):
    # These nulls are business logic (no delay reason needed), not missing data.
    for col in DELAY_REASON_COLS:
        assert df[col].isna().sum() == EXPECTED_DELAY_NULLS, col