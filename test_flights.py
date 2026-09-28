import pandas as pd

df = pd.read_csv("source_flights.csv")


def test_row_count():
    assert len(df) == 285369