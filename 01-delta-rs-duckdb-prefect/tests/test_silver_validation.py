import pandas as pd

from lakehouse.silver import validate


def _row(**kw):
    base = dict(order_id="O1", customer_id="C1", country="KE", amount="10.5", status="paid",
                order_ts="2024-01-01 10:00:00", updated_at="2024-01-01 10:00:00",
                _source_file="f.csv", _batch_id=1)
    base.update(kw)
    return base


def test_valid_row_passes_and_is_typed():
    good, bad = validate(pd.DataFrame([_row()]))
    assert len(good) == 1 and bad.empty
    assert good.iloc[0]["amount"] == 10.5
    assert str(good.iloc[0]["order_date"]) == "2024-01-01"


def test_rejections_have_reasons():
    df = pd.DataFrame([
        _row(order_id="A", amount=None),
        _row(order_id="B", amount="-5"),
        _row(order_id="C", status="unknown"),
        _row(order_id=None),
        _row(order_id="E", order_ts="not-a-date"),
    ])
    good, bad = validate(df)
    assert good.empty
    assert sorted(bad["_reject_reason"]) == sorted(
        ["invalid_amount", "negative_amount", "invalid_status", "missing_order_id", "invalid_timestamp"]
    )
