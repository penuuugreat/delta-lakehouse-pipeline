"""GOLD: analytics-ready daily revenue by country (business-level aggregate).

Incremental: only the order_dates touched by the latest silver batch are recomputed,
then upserted by (order_date, country).
"""
from datetime import date

import duckdb
from deltalake import write_deltalake

from .delta_utils import open_table
from .paths import DEFAULT, Paths


def build(paths: Paths = DEFAULT, affected_dates: list[str] | None = None) -> dict:
    silver = open_table(paths.silver)
    if silver is None:
        return {"rows_upserted": 0}
    if affected_dates is not None and not affected_dates:
        return {"rows_upserted": 0}

    filters = [("order_date", "in", [date.fromisoformat(d) for d in affected_dates])] if affected_dates else None
    tbl = silver.to_pyarrow_table(columns=["order_date", "country", "amount", "status"], filters=filters)

    con = duckdb.connect()
    con.register("silver", tbl)
    result = con.execute(
        """
        SELECT order_date,
               country,
               COUNT(*)                                                        AS orders,
               COUNT(*) FILTER (WHERE status <> 'cancelled')                   AS net_orders,
               ROUND(COALESCE(SUM(amount) FILTER (WHERE status <> 'cancelled'), 0), 2) AS net_revenue,
               ROUND(AVG(amount), 2)                                           AS avg_order_value
        FROM silver
        GROUP BY order_date, country
        """
    ).to_arrow_table()

    gt = open_table(paths.gold)
    if gt is None:
        write_deltalake(paths.gold, result)
    else:
        (
            gt.merge(
                source=result,
                predicate="t.order_date = s.order_date AND t.country = s.country",
                source_alias="s",
                target_alias="t",
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
    return {"rows_upserted": result.num_rows}
