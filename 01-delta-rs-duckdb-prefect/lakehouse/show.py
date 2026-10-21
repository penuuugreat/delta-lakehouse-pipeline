"""Print a summary of each layer, including Delta version history."""
from .delta_utils import open_table
from .paths import DEFAULT

for name, path in [("bronze", DEFAULT.bronze), ("silver", DEFAULT.silver),
                   ("quarantine", DEFAULT.quarantine), ("gold", DEFAULT.gold)]:
    t = open_table(path)
    if t is None:
        print(f"{name}: (missing)")
        continue
    df = t.to_pandas()
    ops = [h["operation"] for h in t.history()][::-1]
    print(f"{name:11s} rows={len(df):5d}  delta_version={t.version()}  history={ops}")
t = open_table(DEFAULT.quarantine)
if t is not None:
    print("\nQuarantine reasons:\n", t.to_pandas()["_reject_reason"].value_counts().to_string())
g = open_table(DEFAULT.gold)
if g is not None:
    print("\nGold sample:\n", g.to_pandas().sort_values(["order_date", "country"]).head(8).to_string(index=False))
