"""Single place that defines where every layer lives (overridable for tests)."""
from dataclasses import dataclass
from pathlib import Path
import json


@dataclass(frozen=True)
class Paths:
    root: Path = Path(__file__).resolve().parents[1] / "data"

    @property
    def source(self) -> Path:
        return self.root / "source"

    @property
    def bronze(self) -> str:
        return str(self.root / "lake" / "bronze" / "orders")

    @property
    def silver(self) -> str:
        return str(self.root / "lake" / "silver" / "orders")

    @property
    def quarantine(self) -> str:
        return str(self.root / "lake" / "silver" / "orders_quarantine")

    @property
    def gold(self) -> str:
        return str(self.root / "lake" / "gold" / "daily_revenue")

    @property
    def state_file(self) -> Path:
        return self.root / "state" / "watermarks.json"


DEFAULT = Paths()


def get_watermark(paths: Paths, key: str, default=0):
    if not paths.state_file.exists():
        return default
    return json.loads(paths.state_file.read_text()).get(key, default)


def set_watermark(paths: Paths, key: str, value) -> None:
    paths.state_file.parent.mkdir(parents=True, exist_ok=True)
    state = json.loads(paths.state_file.read_text()) if paths.state_file.exists() else {}
    state[key] = value
    paths.state_file.write_text(json.dumps(state, indent=2))
