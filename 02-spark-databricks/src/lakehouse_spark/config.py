"""Runtime configuration. Two modes share ALL transformation code:

  databricks : Unity Catalog tables (catalog.schema.table), Auto Loader, UC Volumes
  local      : Delta tables addressed by path, batch file ingest (Auto Loader is Databricks-only)
"""
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Config:
    mode: str = "databricks"
    catalog: str = "main"
    schema: str = "lakehouse"
    landing_path: str | None = None
    checkpoint_root: str | None = None
    local_root: str = "data"

    @property
    def is_local(self) -> bool:
        return self.mode == "local"

    @property
    def landing(self) -> str:
        return self.landing_path or f"/Volumes/{self.catalog}/{self.schema}/landing/orders"

    @property
    def checkpoints(self) -> str:
        return self.checkpoint_root or f"/Volumes/{self.catalog}/{self.schema}/checkpoints"

    @property
    def local_lake(self) -> Path:
        return Path(self.local_root).resolve() / "lake"
