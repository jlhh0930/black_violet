from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, Optional
from google.cloud import bigquery

@dataclass(frozen=True)
class BqConfig:
    project_id: str
    dataset_silver: str
    dataset_gold: str
    dataset_restricted: str
    mapping_table: str  # e.g. "restricted.mapping_patient_id"

class BqClient:
    def __init__(self, cfg: BqConfig):
        self.client = bigquery.Client(project=cfg.project_id)
        self.cfg = cfg

    def ensure_mapping_table(self) -> None:
        # mapping_table format: "<dataset>.<table>"
        ds, table = self.cfg.mapping_table.split(".")
        dataset_ref = f"{self.cfg.project_id}.{ds}"
        table_ref = f"{dataset_ref}.{table}"

        schema = [
            bigquery.SchemaField("patient_id_lookup", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("patient_id_token", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("key_version", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
        ]

        table_obj = bigquery.Table(table_ref, schema=schema)

        try:
            self.client.get_table(table_ref)
        except Exception:
            self.client.create_table(table_obj)

    def execute(self, sql: str) -> None:
        self.client.query(sql).result()
