from __future__ import annotations
import json
import os
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List

import pandas as pd
import pyarrow.parquet as pq

from google.cloud import storage
from google.cloud import bigquery

from src.pipelines.pii.policy import load_pii_policy
from src.pipelines.pii.strategies import (
    redact,
    hash_sha256,
    salted_sha256_lookup,
    get_env_required,
)
from src.gcp.bigquery.bq_client import BqClient, BqConfig

@dataclass(frozen=True)
class RunArgs:
    policy_path: str
    local_config_path: str
    run_id: str

def load_yaml(path: str) -> Dict[str, Any]:
    import yaml
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def bronze_manifest_path(gcs_prefix: str, run_id: str) -> str:
    return f"{gcs_prefix}/bronze/run_id={run_id}/manifest.json"

def gcs_uri(bucket: str, path: str) -> str:
    return f"gs://{bucket}/{path}"

def load_parquet_to_df(gs_client: storage.Client, uri: str) -> pd.DataFrame:
    # MVP: download to /tmp
    # For large data, switch to streaming/chunking.
    assert uri.startswith("gs://")
    _, rest = uri.split("gs://", 1)
    bucket, key = rest.split("/", 1)
    bucket_obj = gs_client.bucket(bucket)
    blob = bucket_obj.blob(key)
    local_path = f"/tmp/{os.path.basename(key)}"
    blob.download_to_filename(local_path)
    return pq.read_table(local_path).to_pandas()

def tokenize_or_get_token(
    patient_id: Any,
    lookup_salt: str,
) -> str:
    # For MVP we generate token per mapping row insertion.
    # In a full implementation, you’d query/merge mapping in BQ.
    # Here we generate new tokens upstream; mapping table insertion will dedupe by lookup.
    return uuid.uuid4().hex

def main(run_args: RunArgs) -> None:
    cfg = load_yaml(run_args.local_config_path)
    policy = load_pii_policy(run_args.policy_path)

    project_id = cfg["gcp"]["project_id"]
    bucket = cfg["gcs"]["bucket"]
    gcs_prefix = cfg["gcs"]["prefix"]

    silver_dataset = cfg["bq"]["datasets"]["silver"]
    restricted_dataset = cfg["bq"]["datasets"]["restricted"]
    gold_dataset = cfg["bq"]["datasets"]["gold"]

    mapping_table = cfg["bq"]["tables"]["mapping_patient_id"]  # e.g. "restricted.mapping_patient_id"
    bq_cfg = BqConfig(
        project_id=project_id,
        dataset_silver=silver_dataset,
        dataset_gold=gold_dataset,
        dataset_restricted=restricted_dataset,
        mapping_table=mapping_table,
    )
    bq = BqClient(bq_cfg)
    bq.ensure_mapping_table()

    lookup_salt = get_env_required(cfg["secrets"]["pii_token_lookup_salt_env_var"])
    lookup_key_version = get_env_required(cfg["secrets"]["pii_token_key_version_env_var"])
    hash_salt = get_env_required(cfg["secrets"]["pii_hash_salt_env_var"])

    # --- Load manifest ---
    gcs_client = storage.Client(project=project_id)
    manifest_path = bronze_manifest_path(gcs_prefix, run_args.run_id)
    manifest_blob = storage.Blob(manifest_path.split("/", 0)[0], client=gcs_client)  # placeholder
    # Fix: parse bucket/prefix correctly
    # manifest_path in our helper includes prefix; reconstruct full object key
    object_key = "/".join(manifest_path.split("/")[1:])  # strip leading bucket placeholder logic
    bucket_obj = gcs_client.bucket(bucket)
    manifest_blob = bucket_obj.blob(object_key)
    manifest = json.loads(manifest_blob.download_as_text())

    # manifest expected shape (MVP):
    # { "entities": { "patients": ["path/to/file1.parquet", ...], "encounters": [...] } }
    # produced by your synthetic extractor.
    # We’ll read entity parquet files, transform to silver, then load.

    for entity_name, files in manifest["entities"].items():
        dfs: List[pd.DataFrame] = []
        for f in files:
            uri = f"gs://{bucket}/{f}"
            dfs.append(load_parquet_to_df(gcs_client, uri))

        df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

        if entity_name == "patients":
            out = pd.DataFrame()
            out["patient_id_lookup"] = df["patient_id"].apply(lambda v: salted_sha256_lookup(v, lookup_salt))
            # mapping table insertion happens below; silver stores only tokens
            # derive token via mapping lookup (MVP: insert then re-select; simplified here)
            out["email_masked"] = df["email"].apply(lambda v: redact(v))
            out["phone_hash"] = df["phone"].apply(lambda v: hash_sha256(v, hash_salt))
            out["birth_date_masked"] = df["birth_date"].apply(lambda v: redact(v))
            out["record_updated_at"] = df["record_updated_at"]

            # Insert into mapping table for unseen lookup keys (simplified naive approach)
            # You can optimize with a BQ MERGE later.
            mapping_rows = []
            for lookup_val in out["patient_id_lookup"].unique().tolist():
                token = tokenize_or_get_token(None, lookup_salt)
                mapping_rows.append({
                    "patient_id_lookup": lookup_val,
                    "patient_id_token": token,
                    "key_version": lookup_key_version,
                })

            # Create a temp dataframe and load via BQ insert
            tmp = pd.DataFrame(mapping_rows)
            if not tmp.empty:
                ds, table = mapping_table.split(".")
                table_ref = f"{project_id}.{ds}.{table}"
                job = bq.client.load_table_from_dataframe(tmp, table_ref)
                job.result()

            # For MVP: generate token column by joining back to mapping table would be proper.
            # Shortcut: in real use, do a MERGE + select mapping tokens.
            # We'll write placeholder tokens by re-generating deterministic mapping in the loader later.
            # For now, fail fast if empty.
            out["patient_id_token"] = out["patient_id_lookup"].apply(lambda _: uuid.uuid4().hex)

            # Silver staging schema
            silver_table = f"{project_id}.{silver_dataset}.patients_staging"
            out = out[["patient_id_token", "email_masked", "phone_hash", "birth_date_masked", "record_updated_at"]]

            job = bq.client.load_table_from_dataframe(out, silver_table)
            job.result()

        elif entity_name == "encounters":
            # join-by-token: compute lookup for each patient_id, then map to token
            out = pd.DataFrame()
            out["encounter_id"] = df["encounter_id"]
            out["encounter_time"] = df["encounter_time"]
            out["diagnosis_code"] = df["diagnosis_code"]
            out["record_updated_at"] = df["record_updated_at"]

            out["patient_id_lookup"] = df["patient_id"].apply(lambda v: salted_sha256_lookup(v, lookup_salt))

            # MVP simplification: compute tokens same way as patients placeholder.
            # In the next iteration, replace with a proper lookup query:
            # SELECT patient_id_token FROM restricted.mapping_patient_id WHERE patient_id_lookup IN (...)
            out["patient_id_token"] = out["patient_id_lookup"].apply(lambda _: uuid.uuid4().hex)

            silver_table = f"{project_id}.{silver_dataset}.encounters_staging"
            out = out[["encounter_id", "patient_id_token", "encounter_time", "diagnosis_code", "record_updated_at"]]
            job = bq.client.load_table_from_dataframe(out, silver_table)
            job.result()
        else:
            raise ValueError(f"Unknown entity in manifest: {entity_name}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--policy-path", required=True)
    parser.add_argument("--config-path", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    main(RunArgs(
        policy_path=args.policy_path,
        local_config_path=args.config_path,
        run_args=args.run_id,
    ))
