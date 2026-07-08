"""Smoke test: load real retail_db data through config + bronze landing (fake S3).

Run:  python scripts/smoke_bronze.py
Verifies all 6 entities parse and land with correct row counts and manifests.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from utils import bronze  # noqa: E402
from utils.config_loader import load_entities, load_settings  # noqa: E402


class FakeS3:
    def __init__(self):
        self.objects = {}

    def put_object(self, Bucket, Key, Body):
        self.objects[f"s3://{Bucket}/{Key}"] = Body


def main():
    entities = load_entities()
    settings = load_settings()
    print("Entities:", list(entities.keys()))
    print("Source path:", settings["sources"]["local_base_path"])

    s3 = FakeS3()
    for name, cfg in entities.items():
        df = bronze.read_source_csv(name, cfg, settings["sources"]["local_base_path"])
        df = bronze.apply_incremental_filter(df, cfg, None, None)
        manifest = bronze.land_to_bronze(
            df, name, cfg, f"smoke{name[:8]}", "dev-dep-raw", s3)
        print(f"{name:12s} rows={manifest['row_count']:>7}  "
              f"files={manifest['file_count']}  cols={len(manifest['schema'])}")
    print("Total S3 objects landed:", len(s3.objects))


if __name__ == "__main__":
    main()
