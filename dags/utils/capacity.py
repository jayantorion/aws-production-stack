"""Dynamic capacity planning — graceful handling of data growth.

Handles: "one day the source sends 80 GB instead of 40 GB" and steady growth.
Strategy:
  1. Glue workers/DPU are sized FROM THE ACTUAL batch bytes (manifests), not a
     fixed number — 40 GB and 80 GB runs both finish inside the same SLA.
  2. Glue auto-scaling bounds the spike between tiers.
  3. Redshift: incremental COPY keeps warehouse load proportional to *new* data,
     not total data; RA3 resized per growth (see LINEAGE §6).
  4. S3: partitioned layout keeps listing/scans O(new data).
"""
from __future__ import annotations

from typing import Any, Dict, List

# (max_input_GB, worker_type, workers, estimated_minutes)
TIERS = [
    (1,   "G.1X", 4,   10),
    (10,  "G.2X", 10,  15),
    (30,  "G.2X", 20,  25),
    (60,  "G.2X", 40,  35),
    (120, "G.2X", 80,  55),   # 80 GB day lands here — same SLA as a 40 GB day
]
FALLBACK = ("G.2X", 120, 75)   # beyond all tiers: max out + alert


def plan_glue_capacity(total_bytes: float) -> Dict[str, Any]:
    """Map total batch input size to a Glue worker plan."""
    gb = total_bytes / (1024 ** 3)
    for max_gb, worker_type, workers, mins in TIERS:
        if gb <= max_gb:
            return {"input_gb": round(gb, 2), "worker_type": worker_type,
                    "number_of_workers": workers, "estimated_minutes": mins}
    return {"input_gb": round(gb, 2), "worker_type": FALLBACK[0],
            "number_of_workers": FALLBACK[1], "estimated_minutes": FALLBACK[2]}


def plan_from_manifests(manifests: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Sum the actual landed bytes across batch manifests and size the job."""
    total = 0
    for m in manifests:
        for f in m.get("files", []):
            total += f.get("bytes", 0)
        total += m.get("bytes_extra", 0)
    return plan_glue_capacity(total)
