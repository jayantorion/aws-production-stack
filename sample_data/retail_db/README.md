# Sample Data — retail_db

Classic **retail_db** sample dataset (from the "Learning Spark"/Databricks education
track), used as the development source-of-record for this pipeline.

| Entity | File | Rows | Grain |
|---|---|---|---|
| departments | `departments/part-00000` | 6 | one row per department |
| categories | `categories/part-00000` | 58 | one row per category |
| customers | `customers/part-00000` | 12,435 | one row per customer |
| products | `products/part-00000` | 1,345 | one row per product |
| orders | `orders/part-00000` | 68,883 | one row per order (incremental on `order_date`) |
| order_items | `order_items/part-00000` | 172,198 | one row per order line |

**Format:** delimiter-separated text, **no header**, UTF-8. Column order per entity is
defined in [`config/entities.yaml`](../../config/entities.yaml).

The pipeline's default source path (`config/settings.yaml → sources.local_base_path`)
points here (`sample_data/retail_db`). In production, point the same entity configs at
MySQL / SFTP / REST API sources — no code change required.

Full-size simulation: to test at 40–80 GB/day scale, generate multiplied copies of these
files — the capacity planner (`dags/utils/capacity.py`) sizes Glue workers automatically.
