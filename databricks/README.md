# Running on Databricks (illustrative)

> **Illustrative only.** The files in this folder show how the local modules map onto
> Databricks. They have not been deployed from this repository; workspace host,
> catalog/schema names, cluster sizes and paths are placeholders.

## Module mapping

| Local module (`src/lakehouse/`) | Databricks Workflows (Jobs) task | Delta Live Tables equivalent |
|---|---|---|
| `generate.py` | not used: real CDC lands from Debezium/Kafka or a vendor feed | n/a |
| `bronze.py` `ingest_entity` | `bronze_ingest` (Python wheel task) | `@dlt.table` over **Auto Loader** (`cloudFiles`, `schemaHints`, `rescuedDataColumn` in place of the quarantine table) |
| `silver.py` `customer_changes` / `transactions` | `silver_build` | `dlt.create_streaming_table` + `@dlt.append_flow` |
| `silver.py` `build_scd2` (`dim_customer`) | `silver_build` (with `--format delta`; the "replace affected keys" step becomes `MERGE INTO`) | `dlt.apply_changes(..., keys=["customer_id"], sequence_by="event_ts", apply_as_deletes="op = 'D'", stored_as_scd_type=2)` |
| `silver.py` `build_current_state` (`accounts`) | `silver_build` | `dlt.apply_changes(..., stored_as_scd_type=1)` |
| `gold.py` | `gold_build` | `@dlt.table` (materialized view) |
| `quality.py` expectations | `dq_gate` task; a failing `error` check fails the job run | `@dlt.expect` (warn), `@dlt.expect_or_drop`, `@dlt.expect_or_fail` (error) |
| `_checkpoints/` ingestion log | Auto Loader checkpoint / RocksDB file state | managed by DLT |
| `conf/pipeline.toml` | job parameters + bundle `variables` | pipeline `configuration` |

## Expectations ported to DLT (sketch)

```python
import dlt
from pyspark.sql import functions as F

@dlt.table(name="silver_transactions")
@dlt.expect_or_fail("txn_id_present", "txn_id IS NOT NULL")
@dlt.expect_or_drop("positive_amount", "amount > 0")
@dlt.expect("known_currency", "currency = 'USD'")
def silver_transactions():
    return dlt.read_stream("bronze_transactions").dropDuplicatesWithinWatermark(["txn_id"])

dlt.create_streaming_table("dim_customer")
dlt.apply_changes(
    target="dim_customer",
    source="bronze_customers_clean",
    keys=["customer_id"],
    sequence_by=F.col("event_ts"),
    apply_as_deletes=F.expr("op = 'D'"),
    except_column_list=["op", "_ingest_run_id"],
    stored_as_scd_type=2,
)
```

## SCD2 `MERGE` on Delta (sketch)

The local implementation rebuilds the history of *affected keys* from the change log
and swaps them in. On Delta, the same step is a single atomic `MERGE`:

```sql
MERGE INTO silver.dim_customer AS t
USING rebuilt_affected_keys AS s          -- output of build_scd2() for affected keys
ON  t.customer_id = s.customer_id AND t.effective_from = s.effective_from
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE AND t.customer_id IN (SELECT customer_id FROM affected_keys) THEN DELETE
```

## Deploying the bundle

`databricks.yml` is an example [Databricks Asset Bundle](https://docs.databricks.com/dev-tools/bundles/):

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run      -t dev lakehouse_medallion_job
```
