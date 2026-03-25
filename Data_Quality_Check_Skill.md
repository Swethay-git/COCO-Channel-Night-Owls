---
name: data-quality-checks
description: "Use for all data quality checks, validation, freshness, row counts, null checks, duplicate detection, SCD validation on POC pipeline tables. Triggers: data quality, check data, validate data, null check, duplicates, freshness, row count, data issues, is data correct, stale data, missing data, SCD check."
---

# Data Quality Checks — POC Pipeline

## Tables to Monitor

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `DB_POC_RAW.SCH_FAST.EVENT_LOG` | Raw events | RECORD_CONTENT, RECORD_METADATA |
| `DB_POC_HARMONIZED.SCH_COMMON_DATA_MODEL.TB_DIM_SAMPLE` | SCD1 dimension | SAMPLE_ID, SAMPLE_NAME, SAMPLE_STATUS, SAMPLE_EMAIL |
| `DB_POC_HARMONIZED.SCH_COMMON_UTILS.TB_HARMONIZED_TRACKING` | Pipeline audit | TRACKING_ID, SOURCE, STATUS |

---

## Queries

### 1. Row Counts Across Pipeline

**Triggered by:** "row counts", "how many records?", "table sizes", "data volume"

```sql
SELECT 'EVENT_LOG (Raw)' AS table_name, COUNT(*) AS row_count FROM DB_POC_RAW.SCH_FAST.EVENT_LOG
UNION ALL
SELECT 'TB_DIM_SAMPLE (Harmonized)', COUNT(*) FROM DB_POC_HARMONIZED.SCH_COMMON_DATA_MODEL.TB_DIM_SAMPLE
UNION ALL
SELECT 'TB_HARMONIZED_TRACKING (Audit)', COUNT(*) FROM DB_POC_HARMONIZED.SCH_COMMON_UTILS.TB_HARMONIZED_TRACKING;
```

### 2. Null Check — Dimension Table

**Triggered by:** "null check", "missing values", "nulls in data"

```sql
SELECT
    COUNT(*) AS total_rows,
    COUNT(*) - COUNT(SAMPLE_ID) AS null_sample_id,
    COUNT(*) - COUNT(SAMPLE_NAME) AS null_sample_name,
    COUNT(*) - COUNT(SAMPLE_STATUS) AS null_sample_status,
    COUNT(*) - COUNT(SAMPLE_EMAIL) AS null_sample_email,
    COUNT(*) - COUNT(_ROW_HASH) AS null_row_hash
FROM DB_POC_HARMONIZED.SCH_COMMON_DATA_MODEL.TB_DIM_SAMPLE;
```

### 3. Duplicate Check — Dimension Table

**Triggered by:** "duplicates", "duplicate records", "duplicate SAMPLE_ID"

```sql
SELECT SAMPLE_ID, COUNT(*) AS occurrences
FROM DB_POC_HARMONIZED.SCH_COMMON_DATA_MODEL.TB_DIM_SAMPLE
GROUP BY SAMPLE_ID
HAVING COUNT(*) > 1
ORDER BY occurrences DESC;
```

### 4. Data Freshness — When Was Data Last Updated?

**Triggered by:** "data freshness", "stale data", "when was data last loaded?", "last update"

```sql
SELECT 
    'TB_DIM_SAMPLE' AS table_name,
    MAX(_ETL_CREATE_TS) AS latest_insert,
    MAX(_ETL_UPDATE_TS) AS latest_update,
    DATEDIFF('minute', MAX(_ETL_UPDATE_TS), CURRENT_TIMESTAMP()) AS minutes_since_last_update
FROM DB_POC_HARMONIZED.SCH_COMMON_DATA_MODEL.TB_DIM_SAMPLE;
```

### 5. Event Type Distribution (Raw Layer)

**Triggered by:** "event types", "what events?", "event distribution"

```sql
SELECT 
    RECORD_CONTENT:Event::STRING AS event_type,
    COUNT(*) AS event_count,
    MIN(TO_TIMESTAMP(RECORD_METADATA:SnowflakeConnectorPushTime::NUMBER / 1000)) AS earliest,
    MAX(TO_TIMESTAMP(RECORD_METADATA:SnowflakeConnectorPushTime::NUMBER / 1000)) AS latest
FROM DB_POC_RAW.SCH_FAST.EVENT_LOG
GROUP BY event_type
ORDER BY event_count DESC;
```

### 6. SCD1 Validation — Records Modified After Creation

**Triggered by:** "SCD check", "updated records", "SCD validation", "changed records"

```sql
SELECT 
    SAMPLE_ID, SAMPLE_NAME, SAMPLE_STATUS, SAMPLE_EMAIL,
    _ETL_CREATE_TS, _ETL_UPDATE_TS,
    CASE WHEN _ETL_UPDATE_TS > _ETL_CREATE_TS THEN 'UPDATED' ELSE 'ORIGINAL' END AS record_state
FROM DB_POC_HARMONIZED.SCH_COMMON_DATA_MODEL.TB_DIM_SAMPLE
ORDER BY _ETL_UPDATE_TS DESC;
```

### 7. Orphan Events — Raw Events Not Yet Processed

**Triggered by:** "unprocessed events", "orphan events", "pending events", "events not loaded"

```sql
WITH last_processed AS (
    SELECT MAX(DATA_END_TIMESTAMP_EPOCH) AS max_epoch
    FROM DB_POC_HARMONIZED.SCH_COMMON_UTILS.TB_HARMONIZED_TRACKING
    WHERE STATUS = 'COMPLETED' AND SOURCE = 'POC_EVENTS'
)
SELECT COUNT(*) AS unprocessed_events
FROM DB_POC_RAW.SCH_FAST.EVENT_LOG e, last_processed lp
WHERE RECORD_METADATA:SnowflakeConnectorPushTime > lp.max_epoch;
```

### 8. Tracking Table Status Distribution

**Triggered by:** "tracking status", "pipeline run statuses", "how many failures?"

```sql
SELECT 
    STATUS, 
    COUNT(*) AS count,
    MIN(PROCESS_TRACKING_TIMESTAMP) AS earliest,
    MAX(PROCESS_TRACKING_TIMESTAMP) AS latest
FROM DB_POC_HARMONIZED.SCH_COMMON_UTILS.TB_HARMONIZED_TRACKING
GROUP BY STATUS
ORDER BY count DESC;
```

### 9. Raw vs Harmonized Reconciliation

**Triggered by:** "reconciliation", "data mismatch", "raw vs harmonized", "record count match"

```sql
WITH raw_counts AS (
    SELECT 
        COUNT(DISTINCT RECORD_CONTENT:id::VARCHAR) AS distinct_ids_raw
    FROM DB_POC_RAW.SCH_FAST.EVENT_LOG
    WHERE RECORD_CONTENT:Event IN ('SampleInsert', 'SampleUpdate')
),
harmonized_counts AS (
    SELECT COUNT(*) AS total_harmonized
    FROM DB_POC_HARMONIZED.SCH_COMMON_DATA_MODEL.TB_DIM_SAMPLE
)
SELECT 
    r.distinct_ids_raw,
    h.total_harmonized,
    r.distinct_ids_raw - h.total_harmonized AS difference
FROM raw_counts r, harmonized_counts h;
```

### 10. Status Value Distribution

**Triggered by:** "status values", "what statuses exist?", "status breakdown"

```sql
SELECT 
    SAMPLE_STATUS, 
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS percentage
FROM DB_POC_HARMONIZED.SCH_COMMON_DATA_MODEL.TB_DIM_SAMPLE
GROUP BY SAMPLE_STATUS
ORDER BY count DESC;
```
