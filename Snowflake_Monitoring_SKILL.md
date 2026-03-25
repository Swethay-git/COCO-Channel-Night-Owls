---
name: snowflake-monitoring
description: "Use for all POC data pipeline monitoring, troubleshooting, and operations. Triggers: pipeline status, task status, delay check, catchup, tracking, event log, harmonized load, pipeline health, task history, error investigation, pipeline ops, is my pipeline running, why is pipeline delayed, check pipeline."
---

# POC Data Pipeline Monitoring & Operations

## Pipeline Architecture

```
[Kafka/Connector] → EVENT_LOG (raw) → TASK_POC_LOAD (every 15 min) → TB_DIM_SAMPLE (harmonized)
                                      ↑
                         TASK_POC_CATCHUP_MONITOR (every 5 min) → SP_CATCHUP_PROCESSOR (if delay > 15 min)
```

### Key Objects

| Object | Type | Location |
|--------|------|----------|
| `DB_POC_RAW.SCH_FAST.EVENT_LOG` | Table | Raw events (VARIANT) |
| `DB_POC_RAW.SCH_FAST.VW_POC_SAMPLE_INSERT` | View | Incremental event reader |
| `DB_POC_HARMONIZED.SCH_COMMON_DATA_MODEL.TB_DIM_SAMPLE` | Table | SCD1 dimension table |
| `DB_POC_HARMONIZED.SCH_COMMON_UTILS.TB_HARMONIZED_TRACKING` | Table | Pipeline tracking/audit |
| `DB_POC_HARMONIZED.SCH_COMMON_UTILS.VW_PIPELINE_DELAY_MONITOR` | View | Delay monitoring |
| `DB_POC_HARMONIZED.SCH_COMMON_DATA_MODEL.SP_POC_LOAD_HARMONIZED` | Procedure | Main ETL load |
| `DB_POC_HARMONIZED.SCH_COMMON_UTILS.SP_CATCHUP_PROCESSOR` | Procedure | Catchup orchestrator |
| `DB_POC_HARMONIZED.SCH_COMMON_UTILS.TASK_POC_LOAD` | Task | Scheduled load (every 15 min) |
| `DB_POC_HARMONIZED.SCH_COMMON_UTILS.TASK_POC_CATCHUP_MONITOR` | Task | Delay monitor (every 5 min) |

### Roles & Warehouses
- **Role:** `ROLE_POC_ENGINEER`
- **Warehouse:** `WH_POC_XS`

---

## Monitoring Queries

### 1. Pipeline Health Check (Current Status)

**Triggered by:** "is my pipeline running?", "pipeline status", "health check", "pipeline health"

```sql
SELECT 
    TRACKING_ID, STATUS, 
    LATEST_PROCESSING_DELAY_MINS,
    ACTUAL_DELAY_MINS, 
    NEEDS_CATCHUP, 
    CATCHUP_ITERATIONS_NEEDED
FROM DB_POC_HARMONIZED.SCH_COMMON_UTILS.VW_PIPELINE_DELAY_MONITOR 
ORDER BY TRACKING_ID DESC 
LIMIT 1;
```

### 2. Task Run History (Last 24 Hours)

**Triggered by:** "task history", "task runs", "when did the task last run?"

```sql
SELECT 
    NAME, STATE, 
    SCHEDULED_TIME, COMPLETED_TIME,
    DATEDIFF('second', SCHEDULED_TIME, COMPLETED_TIME) AS duration_seconds,
    ERROR_CODE, ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TASK_POC_LOAD',
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 50
))
ORDER BY SCHEDULED_TIME DESC;
```

### 3. Task State Check

**Triggered by:** "is the task running?", "task state", "check task"

```sql
SHOW TASKS LIKE 'TASK_POC%' IN SCHEMA DB_POC_HARMONIZED.SCH_COMMON_UTILS;
```

### 4. Pipeline Tracking Log (Recent Activity)

**Triggered by:** "tracking log", "pipeline log", "recent runs", "audit log"

```sql
SELECT 
    TRACKING_ID, SOURCE, STATUS, 
    PROCESS_TRACKING_TIMESTAMP,
    TO_TIMESTAMP(DATA_START_TIMESTAMP_EPOCH / 1000) AS data_start,
    TO_TIMESTAMP(DATA_END_TIMESTAMP_EPOCH / 1000) AS data_end,
    ERROR_MESSAGE
FROM DB_POC_HARMONIZED.SCH_COMMON_UTILS.TB_HARMONIZED_TRACKING
ORDER BY PROCESS_TRACKING_TIMESTAMP DESC
LIMIT 20;
```

### 5. Failed Runs Investigation

**Triggered by:** "failures", "errors", "what failed?", "pipeline errors"

```sql
SELECT 
    TRACKING_ID, SOURCE, STATUS, 
    PROCESS_TRACKING_TIMESTAMP,
    ERROR_MESSAGE,
    SQLERRM
FROM DB_POC_HARMONIZED.SCH_COMMON_UTILS.TB_HARMONIZED_TRACKING
WHERE STATUS = 'FAILED'
ORDER BY PROCESS_TRACKING_TIMESTAMP DESC
LIMIT 10;
```

### 6. Pipeline Delay Trend

**Triggered by:** "delay trend", "is pipeline falling behind?", "delay history"

```sql
SELECT 
    TRACKING_ID,
    PROCESS_TRACKING_TIMESTAMP,
    LATEST_PROCESSING_DELAY_MINS,
    ACTUAL_DELAY_MINS,
    NEEDS_CATCHUP
FROM DB_POC_HARMONIZED.SCH_COMMON_UTILS.VW_PIPELINE_DELAY_MONITOR
ORDER BY PROCESS_TRACKING_TIMESTAMP DESC
LIMIT 20;
```

### 7. Catchup History

**Triggered by:** "catchup history", "was catchup triggered?", "catchup runs"

```sql
SELECT 
    TRACKING_ID, STATUS, ERROR_MESSAGE,
    PROCESS_TRACKING_TIMESTAMP
FROM DB_POC_HARMONIZED.SCH_COMMON_UTILS.TB_HARMONIZED_TRACKING
WHERE SOURCE = 'CATCHUP_MONITOR' OR STATUS = 'Catchup Completed'
ORDER BY PROCESS_TRACKING_TIMESTAMP DESC
LIMIT 10;
```

### 8. Raw Event Volume

**Triggered by:** "how many events?", "event volume", "incoming data"

```sql
SELECT 
    COUNT(*) AS total_events,
    COUNT(CASE WHEN RECORD_CONTENT:Event = 'SampleInsert' THEN 1 END) AS inserts,
    COUNT(CASE WHEN RECORD_CONTENT:Event = 'SampleUpdate' THEN 1 END) AS updates,
    MIN(TO_TIMESTAMP(RECORD_METADATA:SnowflakeConnectorPushTime::NUMBER / 1000)) AS earliest_event,
    MAX(TO_TIMESTAMP(RECORD_METADATA:SnowflakeConnectorPushTime::NUMBER / 1000)) AS latest_event
FROM DB_POC_RAW.SCH_FAST.EVENT_LOG;
```

### 9. Harmonized Data Summary

**Triggered by:** "how many records?", "dimension table status", "harmonized data"

```sql
SELECT 
    COUNT(*) AS total_records,
    COUNT(CASE WHEN DELETE_FLAG = FALSE THEN 1 END) AS active_records,
    COUNT(CASE WHEN DELETE_FLAG = TRUE THEN 1 END) AS deleted_records,
    MIN(_ETL_CREATE_TS) AS earliest_load,
    MAX(_ETL_UPDATE_TS) AS latest_update
FROM DB_POC_HARMONIZED.SCH_COMMON_DATA_MODEL.TB_DIM_SAMPLE;
```

---

## Operations

### Suspend / Resume Tasks

```sql
ALTER TASK DB_POC_HARMONIZED.SCH_COMMON_UTILS.TASK_POC_CATCHUP_MONITOR SUSPEND;
ALTER TASK DB_POC_HARMONIZED.SCH_COMMON_UTILS.TASK_POC_LOAD SUSPEND;

ALTER TASK DB_POC_HARMONIZED.SCH_COMMON_UTILS.TASK_POC_LOAD RESUME;
ALTER TASK DB_POC_HARMONIZED.SCH_COMMON_UTILS.TASK_POC_CATCHUP_MONITOR RESUME;
```

### Manual Catchup

```sql
CALL DB_POC_HARMONIZED.SCH_COMMON_UTILS.SP_CATCHUP_PROCESSOR(50, 15);
```

### Manual Load

```sql
CALL DB_POC_HARMONIZED.SCH_COMMON_DATA_MODEL.SP_POC_LOAD_HARMONIZED();
```
