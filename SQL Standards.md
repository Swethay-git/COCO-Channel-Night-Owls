---
name: team-sql-standards
description: "Use for SQL naming conventions, coding standards, ETL patterns, table design templates, and best practices for the POC pipeline team. Triggers: naming convention, SQL standard, best practice, template, how should I name, create table template, ETL pattern, coding standard, style guide."
---

# Team SQL Standards — POC Pipeline

## Naming Conventions

### Databases
| Pattern | Example |
|---------|---------|
| `DB_POC_<LAYER>` | `DB_POC_RAW`, `DB_POC_HARMONIZED` |

### Schemas
| Pattern | Example | Usage |
|---------|---------|-------|
| `SCH_FAST` | `DB_POC_RAW.SCH_FAST` | Real-time/streaming raw data |
| `SCH_COMMON_DATA_MODEL` | `DB_POC_HARMONIZED.SCH_COMMON_DATA_MODEL` | Dimension/fact tables |
| `SCH_COMMON_UTILS` | `DB_POC_HARMONIZED.SCH_COMMON_UTILS` | Tracking, utilities, monitoring |

### Tables
| Pattern | Example | Usage |
|---------|---------|-------|
| `TB_DIM_<ENTITY>` | `TB_DIM_SAMPLE` | Dimension tables |
| `TB_FACT_<ENTITY>` | `TB_FACT_ORDERS` | Fact tables |
| `TB_<PURPOSE>_TRACKING` | `TB_HARMONIZED_TRACKING` | Audit/tracking tables |
| `EVENT_LOG` | `EVENT_LOG` | Raw event ingestion |

### Views
| Pattern | Example | Usage |
|---------|---------|-------|
| `VW_POC_<ENTITY>_<ACTION>` | `VW_POC_SAMPLE_INSERT` | Incremental processing views |
| `VW_PIPELINE_<PURPOSE>` | `VW_PIPELINE_DELAY_MONITOR` | Monitoring views |

### Procedures
| Pattern | Example | Usage |
|---------|---------|-------|
| `SP_POC_LOAD_<TARGET>` | `SP_POC_LOAD_HARMONIZED` | ETL load procedures |
| `SP_POC_<PURPOSE>` | `SP_POC_CATCHUP_PROCESSOR` | Utility procedures |

### Tasks
| Pattern | Example | Usage |
|---------|---------|-------|
| `TASK_POC_<PURPOSE>` | `TASK_POC_LOAD` | Scheduled ETL tasks |
| `TASK_POC_<PURPOSE>_MONITOR` | `TASK_POC_CATCHUP_MONITOR` | Monitoring tasks |

### Roles & Warehouses
| Pattern | Example | Usage |
|---------|---------|-------|
| `ROLE_POC_<LEVEL>` | `ROLE_POC_ADMIN`, `ROLE_POC_ENGINEER` | Access roles |
| `WH_POC_<SIZE>` | `WH_POC_XS` | Compute warehouses |

---

## Role Hierarchy

```
ACCOUNTADMIN
  └── ROLE_POC_ADMIN        (database creation, admin ops)
        └── ROLE_POC_ENGINEER  (ETL execution, table CRUD, task management)
```

---

## ETL Metadata Columns

Every table MUST include these audit columns:

| Column | Type | Default | Purpose |
|--------|------|---------|---------|
| `_ETL_CREATE_TS` | TIMESTAMP_NTZ | CURRENT_TIMESTAMP() | Row creation time |
| `_ETL_UPDATE_TS` | TIMESTAMP_NTZ | CURRENT_TIMESTAMP() | Last update time |
| `_ROW_HASH` | VARCHAR | MD5 of business columns | Change detection |
| `DELETE_FLAG` | BOOLEAN | FALSE | Soft delete indicator |
| `SRC_EVENT_CREATE_TIME` | FLOAT | — | Source event timestamp |

---

## Table Templates

### Dimension Table (SCD1)

```sql
CREATE OR REPLACE TABLE DB_POC_HARMONIZED.SCH_COMMON_DATA_MODEL.TB_DIM_<ENTITY> (
    <ENTITY>_ID VARCHAR(50000) NOT NULL,
    <ENTITY>_NAME VARCHAR(50000),
    <ENTITY>_STATUS VARCHAR(50000),
    _ROW_HASH VARCHAR,
    _ETL_CREATE_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _ETL_UPDATE_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SRC_EVENT_CREATE_TIME FLOAT,
    DELETE_FLAG BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (<ENTITY>_ID)
) COMMENT = '<Entity> dimension table - SCD Type 1';
```

### Tracking Table

```sql
CREATE TABLE IF NOT EXISTS DB_POC_HARMONIZED.SCH_COMMON_UTILS.TB_<PURPOSE>_TRACKING (
    TRACKING_ID NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    SOURCE VARCHAR(100),
    STATUS VARCHAR(50),
    PROCESS_TRACKING_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    DATA_START_TIMESTAMP_EPOCH FLOAT,
    DATA_END_TIMESTAMP_EPOCH FLOAT,
    ERROR_MESSAGE VARCHAR(1677216),
    PRIMARY KEY (TRACKING_ID)
) COMMENT = 'Tracks processing windows for <purpose>';
```

### Raw Event Table

```sql
CREATE TABLE IF NOT EXISTS DB_POC_RAW.SCH_FAST.<SOURCE>_LOG (
    RECORD_CONTENT VARIANT,
    RECORD_METADATA VARIANT,
    _ETL_CREATE_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Raw event log from <source>';
```

---

## ETL Patterns

### SCD1 MERGE Pattern

```sql
MERGE INTO DB_POC_HARMONIZED.SCH_COMMON_DATA_MODEL.TB_DIM_<ENTITY> tgt
USING <SOURCE_VIEW> src
ON tgt.<ENTITY>_ID = src.<ENTITY>_ID
WHEN MATCHED AND tgt._ROW_HASH != src._ROW_HASH THEN
    UPDATE SET
        tgt.<COL1> = src.<COL1>,
        tgt.<COL2> = src.<COL2>,
        tgt._ROW_HASH = src._ROW_HASH,
        tgt._ETL_UPDATE_TS = CURRENT_TIMESTAMP(),
        tgt.SRC_EVENT_CREATE_TIME = src.SRC_EVENT_CREATE_TIME
WHEN NOT MATCHED THEN
    INSERT (<ENTITY>_ID, <COL1>, <COL2>, _ROW_HASH, _ETL_CREATE_TS, _ETL_UPDATE_TS, SRC_EVENT_CREATE_TIME, DELETE_FLAG)
    VALUES (src.<ENTITY>_ID, src.<COL1>, src.<COL2>, src._ROW_HASH, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src.SRC_EVENT_CREATE_TIME, FALSE);
```

### Row Hash Pattern

```sql
MD5(
    COALESCE(<COL1>::VARCHAR, '') || '|' ||
    COALESCE(<COL2>::VARCHAR, '') || '|' ||
    COALESCE(<COL3>::VARCHAR, '')
) AS _ROW_HASH
```

### Incremental View Pattern

```sql
CREATE OR REPLACE VIEW DB_POC_RAW.SCH_FAST.VW_POC_<ENTITY>_<ACTION> AS
WITH tracking AS (
    SELECT
        MAX(DATA_START_TIMESTAMP_EPOCH) AS DATA_START_TIMESTAMP_MAX,
        MAX(DATA_END_TIMESTAMP_EPOCH) AS DATA_END_TIMESTAMP_MAX
    FROM DB_POC_HARMONIZED.SCH_COMMON_UTILS.TB_HARMONIZED_TRACKING
    WHERE STATUS = 'STARTED' AND SOURCE = '<SOURCE>'
)
SELECT
    RECORD_CONTENT:<id_field>::VARCHAR AS <ENTITY>_ID,
    RECORD_CONTENT:<field1>::VARCHAR AS <FIELD1>,
    RECORD_METADATA:SnowflakeConnectorPushTime::FLOAT AS SRC_EVENT_CREATE_TIME,
    MD5(COALESCE(RECORD_CONTENT:<id_field>::VARCHAR, '') || '|' || COALESCE(RECORD_CONTENT:<field1>::VARCHAR, '')) AS _ROW_HASH
FROM DB_POC_RAW.SCH_FAST.EVENT_LOG e, tracking t
WHERE RECORD_CONTENT:Event::VARCHAR = '<EventType>'
    AND RECORD_METADATA:SnowflakeConnectorPushTime > t.DATA_START_TIMESTAMP_MAX
    AND RECORD_METADATA:SnowflakeConnectorPushTime <= t.DATA_END_TIMESTAMP_MAX;
```

### Task Schedule Pattern

```sql
CREATE OR REPLACE TASK DB_POC_HARMONIZED.SCH_COMMON_UTILS.TASK_POC_<PURPOSE>
    WAREHOUSE = WH_POC_XS
    SCHEDULE = 'USING CRON <cron_expr> UTC'
    COMMENT = '<description>'
AS
    CALL DB_POC_HARMONIZED.<SCHEMA>.SP_POC_<PURPOSE>();
```

---

## SQL Style Rules

1. **Keywords:** UPPERCASE (`SELECT`, `FROM`, `WHERE`)
2. **Identifiers:** UPPER_SNAKE_CASE (`SAMPLE_ID`, `TB_DIM_SAMPLE`)
3. **Aliases:** Lowercase short aliases for joins (`src`, `tgt`, `e`, `t`)
4. **Indentation:** 4 spaces
5. **Comments:** Use `--` for inline, `COMMENT =` property on all objects
6. **Semicolons:** Required after every statement
7. **Fully qualified names:** Always use `DATABASE.SCHEMA.OBJECT`
