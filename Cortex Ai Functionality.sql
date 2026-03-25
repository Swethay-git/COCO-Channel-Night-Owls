-- Cortex AI: Classify and analyze event logs
SELECT 
    RECORD_CONTENT:Event::STRING AS event_type,
    RECORD_CONTENT:name::STRING AS name,
    RECORD_CONTENT:status::STRING AS status,
    AI_CLASSIFY(
        RECORD_CONTENT:Event::STRING, 
        ['Data Creation', 'Data Modification', 'Data Deletion', 'System Event']
    ) AS ai_event_category,
    AI_COMPLETE(
        'llama3.1-8b',
        'In one short sentence, describe what this event means for data governance: ' || RECORD_CONTENT::STRING
    ) AS ai_insight
FROM DB_POC_RAW.SCH_FAST.EVENT_LOG
LIMIT 5;
