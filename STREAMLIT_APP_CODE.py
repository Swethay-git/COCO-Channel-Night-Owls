import streamlit as st
from snowflake.snowpark.context import get_active_session

session = get_active_session()

TRACKING_TABLE = "DB_POC_HARMONIZED.SCH_COMMON_UTILS.TB_HARMONIZED_TRACKING"
EVENT_LOG = "DB_POC_RAW.SCH_FAST.EVENT_LOG"
DIM_TABLE = "DB_POC_HARMONIZED.SCH_COMMON_DATA_MODEL.TB_DIM_SAMPLE"
DELAY_VIEW = "DB_POC_HARMONIZED.SCH_COMMON_UTILS.VW_PIPELINE_DELAY_MONITOR"
CATCHUP_TASK = "DB_POC_HARMONIZED.SCH_COMMON_UTILS.TASK_POC_CATCHUP_MONITOR"
LOAD_TASK = "DB_POC_HARMONIZED.SCH_COMMON_UTILS.TASK_POC_LOAD"

st.markdown("""
<style>
    .block-container {padding-top: 1rem;}
    .app-header {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
        padding: 24px 32px; border-radius: 12px; margin-bottom: 20px; color: white;
    }
    .app-header h1 {margin: 0; font-size: 1.6rem;}
    .app-header p {margin: 4px 0 0 0; opacity: 0.8; font-size: 0.9rem;}
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #f0f4ff 0%, #e8eeff 100%);
        border-radius: 10px; padding: 14px 16px;
        border-left: 4px solid #4361ee; box-shadow: 0 2px 6px rgba(0,0,0,0.06);
    }
    div[data-testid="stMetric"] label {font-size: 0.82rem; color: #555;}
    .section-header {
        background: linear-gradient(90deg, #4361ee 0%, #3a0ca3 100%);
        color: white; padding: 8px 16px; border-radius: 8px;
        font-size: 0.95rem; font-weight: 600; margin: 12px 0 10px 0;
        display: inline-block;
    }
    .lifecycle-step {
        padding: 10px 16px; border-radius: 8px; margin: 4px 0;
        font-size: 0.9rem; text-align: center;
    }
    .step-active {background: #d4edda; border: 2px solid #28a745; color: #155724; font-weight: 600;}
    .step-done {background: #e8e8e8; border: 1px solid #aaa; color: #666; text-decoration: line-through;}
    .step-pending {background: #f8f9fa; border: 1px dashed #ccc; color: #999;}
    .step-arrow {text-align: center; font-size: 1.2rem; color: #888; margin: 2px 0;}
    .hist-card {
        background: #f8f9fc; border-radius: 8px; padding: 14px 18px;
        border-left: 5px solid #4361ee; margin-bottom: 10px;
        box-shadow: 0 1px 4px rgba(0,0,0,0.06);
    }
    .hist-card-fail {border-left-color: #e63946;}
    .hist-card-success {border-left-color: #2a9d8f;}
    .hist-card-skip {border-left-color: #e9c46a;}
    .hist-card-sched {border-left-color: #888;}
    .hist-num {
        display: inline-block; background: #4361ee; color: white;
        border-radius: 50%; width: 26px; height: 26px; text-align: center;
        font-size: 0.8rem; line-height: 26px; margin-right: 8px; font-weight: 700;
    }
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="app-header">
    <h1>Pipeline Catchup Test Dashboard</h1>
    <p>Simulate a pipeline outage, trigger automated catchup, and monitor results in real-time.</p>
</div>
""", unsafe_allow_html=True)

tab1, tab2, tab3 = st.tabs([
    "Setup Test Data",
    "Monitor & Trigger",
    "Results"
])


with tab1:
    st.markdown('<div class="section-header">Prepare Test Scenario</div>', unsafe_allow_html=True)

    col_state, col_action = st.columns([1, 1.2])

    with col_state:
        st.markdown("**Current Table Counts**")
        c1, c2, c3 = st.columns(3)
        c1.metric("EVENT_LOG", session.sql(f"SELECT COUNT(*) AS C FROM {EVENT_LOG}").collect()[0]["C"])
        c2.metric("Tracking", session.sql(f"SELECT COUNT(*) AS C FROM {TRACKING_TABLE}").collect()[0]["C"])
        c3.metric("DIM_SAMPLE", session.sql(f"SELECT COUNT(*) AS C FROM {DIM_TABLE}").collect()[0]["C"])

    with col_action:
        st.markdown("**Actions**")
        a1, a2 = st.columns(2)
        with a1:
            if st.button("Clean All Data"):
                for t in [EVENT_LOG, TRACKING_TABLE, DIM_TABLE]:
                    session.sql(f"TRUNCATE TABLE {t}").collect()
                st.success("Cleaned!")
                st.rerun()
        with a2:
            if st.button("Insert Events + Simulate Delay", type="primary"):
                session.sql(f"""
                    INSERT INTO {EVENT_LOG} (RECORD_CONTENT, RECORD_METADATA)
                    SELECT PARSE_JSON(rc), PARSE_JSON(rm) FROM (
                        SELECT '{{"Event":"SampleInsert","id":"S001","name":"Rahul Sharma","status":"Active","email":"rahul.sharma@corp.com"}}' AS rc,
                               '{{"SnowflakeConnectorPushTime":' || DATE_PART(EPOCH_MILLISECOND, DATEADD('minute', -55, CURRENT_TIMESTAMP())) || '}}' AS rm
                        UNION ALL
                        SELECT '{{"Event":"SampleInsert","id":"S002","name":"Priya Nair","status":"Active","email":"priya.nair@tech.io"}}',
                               '{{"SnowflakeConnectorPushTime":' || DATE_PART(EPOCH_MILLISECOND, DATEADD('minute', -42, CURRENT_TIMESTAMP())) || '}}'
                        UNION ALL
                        SELECT '{{"Event":"SampleInsert","id":"S003","name":"Amit Patel","status":"Pending","email":"amit.patel@data.co"}}',
                               '{{"SnowflakeConnectorPushTime":' || DATE_PART(EPOCH_MILLISECOND, DATEADD('minute', -33, CURRENT_TIMESTAMP())) || '}}'
                        UNION ALL
                        SELECT '{{"Event":"SampleUpdate","id":"S001","name":"Rahul Sharma","status":"Premium","email":"rahul.sharma.vip@corp.com"}}',
                               '{{"SnowflakeConnectorPushTime":' || DATE_PART(EPOCH_MILLISECOND, DATEADD('minute', -24, CURRENT_TIMESTAMP())) || '}}'
                        UNION ALL
                        SELECT '{{"Event":"SampleInsert","id":"S004","name":"Sneha Reddy","status":"Active","email":"sneha.reddy@cloud.dev"}}',
                               '{{"SnowflakeConnectorPushTime":' || DATE_PART(EPOCH_MILLISECOND, DATEADD('minute', -18, CURRENT_TIMESTAMP())) || '}}'
                    )
                """).collect()
                session.sql(f"""
                    INSERT INTO {TRACKING_TABLE}
                        (SOURCE, PROCESS_TRACKING_TIMESTAMP, DATA_START_TIMESTAMP_EPOCH, DATA_END_TIMESTAMP_EPOCH, STATUS)
                    SELECT 'POC_EVENTS', DATEADD('minute', -60, CURRENT_TIMESTAMP()),
                        DATE_PART(EPOCH_MILLISECOND, DATEADD('minute', -75, CURRENT_TIMESTAMP())),
                        DATE_PART(EPOCH_MILLISECOND, DATEADD('minute', -60, CURRENT_TIMESTAMP())), 'COMPLETED'
                """).collect()
                st.success("5 events + 1-hour delay simulated!")
                st.rerun()

    st.divider()
    st.markdown('<div class="section-header">EVENT_LOG Preview</div>', unsafe_allow_html=True)
    events_df = session.sql(f"""
        SELECT
            ROW_NUMBER() OVER (ORDER BY RECORD_METADATA:SnowflakeConnectorPushTime DESC) AS SNO,
            RECORD_CONTENT:Event::VARCHAR AS EVENT,
            RECORD_CONTENT:id::VARCHAR AS ID,
            RECORD_CONTENT:name::VARCHAR AS NAME,
            RECORD_CONTENT:status::VARCHAR AS STATUS,
            RECORD_CONTENT:email::VARCHAR AS EMAIL,
            TO_TIMESTAMP(RECORD_METADATA:SnowflakeConnectorPushTime::NUMBER / 1000) AS PUSH_TIME
        FROM {EVENT_LOG} ORDER BY RECORD_METADATA:SnowflakeConnectorPushTime DESC
    """).to_pandas()

    if len(events_df) > 0:
        st.caption("Sorted latest first.")
        st.dataframe(events_df,use_container_width=True)
    else:
        st.info("No events yet. Click **Insert Events + Simulate Delay** above.")


with tab2:
    st.markdown('<div class="section-header">Pipeline Delay Monitor</div>', unsafe_allow_html=True)

    delay_df = session.sql(f"""
        SELECT TRACKING_ID, STATUS, LATEST_PROCESSING_DELAY_MINS,
            ACTUAL_DELAY_MINS, NEEDS_CATCHUP, CATCHUP_ITERATIONS_NEEDED
        FROM {DELAY_VIEW} ORDER BY TRACKING_ID DESC LIMIT 1
    """).to_pandas()

    if len(delay_df) > 0:
        row = delay_df.iloc[0]
        delay_mins = int(row["LATEST_PROCESSING_DELAY_MINS"])
        needs = bool(row["NEEDS_CATCHUP"])
        iters = int(row["CATCHUP_ITERATIONS_NEEDED"])

        c1, c2, c3 = st.columns(3)
        c1.metric("Delay (mins)", delay_mins, delta=f"{'OVER THRESHOLD' if delay_mins > 15 else 'OK'}", delta_color="inverse")
        c2.metric("Needs Catchup?", "YES" if needs else "NO")
        c3.metric("Iterations Needed", iters)
    else:
        delay_mins = 0
        needs = False
        iters = 0
        st.info("No tracking data yet. Go to **Setup Test Data** tab first.")

    st.divider()

    monitor_state = session.sql("SHOW TASKS LIKE 'TASK_POC_CATCHUP_MONITOR' IN SCHEMA DB_POC_HARMONIZED.SCH_COMMON_UTILS").collect()[0]["state"]
    load_state = session.sql("SHOW TASKS LIKE 'TASK_POC_LOAD' IN SCHEMA DB_POC_HARMONIZED.SCH_COMMON_UTILS").collect()[0]["state"]

    catchup_completed = session.sql(f"""
        SELECT COUNT(*) AS C FROM {TRACKING_TABLE}
        WHERE SOURCE = 'CATCHUP_MONITOR' AND STATUS IN ('CATCHUP_COMPLETED', 'CATCHUP_TRIGGERED')
    """).collect()[0]["C"]

    is_catchup_done = catchup_completed > 0

    col_control, col_lifecycle = st.columns([1, 1.2])

    with col_control:
        st.markdown('<div class="section-header">Task Status</div>', unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        c1.metric("Catchup Monitor", monitor_state)
        c2.metric("Load Task", load_state)

        st.markdown("---")

        if is_catchup_done and delay_mins <= 15:
            st.success("Catchup **completed** successfully! Pipeline is current.")
            if monitor_state.lower() == "started":
                st.markdown("The monitor task is still running. You can suspend it now.")
                if st.button("Suspend Monitor (Catchup Done)", type="primary"):
                    session.sql(f"ALTER TASK {CATCHUP_TASK} SUSPEND").collect()
                    st.info("Monitor suspended.")
                    st.rerun()
        elif monitor_state.lower() == "suspended":
            st.markdown("Click below to **start the automated catchup**. The monitor will detect the delay, suspend TASK_POC_LOAD, run catchup iterations, then resume it.")
            if st.button("Trigger Catchup Pipeline", type="primary"):
                session.sql(f"ALTER TASK {CATCHUP_TASK} RESUME").collect()
                st.success("Catchup Monitor started! It will fire on the next 2-min cron tick.")
                st.rerun()
        else:
            st.warning("Catchup Monitor is **running**. Catchup in progress — click Refresh to check status.")

    with col_lifecycle:
        st.markdown('<div class="section-header">Catchup Lifecycle</div>', unsafe_allow_html=True)

        all_done = is_catchup_done and delay_mins <= 15

        def step_class(active, done):
            if done:
                return "step-done"
            if active:
                return "step-active"
            return "step-pending"

        if all_done:
            steps = [
                ("Delay Detected (> 15 mins)", False, True, "Resolved"),
                ("Catchup Monitor Fired", False, True, "Completed"),
                ("TASK_POC_LOAD Suspended", False, True, "Was suspended during catchup"),
                ("Catchup Iterations Completed", False, True, "All windows processed"),
                ("TASK_POC_LOAD Resumed", False, True, f"Load Task: {load_state.upper()}"),
            ]
        else:
            s1_active = delay_mins > 15 and not is_catchup_done
            s2_active = monitor_state.lower() == "started" and not is_catchup_done and not s1_active
            s3_active = not is_catchup_done and monitor_state.lower() == "started" and load_state.lower() == "suspended"
            s4_active = s3_active
            s5_active = False

            steps = [
                ("Delay Detected (> 15 mins)", s1_active, delay_mins > 15, f"Delay: {delay_mins} mins"),
                ("Catchup Monitor Fires", s2_active, monitor_state.lower() == "started", f"Monitor: {monitor_state}"),
                ("TASK_POC_LOAD Suspended", s3_active, False, f"Load Task: {load_state.upper()}"),
                ("Catchup Iterations Running", s4_active, False, f"{iters} iteration(s) needed"),
                ("TASK_POC_LOAD Resumed", s5_active, False, f"Load Task: {load_state.upper()}"),
            ]

        for i, (title, active, done, detail) in enumerate(steps):
            css = step_class(active, done)
            marker = "Done" if done else ("In Progress" if active else "Waiting")
            show_detail = f"  —  {detail}" if (active or done) else ""
            st.markdown(f'<div class="lifecycle-step {css}">{title}{show_detail} [{marker}]</div>', unsafe_allow_html=True)
            if i < len(steps) - 1:
                st.markdown('<div class="step-arrow">|</div>', unsafe_allow_html=True)

        if all_done:
            st.markdown('<div style="text-align:center;margin-top:12px;padding:10px;background:#d4edda;border-radius:8px;color:#155724;font-weight:600;">CATCHUP COMPLETE</div>', unsafe_allow_html=True)

    if st.button("Refresh Status", key="r2"):
        st.rerun()


with tab3:
    if st.button("Refresh", type="primary", key="r3"):
        st.rerun()

    st.markdown('<div class="section-header">Tracking Table (Pipeline Log)</div>', unsafe_allow_html=True)
    tracking_df = session.sql(f"""
        SELECT
            ROW_NUMBER() OVER (ORDER BY TRACKING_ID DESC) AS SNO,
            TRACKING_ID, SOURCE, STATUS, PROCESS_TRACKING_TIMESTAMP,
            DATA_START_TIMESTAMP_EPOCH, DATA_END_TIMESTAMP_EPOCH, ERROR_MESSAGE
        FROM {TRACKING_TABLE} ORDER BY TRACKING_ID DESC
    """).to_pandas()

    if len(tracking_df) > 0:
        st.caption("Sorted latest first.")
        st.dataframe(tracking_df,use_container_width=True)
    else:
        st.info("No tracking records yet.")

    st.markdown('<div class="section-header">TB_DIM_SAMPLE (Target Table)</div>', unsafe_allow_html=True)
    dim_df = session.sql(f"""
        SELECT
            ROW_NUMBER() OVER (ORDER BY _ETL_CREATE_TS DESC) AS SNO,
            SAMPLE_ID, SAMPLE_NAME, SAMPLE_STATUS, SAMPLE_EMAIL, _ETL_CREATE_TS, _ETL_UPDATE_TS
        FROM {DIM_TABLE} ORDER BY _ETL_CREATE_TS DESC
    """).to_pandas()

    if len(dim_df) > 0:
        st.caption("Target dimension table.")
        st.dataframe(dim_df,use_container_width=True)
    else:
        st.info("No dimension records yet.")

    st.markdown('<div class="section-header">Delay Status (Latest 5)</div>', unsafe_allow_html=True)
    delay_res = session.sql(f"""
        SELECT
            ROW_NUMBER() OVER (ORDER BY TRACKING_ID DESC) AS SNO,
            TRACKING_ID, STATUS, LATEST_PROCESSING_DELAY_MINS, NEEDS_CATCHUP, CATCHUP_ITERATIONS_NEEDED
        FROM {DELAY_VIEW} ORDER BY TRACKING_ID DESC LIMIT 5
    """).to_pandas()

    if len(delay_res) > 0:
        st.caption("Latest delay status.")
        st.dataframe(delay_res,use_container_width=True)
    else:
        st.info("No delay data yet.")