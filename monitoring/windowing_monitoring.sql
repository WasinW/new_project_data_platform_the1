-- monitoring/windowing_monitoring.sql
-- BigQuery views for monitoring windowing performance

-- Window processing statistics
CREATE OR REPLACE VIEW `{project_id}.{domain}_audit.window_processing_stats` AS
SELECT 
    DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', window_start)) as processing_date,
    EXTRACT(HOUR FROM PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', window_start)) as processing_hour,
    window_start,
    window_end,
    pipeline_mode,
    pipeline_name,
    COUNT(*) as window_count,
    SUM(record_count) as total_records,
    AVG(record_count) as avg_records_per_window,
    MIN(record_count) as min_records_per_window,
    MAX(record_count) as max_records_per_window,
    STDDEV(record_count) as stddev_records_per_window,
    MIN(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', audit_timestamp)) as first_processed,
    MAX(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', audit_timestamp)) as last_processed,
    TIMESTAMP_DIFF(
        MAX(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', audit_timestamp)),
        MIN(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', audit_timestamp)),
        SECOND
    ) as processing_duration_seconds
FROM `{project_id}.{domain}_audit.window_summaries`
WHERE DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', audit_timestamp)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY 
    processing_date, 
    processing_hour,
    window_start, 
    window_end, 
    pipeline_mode,
    pipeline_name
ORDER BY processing_date DESC, processing_hour DESC;

-- Window latency analysis
CREATE OR REPLACE VIEW `{project_id}.{domain}_audit.window_latency_analysis` AS
SELECT 
    pipeline_mode,
    pipeline_name,
    DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', window_start)) as processing_date,
    EXTRACT(HOUR FROM PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', window_start)) as hour_of_day,
    COUNT(*) as window_count,
    -- Latency from window end to processing
    AVG(TIMESTAMP_DIFF(
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', audit_timestamp),
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', window_end),
        SECOND
    )) as avg_window_latency_seconds,
    PERCENTILE_CONT(TIMESTAMP_DIFF(
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', audit_timestamp),
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', window_end),
        SECOND
    ), 0.5) OVER (PARTITION BY pipeline_mode, DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', window_start))) as median_latency_seconds,
    PERCENTILE_CONT(TIMESTAMP_DIFF(
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', audit_timestamp),
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', window_end),
        SECOND
    ), 0.95) OVER (PARTITION BY pipeline_mode, DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', window_start))) as p95_latency_seconds,
    PERCENTILE_CONT(TIMESTAMP_DIFF(
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', audit_timestamp),
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', window_end),
        SECOND
    ), 0.99) OVER (PARTITION BY pipeline_mode, DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', window_start))) as p99_latency_seconds,
    -- Window duration analysis
    AVG(TIMESTAMP_DIFF(
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', window_end),
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', window_start),
        SECOND
    )) as avg_window_duration_seconds
FROM `{project_id}.{domain}_audit.window_summaries`
WHERE DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', audit_timestamp)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY 
    pipeline_mode, 
    pipeline_name,
    processing_date,
    hour_of_day
ORDER BY processing_date DESC, hour_of_day DESC;

-- Window throughput analysis
CREATE OR REPLACE VIEW `{project_id}.{domain}_audit.window_throughput_analysis` AS
SELECT 
    pipeline_name,
    pipeline_mode,
    DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', window_start)) as processing_date,
    EXTRACT(HOUR FROM PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', window_start)) as hour_of_day,
    COUNT(DISTINCT window_start) as unique_windows,
    SUM(record_count) as total_records,
    AVG(record_count) as avg_records_per_window,
    SUM(record_count) / COUNT(DISTINCT window_start) as records_per_window_avg,
    -- Throughput calculations
    SUM(record_count) / (COUNT(DISTINCT window_start) * AVG(TIMESTAMP_DIFF(
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', window_end),
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', window_start),
        SECOND
    ))) as records_per_second,
    -- Processing efficiency
    COUNT(*) / COUNT(DISTINCT window_start) as processing_attempts_per_window,
    -- Data quality in windows
    SAFE_DIVIDE(
        SUM(CASE WHEN record_count > 0 THEN 1 ELSE 0 END),
        COUNT(*)
    ) * 100 as non_empty_window_percentage
FROM `{project_id}.{domain}_audit.window_summaries`
WHERE DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', audit_timestamp)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY 
    pipeline_name,
    pipeline_mode,
    processing_date,
    hour_of_day
ORDER BY processing_date DESC, hour_of_day DESC;

-- Window error analysis
CREATE OR REPLACE VIEW `{project_id}.{domain}_audit.window_error_analysis` AS
WITH error_windows AS (
  SELECT 
    pipeline_name,
    pipeline_mode,
    window_start,
    window_end,
    record_count,
    audit_timestamp,
    'processing_error' as error_type
  FROM `{project_id}.{domain}_audit.window_summaries`
  WHERE record_count = 0 -- No records processed in window
  
  UNION ALL
  
  SELECT 
    pipeline_name,
    'realtime' as pipeline_mode,
    window_start,
    window_end,
    0 as record_count,
    timestamp as audit_timestamp,
    'dependency_error' as error_type
  FROM `{project_id}.{domain}_audit.failed_dependencies`
  WHERE window_start IS NOT NULL
),
daily_stats AS (
  SELECT 
    DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', audit_timestamp)) as error_date,
    pipeline_name,
    pipeline_mode,
    error_type,
    COUNT(*) as error_count,
    COUNT(DISTINCT window_start) as affected_windows
  FROM error_windows
  WHERE DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', audit_timestamp)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY error_date, pipeline_name, pipeline_mode, error_type
)
SELECT 
  error_date,
  pipeline_name,
  pipeline_mode,
  error_type,
  error_count,
  affected_windows,
  LAG(error_count) OVER (PARTITION BY pipeline_name, pipeline_mode, error_type ORDER BY error_date) as prev_day_errors,
  error_count - LAG(error_count) OVER (PARTITION BY pipeline_name, pipeline_mode, error_type ORDER BY error_date) as error_count_change
FROM daily_stats
ORDER BY error_date DESC, pipeline_name, pipeline_mode, error_type;

-- Real-time windowing alerts query
CREATE OR REPLACE VIEW `{project_id}.{domain}_audit.windowing_alerts` AS
WITH recent_windows AS (
  SELECT 
    pipeline_name,
    pipeline_mode,
    window_start,
    window_end,
    record_count,
    audit_timestamp,
    TIMESTAMP_DIFF(
      PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', audit_timestamp),
      PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', window_end),
      SECOND
    ) as latency_seconds
  FROM `{project_id}.{domain}_audit.window_summaries`
  WHERE PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', audit_timestamp) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
),
alert_conditions AS (
  SELECT 
    *,
    CASE 
      WHEN latency_seconds > 300 THEN 'HIGH_LATENCY'
      WHEN record_count = 0 THEN 'EMPTY_WINDOW'
      WHEN latency_seconds > 180 THEN 'MODERATE_LATENCY'
      ELSE 'NORMAL'
    END as alert_level,
    CASE 
      WHEN latency_seconds > 300 THEN CONCAT('Window latency (', latency_seconds, 's) exceeds 5 minutes')
      WHEN record_count = 0 THEN 'Window processed zero records'
      WHEN latency_seconds > 180 THEN CONCAT('Window latency (', latency_seconds, 's) exceeds 3 minutes')
      ELSE 'Normal processing'
    END as alert_message
  FROM recent_windows
)
SELECT 
  pipeline_name,
  pipeline_mode,
  window_start,
  window_end,
  record_count,
  latency_seconds,
  alert_level,
  alert_message,
  audit_timestamp,
  CURRENT_TIMESTAMP() as generated_at
FROM alert_conditions
WHERE alert_level != 'NORMAL'
ORDER BY 
  CASE alert_level
    WHEN 'HIGH_LATENCY' THEN 1
    WHEN 'EMPTY_WINDOW' THEN 2
    WHEN 'MODERATE_LATENCY' THEN 3
  END,
  latency_seconds DESC;
