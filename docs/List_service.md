List_service
    - iam
    - service account
        - internal_sa : composer + dataflow + bq + gcs + dataplex  (orchestrate + compute + database/storage + lineage  )  (รวมทุก service ใน account เดียวไปเลยง่ายกว่า )
        - external_sa : s3 (aws source) + refshift (aws source)

    - s3 (source aws) 
    - gcs 
        - framwork 
            - init 
            - airflow 
                - config
                - dags
            - config 
                - <config_job>
            - dataflow
            - monitoring
        - audit log 
            - audit_job_log 
            - audit_data_quality_log 
            - audit_reconciled_log 
        - staging (external table) (ใช้สำหรับ temp table ที่จะ initiate มา กับ temp table ที่จะมา reconciled เท่านั้น)
        - raw (external table) (มีแค่ table raw เท่านั้นที่ ที่เป็น external table )
    - bq 
        - dataset framework 
            - audit job log
            - audit data quality log
            - audit reconciled log
        - dataset staging ( external table store as parquet + dataplex + iceberg )
        - dataset raw ( external table store as parquet + dataplex + iceberg )
        - dataset structure ( view of raw on bq)
        - dataset refined (native table on bq + dataplex )
        - dataset analytics (native table on bq + dataplex )

    - dataplex
        - staging : asset gcs + asset bq
        - raw : asset gcs + asset bq 
        - refined : asset bq
        - analytics : asset bq

    - pubsub
        - sub 
        - topic 
            - create 
            - update 

    - secrete manager
        - secrete external_sa
        - secrete internal_sa (ควร create แยก service account เลยมั้ยหรือ create รวมเลย)

    - Storage Transfer Service:
        - job Transfer :
            - table_transfer (<data_zone>_<table_name>_<source>_<target>) เช่น analytics_ms_member_s3_gcs , refined_member_address_s3_gc

    - composer
        - pipeline initiate
        - pipeline realtime
        - pipeline batch (short term for realtime pipeline )
        - pipeline reconciled



