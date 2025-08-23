Pipeline Details

get secret จะถูกใช้ใน 
    1. pipeline initiate : get secrete aws s3 , aws  redshift , gcp bq (target table) 
    2. pipeline realtime: get secrete  gcp gcs  (target ), gcp bq (source table) ,gcp bq(target table) 
    3. pipeline reconciled : get secrete  aws s3 , aws  redshift , gcp bq (target table) 
    4. pipeline batch : get secrete  aws s3 , aws  redshift , gcp bq (source table) ,gcp bq(target table) 
// เนื่องจาก source จะอยู่ที่ bq คนล่ะโปรเจ็ค เลยต้องมีแยกออกมา

STS ใช้ใน 
    1. pipeline initiate : คือ ต้อง copy s3 ลงมา gcs เพื่อเป็นตัวเริ่มต้น 
    2. pipeline reconciled : ต้องดึงลงมาเหมือนกันเพื่อจะเทียบกับ new pipeline on going 

แสดงว่าต้องสร้าง transfer job ใน Storage transfer service มา 2 ตัวต่อ table ไว้สำหรับ Initiate pipeline ตัวนึง และ reconciled pipeline ตัวนึง 

และ service orchestrate กับ compute นี้มี 
    1. pipeline initiate : composer airflow only ( 
            use get secrete from secret manager (ทั้ง key สำหรับ s3 (STS), redshift , bq table)
            > trigger STS (ที่สร้าง transfer job ไว้แล้ว ) copy data from s3 to gcs  
            > execute bq recreate table temp 
            > execute bq insert to target table
            > audit data lineage 
            > audit log 
        ) ใช้ service เหล่านี้บน airflow เลย 
    2. pipeline  realtime  : composer airflow + dataflow ( 
            composer airflow แค่ trigger ให้ dataflow on run เฉยๆ การทำงานส่วนใหญ่เกิด ใน dataflow
                start dataflow  
                > get config job 
                > open windowing for
                    > consume notification from pusub topic create and update
                    > and closed follower by config windowing in job config

                > open windowing for get data source 
                    > if exists notification in pusub topic 
                        > get secret key from secret manager for get data from source.
                        > and get secret key from secret manager for write data to target (key gcs for table raw , key bq for refined/analytics).
                        > get data from source table in bigtable  
                        > and write data from bigtable to gcs (for raw zone only because table raw zone is external table ) and refined and analytics write in bq (because refined and analytics table are native table)
                        > tracking write lineage on dataplex 
                        > audit log
                    > and closed follower by config windowing in job config
                
                > open windowing for check dependency // for case dependency สำหรับ zone refined/analytics ที่ต้อง join กับ table อื่นที่ cross domain กันมากกว่า  
                    // หน้าตา config จะประมาณนี้ 
                    config dependency : [
                        {
                            "table_nm": "member_address",
                            "depend": [{"table_nm":"segment_master","check_depend":"custom_module_check_depend_segment_master"}]
                        }
                        ]
                    ไรงี้
                        
                    > if exists dependency config
                        > check dependency from custom check dependency module // เราต้องทำ module แยกมาเอง แล้วมาใส่ในนี้ 
                        > if pass next step if not pass ignore // เว้นไว้ก่อนยังไม่ confirm by pass ไป 
                    > if not have dependency pass to next step  
                    > and closed follower by config windowing in job config
                
                > open windowing for get transformation 
                    // ถ้ามี config transformation logic ก็ทำงาน ถ้าไม่มีก็ไม่ต้องทำอะไร  
                    // ถ้ามีหลาย transformation config ก็ทำงาน ตาม config เลยว่า level ไหนทำก่อนหล้ง 
                        เช่น ถ้า config sequence_table : [
                            {
                                "table_nm": "ms_member",
                                "depend": ["member_profile","member_address"]
                            },
                            {
                                "table_nm": "member_address",
                                "depend": ["s_loy_tier","s_loy_mem_tier","s_contact"]
                            }
                        ]
                        ไรงี้ // sequence_table คนล่ะอันกับ dependency นะ อันนั้นมันสำหรับต้องรอ job ก่อนหน้า เช่น 
                        config sequence_table แบบมี dependency : [
                            {
                                "table_nm": "ms_member",
                                "depend": ["member_profile","member_address"]
                            },
                            {
                                "table_nm": "member_address",
                                "depend": ["s_loy_tier","s_loy_mem_tier","s_contact",
                                    ## dependency สำหรับ table ที่ต้องรอ job ก่อนหน้า
                                    "segment_master"
                                ]
                            }
                        ]

                    > if exists transformation config  
                        > get secret key from secret manager for write data to target (key gcs for table raw , key bq for refined/analytics).
                        > sent message to transformation module // เราต้องทำ module แยกมาเอง แล้วมาใส่ในนี้  
                        > and write message to refined or analytics write in bq 
                        > tracking write lineage on dataplex 
                        > audit log
                    > and closed follower by config windowing in job config

                > open windowing for get transformation // ถ้ามี config transformation logic ก็ทำงาน ถ้าไม่มีก็ไม่ต้องทำอะไร  
                    > if exists transformation config  
                        > sent message to transformation module // เราต้องทำ module แยกมาเอง แล้วมาใส่ในนี้  
                        > and write message to refined or analytics write in bq 
                    > and closed follower by config windowing in job config


            ) 
    3. pipeline batch : composer airflow + dataflow batch (เป็น dataflow เดียวกันกับ realtime หรือคล้ายกันก็ได้ แต่จะแยกจาก param ที่ส่งไปว่าเป็น batch นะ ไรงี้ )
        // จะเหมือนกับ realtime เลยเพราะ batch อันนี้เป็น short term ไว้สำหรับ switch เมื่อ notification ที่ต้นทาง provide มาให้เสร็จแล้ว แต่ตอนนี้ยัง 
        เลยมีปรับนิดหน่อย คือ ไม่มี step consume notification แต่จะเป็น batch hourly ไป get data ตรงๆจาก bq เลย 
        และ ไม่ต้องมี windowing เพราะไม่ใช่ realtime 
        > composer airflow แค่ trigger ให้ dataflow on run เฉยๆ การทำงานส่วนใหญ่เกิด ใน dataflow
            start dataflow  
            > get config job 
            > get secret key from secret manager for get data from source.
            > and get secret key from secret manager for write data to target (key gcs for table raw , key bq for refined/analytics).
            > get data from source table in bigtable  
            > and write data from bigtable to gcs (for raw zone only because table raw zone is external table ) and refined and analytics write in bq (because refined and analytics table are native table)
            > tracking write lineage on dataplex 
            > audit log
            
            > check dependency // for case dependency สำหรับ zone refined/analytics ที่ต้อง join กับ table อื่นที่ cross domain กันมากกว่า  
                // หน้าตา config จะประมาณนี้ 
                config dependency : [
                    {
                        "table_nm": "member_address",
                        "depend": [{"table_nm":"segment_master","check_depend":"custom_module_check_depend_segment_master"}]
                    }
                    ]
                ไรงี้
                
            > if exists dependency config
                > check dependency from custom check dependency module // เราต้องทำ module แยกมาเอง แล้วมาใส่ในนี้ 
                > if pass next step if not pass ignore // เว้นไว้ก่อนยังไม่ confirm by pass ไป 
            > if not have dependency pass to next step  
            > ... 
        ประมาณนี้             
    4. pipeline reconciled : composer airflow +  dataflow 
            use get secrete from secret manager (ทั้ง key สำหรับ s3 (STS), redshift , bq table)
            > trigger STS (ที่สร้าง transfer job ไว้แล้ว ) copy data from s3 to gcs
            > open service using sql like bq , dataproc , dataflow (maybe dataflow)   
                > recreate external table temp for reconciled 
                > compare data external table temp and real native table 
                // if using dataproc/dataflow use spark sql 
                > audit reconciled
            > audit log 
        