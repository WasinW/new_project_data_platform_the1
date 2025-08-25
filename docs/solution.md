**สถาปัตยกรรม (สรุป)**

* **Zones**: RAW/STAGING (External – BigLake), REFINED/ANALYTICS (Native BQ) ตาม **List\_service.md**&#x20;
* **Orchestration**: Cloud Composer (Airflow) เรียก STS/Dataflow/BigQuery/Dataplex/Secret Manager
* **Compute**: Dataflow (Realtime/Batch), STS (S3→GCS)
* **Governance**: Dataplex + Data Lineage API
* **Secrets**: GCP Secret Manager (AWS S3, Redshift, BQ cross‑project) ตาม **Pipeline Details**&#x20;

**Pipeline ตามประเภท**

1. **Initiate**

   * Airflow:

     1. Get secrets (AWS S3 / Redshift / BQ)
     2. Run STS (job ที่สร้างไว้ล่วงหน้า) copy S3→GCS
     3. สร้าง external temp/recreate
     4. Insert/Load เข้า target table
     5. Track lineage + Audit log
   * ใช้สิทธิ์: Orchestrator SA + STS admin + BQ data editor + Storage object admin
   * อ้างอิง STS CLI/Perms: ([Google Cloud][3])

2. **Realtime (Pub/Sub → Dataflow → RAW/REFINED/ANALYTICS)**

   * Airflow trigger Dataflow พร้อม `--params`
   * Dataflow: windowing consume topics `create/update`; ดึง secrets; เขียน RAW (GCS/Parquet) & REFINED/ANALYTICS (BQ) + lineage + audit; ตรวจ dependency ตาม config
   * SA ที่ใช้รันงาน: `sa-dataflow-runner` มี roles: dataflow\.worker, bq editor, storage object admin, pubsub sub/publish, secret accessor ([Google Cloud][7])

3. **Batch (short‑term)**

   * เหมือน realtime แต่ไม่มี window/notification; ดึงตรงจาก source (BQ ข้ามโปรเจกต์) ทุกชั่วโมง; ใช้ param แยกโหมดใน Dataflow
   * Audit+Lineage เช่นกัน&#x20;

4. **Reconciled**

   * Airflow: STS สร้าง snapshot ลง RAW, สร้าง external temp, compare กับ native table (query/spark/sql), บันทึกผล audit (diff/metrics) และ lineage
   * มี 2 STS jobs/ตาราง: **initiate** และ **reconciled** ตามรายละเอียดคุณ&#x20;

**Security/IAM**

* เปิด API ตามข้อ 1.1, จัด SA ตาม Simple/Better
* Composer env ผูก SA พร้อม `composer.worker` และเพิ่ม roles ที่ DAG ต้องใช้เท่านั้น ([Google Cloud][4])
* BigLake ใช้ Connection SA + GCS objectViewer เฉพาะ path ที่จำเป็น ([Google Cloud][11])

**Data Modeling & DDL**

* RAW/STAGING = BigLake external (Parquet)
* STRUCTURE = Views บน RAW
* REFINED/ANALYTICS = Native tables + partition/cluster + MERGE

**Operations**

* Deploy 4 ขั้นตอนด้วยสคริปต์ในข้อ 2
* Airflow variables: เก็บ config (แหล่งที่มา, target tables, dependency graph, batch interval ฯลฯ)
* Monitoring: Airflow task states, Dataflow job logs, STS job logs, custom metrics, alerting
