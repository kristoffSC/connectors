# Verification of cluster - cloud jobs

Verification process uses Flink UI - available under http://localhost:8081
and  Flink Task Manager logs. 

Logs can be found from Flink UI, for example:
![flink job logs](../src/main/resources/assets/images/source-pipeline-logs.png)

or read directly from file located under `<local-flink-cluster-dir>/log` folder.

Locate the newest files based on last modification date.
The `xxx-standalonesession-xxx.log` file contains logs from Job Manager.
The `xxx-taskexecutor-xxx.log` file contains logs from Task Manager.

After submitting job to the cluster, navigate to Flink UI. Depending on the moment when you will navigate
to Flink UI, the job can be found under `Jobs -> Running Jobs` or `Jobs -> Completed jobs`

![flink-ui](UI.PNG)

## SinkBatchSqlClusterJob
This job writes three records into Delta Table:

Job status - FINISHED (can be RUNNING for some time)
### S3
Job Status
![sinkBatchSQL_S3](S3/S3Sink_Batch/SinkBatchSQL_S3.PNG)

Files created on S3 bucket
![sinkBatchSQL_S3_bucket](S3/S3Sink_Batch/SinkBatchSQL_S3_bucket.PNG)

### GCP
Job Status
![sinkBatchSQL_GCP](GCP/SinkBatchSqlClusterJob_job.PNG)

Files created in bucket:
![sinkBatchSQL_GCP_bucket_1](GCP/SinkBatchSqlClusterJob_bucket_1.PNG)
![sinkBatchSQL_GCP_bucket_2](GCP/SinkBatchSqlClusterJob_bucket_2.PNG)

## SourceBatchSqlClusterJob
This will run a SQL batch job that read all records from Delta table and print them in logs/console.

### S3
Job Status
![sourceBatchSQL_S3](S3/S3Source_Batch.PNG)

Records printed in logs:
![sourceBatchSQL_S3_log](S3/S3Source_Batch_Log.PNG)

### GCP
Job Status
![sourceBatchSQL_GCP](GCP/SourceBatchSqlClusterJob_job.PNG)

Logs:
![sourceBatchSQL_GCP_logs](GCP/SourceBatchSqlClusterJob_logs.PNG)

## StreamingApiSourceToTableDeltaSinkJob
### S3
Job Status
![streamingToTable_S3](S3/S3_StreamingApi_ToTableDeltaApi/StreamingSource_ToTableApiDeltaSink_S3.PNG)

Bucket:
![streamingToTable_s3bucket1](S3/S3_StreamingApi_ToTableDeltaApi/StreamingSource_ToTableApiDeltaSink_S3_Bucket_1.PNG)
![streamingToTable_s3bucket2](S3/S3_StreamingApi_ToTableDeltaApi/StreamingSource_ToTableApiDeltaSink_S3_Bucket_2.PNG)

### GCP
Job Status
![streamingToTable_gcp](GCP/StreamingApi_ToTableDeltaApi/StreamingSource_ToTableApiDeltaSink_gcp.PNG)

Bucket:
![streamingToTable_gcpbucket1](GCP/StreamingApi_ToTableDeltaApi/StreamingSource_ToTableApiDeltaSink_gcp_objectStore_1.PNG)
![streamingToTable_gcpbucket2](GCP/StreamingApi_ToTableDeltaApi/StreamingSource_ToTableApiDeltaSink_gcp_objectStore_2.PNG)

## SourceBatchSqlCountJob
This job read and count records from Delta table created by `StreamingApiSourceToTableDeltaSinkJob`
SQL Job:
![countJob_SQL](S3/S3_StreamingApi_ToTableDeltaApi/verifiation/SelectCountJob.PNG)

### S3
Job Status
![countJob_S3](S3/S3_StreamingApi_ToTableDeltaApi/verifiation/SelectDelta_WithCountSQL.PNG)

Logs:
![count_logs_aws](S3/S3_StreamingApi_ToTableDeltaApi/verifiation/SelectCountJob_ConsoleSinkResult.PNG)

### GCP
Job Status
![countJob_gcp](GCP/StreamingApi_ToTableDeltaApi/Verification/RecordCountJob.PNG)

Logs:
![count_logs_gcp](GCP/StreamingApi_ToTableDeltaApi/Verification/RecordCountJob_log.PNG)
