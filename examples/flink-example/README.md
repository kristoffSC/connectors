# Introduction
This is an example project that shows how to use `delta-flink` connector to read/write data from/to a Delta table using Apache Flink.

# Delta Source
Examples for Delta Flink source are using already created Delta table that can be found under
"src/main/resources/data/source_table_no_partitions" folder.
The detailed description of this table can be found in its [README.md](src/main/resources/data/source_table_no_partitions/README.md)

For Maven and SBT examples, if you wished to use Flink connector SNAPSHOT version,
you need to build it locally and publish to your local repository. You can do it using below code:
```shell
build/sbt standaloneCosmetic/publishM2
build/sbt flink/publishM2
```

### Local IDE
To run Flink example job reading data from Delta table from Local IDE
simply run class that contains `main` method from `org.example.source` package.

  For bounded mode:
  - `org.example.source.bounded.DeltaBoundedSourceExample` class.
  - `org.example.source.bounded.DeltaBoundedSourceUserColumnsExample` class.
  - `org.example.source.bounded.DeltaBoundedSourceVersionAsOfExample` class.

Examples for bound mode will terminate after reading all data from Snapshot. This is expected since those are examples of batch jobs.
The ConsoleSink out in logs can look something like log snipped below, where the order of log lines can be different for every run.
```
org.utils.ConsoleSink [] - Delta table row content: f1 -> [f1_val15], f2 -> [f2_val15], f3 -> [15]
org.utils.ConsoleSink [] - Delta table row content: f1 -> [f1_val6], f2 -> [f2_val6], f3 -> [6]
org.utils.ConsoleSink [] - Delta table row content: f1 -> [f1_val19], f2 -> [f2_val19], f3 -> [19]
```

For rest of the examples for bounded mode, you will see similar logs but with different number of rows (depending on version used for `versionAsOf` option)
or different number of columns depending on used value in builder's `.columnNames(String[])` method.

  For continuous mode:
  - `org.example.source.continuous.DeltaContinuousSourceExample` class.
  - `org.example.source.continuous.DeltaContinuousSourceStartingVersionExample` class.
  - `org.example.source.continuous.DeltaContinuousSourceUserColumnsExample` class.

Examples for continuous mode will not terminate by themselves. In order to stop, you need to terminate the manually using `ctr + c` command.
This is expected since those are examples of streaming jobs that by design run forever.
The ConsoleSink out in logs can look something like log snipped below, where the order of log lines can be different for every run.
```
org.utils.ConsoleSink [] - Delta table row content: f1 -> [f1_val6], f2 -> [f2_val6], f3 -> [6]
org.utils.ConsoleSink [] - Delta table row content: f1 -> [f1_val19], f2 -> [f2_val19], f3 -> [19]
org.utils.ConsoleSink [] - Delta table row content: f1 -> [f1_newVal_0], f2 -> [f2_newVal_0], f3 -> [0]
org.utils.ConsoleSink [] - Delta table row content: f1 -> [f1_newVal_1], f2 -> [f2_newVal_1], f3 -> [1]
```
The example is constructed in a way, after few moments from finishing reading Delta table consent, new records will begin to be added to the table.
The Sink connector will read them as well. New records will have `newVal` for `f1` and `f2` column values.

For rest of the examples for continuous mode, you will see similar logs but with different number of rows (depending on version used for `startingVersion` option)
or different number of columns depending on used value in builder's `.columnNames(String[])` method.

### Maven
To run Flink example job reading data from Delta table from Maven, simply run:
```shell
> cd examples/flink-example/
>
> mvn package exec:java -Dexec.cleanupDaemonThreads=false -Dexec.mainClass=org.example.source.bounded.DeltaBoundedSourceExample -Dstaging.repo.url={maven_repo} -Dconnectors.version={version}
```

In `-Dexec.mainClass` argument you can use any of the full class names from `Local IDE` paragraph.

### SBT
To run Flink example job reading data from Delta table from SBT, simply run:
```shell
> cd examples/
> export STANDALONE_VERSION=x.y.z  # update to desired version
> export EXTRA_MAVEN_REPO={staged_repo}  # include staged repo if desired
>
> build/sbt "flinkExample/runMain org.example.source.bounded.DeltaBoundedSourceExample"
```

Similar to `Maven` paragraph, here you can also use any of the full class names from the `Local IDE` paragraph as `build/sbt "flinkExample/runMain` argument.

# Delta Sink
## Run example for non-partitioned Delta table
To run example in-memory Flink job writing data a non-partitioned Delta table run:

### Local IDE
  Simply run `org.example.sink.DeltaSinkExample` class that contains `main` method

### Maven
```shell
> cd examples/flink-example/
>
> mvn package exec:java -Dexec.cleanupDaemonThreads=false -Dexec.mainClass=org.example.sink.DeltaSinkExample -Dstaging.repo.url={maven_repo} -Dconnectors.version={version}
```

### SBT
```shell
> cd examples/
> export STANDALONE_VERSION=x.y.z  # update to desired version
> export EXTRA_MAVEN_REPO={staged_repo}  # include staged repo if desired
>
> build/sbt "flinkExample/runMain org.example.sink.DeltaSinkExample"
```

## Run example for partitioned Delta table
To run example in-memory Flink job writing data a non-partitioned Delta table run:

### Local IDE
  Simply run `org.example.sink.DeltaSinkPartitionedTableExample` class that contains `main` method

### Maven
```shell
> cd examples/flink-example/
>
> mvn package exec:java -Dexec.cleanupDaemonThreads=false -Dexec.mainClass=org.example.sink.DeltaSinkPartitionedTableExample -Dstaging.repo.url={maven_repo} -Dconnectors.version={version}
```

### SBT
```shell
> cd examples/
> export STANDALONE_VERSION=x.y.z  # update to desired version
> export EXTRA_MAVEN_REPO={staged_repo}  # include staged repo if desired
>
> build/sbt "flinkExample/runMain org.example.sink.DeltaSinkPartitionedTableExample"
```

## Verify
After performing above steps you may observe your command line that will be printing descriptive information
about produced data. Streaming Flink job will run until manual termination and will be producing 1 event
in the interval of 800 millis by default.

To inspect written data look inside `examples/flink-example/src/main/resources/example_table` or
`examples/flink-example/src/main/resources/example_partitioned_table` which will contain created Delta tables along with the written Parquet files.

NOTE: there is no need to manually delete previous data to run the example job again - the example application will do it automatically

# Run an example on a local Flink cluster
## Setup 
1. Setup Flink cluster on your local machine by following the instructions provided [here](https://nightlies.apache.org/flink/flink-docs-release-1.16/try-flink/local_installation.html) (note: link redirects to Flink 1.16 release so be aware to choose your desired release).
2. Go to the `examples` directory in order to package the jar
```shell
> cd examples/flink-example/
> mvn -P cluster clean package -Dstaging.repo.url={maven_repo} -Dconnectors.version={version}
```
After that you should find the packaged fat-jar under path: `<connectors-repo-local-dir>/flink-example/target/flink-example-<version>-jar-with-dependencies.jar`
3. Assuming you've downloaded and extracted Flink binaries from step 1 to the directory `<local-flink-cluster-dir>` run:
```shell
> cd <local-flink-cluster-dir>
> ./bin/start-cluster.sh
> ./bin/flink run -c org.example.sink.DeltaSinkExampleCluster <connectors-repo-local-dir>/flink-example/target/flink-example-<version>-jar-with-dependencies.jar
```
The example above will submit Flink example job for Delta Sink. To submit FLink example job for Delta Source use
`org.example.source.bounded.DeltaBoundedSourceClusterExample` or `org.example.source.continuous.DeltaContinuousSourceClusterExample`.
First will submit a batch job, and second will submit streaming job.

NOTE:<br>
Before running cluster examples for Delta Source, you need to manually copy Delta table data from `src/main/resources/data/source_table_no_partitions`
to `/tmp/delta-flink-example/source_table`.

## Verify
### Delta Sink
Go the http://localhost:8081 on your browser where you should find Flink UI and you will be able to inspect your running job.
You can also look for the written files under `/tmp/delta-flink-example/<UUID>` directory.
![flink job ui](src/main/resources/assets/images/flink-cluster-job.png)

### Delta Source
Go the http://localhost:8081 on your browser where you should find Flink UI and you will be able to inspect your running job.
You can also look at Task Manager logs for `ConsoleSink` output.
![flink job ui](src/main/resources/assets/images/source-pipeline.png)
![flink job logs](src/main/resources/assets/images/source-pipeline-logs.png)

# Running cluster examples using AWS S3 and GCP Object Store.
Below instructions are for running Flink jobs that are meant to be run on a real Flink cluster
and interact with AWS S3 and GCP Object Store services for reading/writing Delta tables.

The Java classes containing Flink Jobs for this example are placed under `org.example.cluster` package.
Instructions here does not cover S3 and GCP Object store configuration. From Flink point of view,
respective buckets have to be created with roles/access keys allowing for uploading new files and reading them back.

We will use environment variable to configure S3 and GCP object store access.
For details please see Flink [S3](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/deployment/filesystems/s3/#configure-access-credentials)
and [GCP](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/deployment/filesystems/gcs/#authentication-to-access-gcs) documentation.

Environment variables for AWS:
- `AWS_REGION` - whatever region since S3 are global and not region specific.
- `AWS_ACCESS_KEY_ID` - contains AWS access key value that has to be extracted from AWS console
- `AWS_SECRET_ACCESS` - contains AWS secret access key value that has to be extracted from AWS console

Environment variables for GCP:
- `GOOGLE_APPLICATION_CREDENTIALS` - path to GCP JSON credential file. The file has to be generated for service account with privileges
  to Object Store bucket used in tests. 

## Setup
1. Setup Flink cluster on your local machine by following the instructions provided [here](https://nightlies.apache.org/flink/flink-docs-release-1.16/try-flink/local_installation.html)
   (note: link redirects to Flink 1.16 release so be aware to choose your desired release).
2. Add S3 hadoop and plugins to Flink cluster following Flink documentation:
   1. flink-s3-fs-hadoop plugin -> [here](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/deployment/filesystems/s3/#hadooppresto-s3-file-systems-plugins)
   2. flink-gs-fs-hadoop-1.16.0.jar plugin -> [here](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/deployment/filesystems/gcs/#gcs-file-system-plugin)
3. create `hdfs-site.xml` file with below content:
  ```xml
   <?xml version="1.0"?>
   <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
      <configuration>
         <property>
            <name>fs.gs.impl</name>
            <value>com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem</value>
         </property>
      </configuration>
  ```
   If you already have hadoop configuration files on your system, add `fs.gs.impl` property to it.
   This is needed for GCP Object Store interactions.
4. Go to the example's directory in order to package the jar
```shell
> cd examples/flink-example/
> mvn clean package -P cluster-aws -f cluster-cloud-pom.xml -Dstaging.repo.url={maven_repo} -Dconnectors.version={version}
```
After that you should find the packaged fat-jar under path: `<connectors-repo-local-dir>/flink-example/target/flink-cluster-cloud-example-<version>-jar-with-dependencies.jar`
5. Set environment variables. Please note that below example sets environment variables per bash session.
   Those must be set for terminal console from where Flink cluster will be started and from Flink job will be submitted.

   Environment variables to set:
   ```shell
   export AWS_REGION=us-east-1 (you can set any you whish)
   export AWS_ACCESS_KEY_ID=<your-key>
   export AWS_SECRET_ACCESS_KEY=<your-secret-key>
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/gcp-secret.json
   export HADOOP_CONF_DIR=/path/to/folder/with/hadoop-configuration (this folder should contain file with property added in step #3)
    ```
7. Assuming you've downloaded and extracted Flink binaries from step 1 to the directory `<local-flink-cluster-dir>` run:
```shell
> cd <local-flink-cluster-dir>
> ./bin/start-cluster.sh
> ./bin/flink run -c org.example.cluster.<flink-job-class> <connectors-repo-local-dir>/flink-example/target/flink-cluster-cloud-example-<version>-jar-with-dependencies.jar -tablePath <path-to-cloud-storage>
```
The example above will submit Flink example job.

## Test jobs
Test SQL jobs using S3/Object Store are located under `org.example.cluster` package.
The jos are:
- `SinkBatchSqlClusterJob` - writes Delta table in batch mode.
- `SourceBatchSqlClusterJob` - reads Delta table in batch mode and print its content in logs.
- `SourceBatchSqlCountJob` - reds all records from Delta table and count them.
- `StreamingApiSourceToTableDeltaSinkJob` - writes Delta table in streaming mode. Job will write 5 records for every Flink checkpoint, lasting 25 checkpoints in total. The result table should have 125 records.

Additional job:
- `SourceBatchClusterJob` Streaming API batch job that will read records from Delta table.
- `SinkBatchClusterJob` Streaming API job that will write 100 rows into Delta table.

## Test job verification
Details about test job verification are presented [here](doc/TEST_JOB_VERIFICATION.md)

# Cleaning up after running jobs on real cluster
1. You cancel your job from the UI after you've verified your test. 
2. To shut down the cluster go back to the command line and run 
```shell
> ./bin/stop-cluster.sh
```
