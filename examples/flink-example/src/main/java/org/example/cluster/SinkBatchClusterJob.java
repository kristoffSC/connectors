package org.example.cluster;

import io.delta.flink.sink.DeltaSink;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.example.cluster.utils.JobClusterUtils;

/**
 * Streaming API job that writes 100 records into the Delta table.
 */
public class SinkBatchClusterJob {

    // Program arg be like:
    // -tablePath delta/table; -next val
    public static void main(String[] args) throws Exception {

        // Extract target table path from job parameters.
        // The random string will be added to the end of the path.
        ParameterTool parameter = ParameterTool.fromArgs(args);
        String tablePath = parameter.get("tablePath") + JobClusterUtils.randomString();
        System.out.println("Table Path - " + tablePath);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        addPipeline(env, tablePath);

        env.execute("Flink-Delta Batch Demo");
    }

    public static void addPipeline(StreamExecutionEnvironment env, String tablePath) {

        Configuration configuration = new Configuration();
        // for GCP object store
        configuration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");

        DeltaSink<RowData> deltaSink =
            JobClusterUtils.createDeltaSink(
                tablePath,
                JobClusterUtils.TEST_ROW_TYPE,
                configuration
            );

        env
            .fromCollection(JobClusterUtils.getTestRowData(100))
            .setParallelism(1)
            .sinkTo(deltaSink)
            .setParallelism(3);
    }

}
