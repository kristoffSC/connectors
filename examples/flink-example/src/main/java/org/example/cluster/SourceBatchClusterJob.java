package org.example.cluster;

import io.delta.flink.source.DeltaSource;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.example.cluster.utils.JobClusterUtils;
import org.utils.ConsoleSink;

/**
 * Streaming API Batch job that read all records from Delta table and print them in logs/console.
 */
public class SourceBatchClusterJob {

    // Program arg be like:
    // -tablePath delta/table; -next val
    public static void main(String[] args) throws Exception {

        // Extract target table path from job parameters.
        ParameterTool parameter = ParameterTool.fromArgs(args);
        String tablePath = parameter.get("tablePath");
        System.out.println("Table Path - " + tablePath);

        // set up the batch execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        addPipeline(env, tablePath);

        env.execute("Flink-Delta Batch Streaming Demo");
    }

    public static void addPipeline(StreamExecutionEnvironment env, String tablePath) {
        Configuration conf = new Configuration();

        // for GCP object store
        conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");

        DeltaSource<RowData> deltaSource = JobClusterUtils
            .createBoundedDeltaSource(tablePath, conf);

        env
            .fromSource(deltaSource, WatermarkStrategy.noWatermarks(),
                "Processing pipeline Delta Source")
            .setParallelism(1)
            .addSink(new ConsoleSink(JobClusterUtils.TEST_ROW_TYPE))
            .name("Processing pipeline Console Sink")
            .setParallelism(1);
    }

}
