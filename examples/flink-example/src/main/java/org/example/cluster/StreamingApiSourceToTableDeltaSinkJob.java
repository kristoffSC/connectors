package org.example.cluster;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.example.cluster.utils.CheckpointCountingSource;
import org.example.cluster.utils.JobClusterUtils;

/**
 * A Flink streaming job that combines Streaming API and Table/SQL API.
 * This job will produce 125 records using CheckpointCountingSource and write them into Delta table.
 *
 * The CheckpointCountingSource is registered as Flink table using Table API.
 */
public class StreamingApiSourceToTableDeltaSinkJob {

    // Program arg be like:
    // -tablePath delta/table; -next val
    public static void main(String[] args) {

        // Extract target table path from job parameters.
        // The random string will be added to the end of the path.
        ParameterTool parameter = ParameterTool.fromArgs(args);
        String tablePath = parameter.get("tablePath") + JobClusterUtils.randomString();
        System.out.println("Table Path - " + tablePath);

        StreamExecutionEnvironment streamEnv = createTestStreamEnv(true);
        StreamTableEnvironment tableEnv = createTableStreamingEnv(streamEnv);
        addPipeline(streamEnv, tableEnv, tablePath);
    }

    private static void addPipeline(
        StreamExecutionEnvironment streamEnv,
        StreamTableEnvironment tableEnv,
        String sinkTablePath) {

        DataStreamSource<RowData> sourceStream =
            streamEnv.addSource(new CheckpointCountingSource(5, 25));

        // setup Delta Catalog
        tableEnv.executeSql("CREATE CATALOG myDeltaCatalog WITH ('type' = 'delta-catalog')");
        tableEnv.executeSql("USE CATALOG myDeltaCatalog");

        Table sourceTable = tableEnv.fromDataStream(sourceStream);
        tableEnv.createTemporaryView("InputTable", sourceTable);

        tableEnv.executeSql(String.format(""
                + "CREATE TABLE sinkTable ("
                + "f1 STRING,"
                + "f2 STRING,"
                + "f3 INT"
                + ") WITH ("
                + " 'connector' = 'delta',"
                + " 'table-path' = '%s'"
                + ")",
            sinkTablePath)
        );

        tableEnv.executeSql("INSERT INTO sinkTable SELECT * FROM InputTable");
    }

    private static StreamExecutionEnvironment createTestStreamEnv(boolean isStreaming) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());

        if (isStreaming) {
            env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
            env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        } else {
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        }

        return env;
    }

    private static StreamTableEnvironment createTableStreamingEnv(StreamExecutionEnvironment env) {
        return StreamTableEnvironment.create(env);
    }
}
