package org.example.cluster;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.cluster.utils.JobClusterUtils;

/**
 * SQL job that will write 3 records into the Delta table.
 */
public class SinkBatchSqlClusterJob {

    // Program arg be like:
    // -tablePath delta/table; -next val
    public static void main(String[] args) throws Exception {

        // Extract target table path from job parameters.
        // The random string will be added to the end of the path.
        ParameterTool parameter = ParameterTool.fromArgs(args);
        String tablePath = parameter.get("tablePath") + JobClusterUtils.randomString();
        System.out.println("Table Path - " + tablePath);

        StreamExecutionEnvironment streamEnv = createTestStreamEnv(false);
        StreamTableEnvironment tableEnv = createTableStreamingEnv(streamEnv);
        addPipeline(tableEnv, tablePath);
    }

    private static void addPipeline(StreamTableEnvironment tableEnv, String tablePath) {
        // setup Delta Catalog
        tableEnv.executeSql("CREATE CATALOG myDeltaCatalog WITH ('type' = 'delta-catalog')");
        tableEnv.executeSql("USE CATALOG myDeltaCatalog");

        // SQL definition for Delta Table where we will insert rows.
        tableEnv.executeSql(String.format(""
                + "CREATE TABLE sinkTable ("
                + "f1 STRING,"
                + "f2 STRING,"
                + "f3 INT"
                + ") WITH ("
                + " 'connector' = 'delta',"
                + " 'table-path' = '%s'"
                + ")",
            tablePath)
        );

        // A SQL query that inserts three rows (three columns per row) into sinkTable.
        tableEnv.executeSql(""
            + "INSERT INTO sinkTable VALUES "
            + "('a', 'b', 1),"
            + "('c', 'd', 2),"
            + "('e', 'f', 3)"
        );
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
