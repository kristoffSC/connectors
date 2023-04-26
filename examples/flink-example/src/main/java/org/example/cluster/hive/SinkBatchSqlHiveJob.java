package org.example.cluster.hive;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.cluster.utils.JobClusterUtils;

public class SinkBatchSqlHiveJob {

    // Program arg be like:
    // -tablePath delta/table; -sinkTableName tableName; -hiveConfDir /path/to/dir/
    public static void main(String[] args) throws Exception {

        ParameterTool parameter = ParameterTool.fromArgs(args);
        String tablePath = parameter.get("tablePath") + JobClusterUtils.randomString();
        String sinkTableName = parameter.get("sinkTableName");
        String hiveConfDir = parameter.get("hiveConfDir");

        System.out.println("Table Path - " + tablePath);
        System.out.println("Sink table name - " + sinkTableName);
        System.out.println("Hive conf dir path - " + hiveConfDir);

        StreamExecutionEnvironment streamEnv = createTestStreamEnv(false);
        StreamTableEnvironment tableEnv = createTableStreamingEnv(streamEnv);
        addPipeline(tableEnv, tablePath, sinkTableName, hiveConfDir);
    }

    private static void addPipeline(
            StreamTableEnvironment tableEnv,
            String tablePath,
            String sinkTableName,
            String hiveConfDir) {

        String catalogConfig = String.format(", "
                + "'catalog-type' = 'hive',"
                + "'hadoop-conf-dir' = '%s'",
            hiveConfDir);

        // setup Delta Catalog
        tableEnv.executeSql(
            String.format(
                "CREATE CATALOG myDeltaCatalog WITH ('type' = 'delta-catalog'%s)",
                catalogConfig)
        );
        tableEnv.executeSql("USE CATALOG myDeltaCatalog");

        // SQL definition for Delta Table where we will insert rows.
        tableEnv.executeSql(String.format(""
                + "CREATE TABLE IF NOT EXISTS %s ("
                + "f1 STRING,"
                + "f2 STRING,"
                + "f3 INT"
                + ") WITH ("
                + " 'connector' = 'delta',"
                + " 'table-path' = '%s'"
                + ")",
            sinkTableName,
            tablePath)
        );

        // A SQL query that inserts three rows (three columns per row) into sinkTable.
        tableEnv.executeSql(String.format(
            "INSERT INTO %s VALUES "
            + "('a', 'b', 1),"
            + "('c', 'd', 2),"
            + "('e', 'f', 3)",
            sinkTableName)
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
