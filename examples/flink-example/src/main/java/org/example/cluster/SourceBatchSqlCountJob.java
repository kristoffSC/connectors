package org.example.cluster;

import java.util.Collections;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.example.cluster.utils.JobClusterUtils;
import org.utils.ConsoleSink;
import org.utils.job.sql.RowMapperFunction;

public class SourceBatchSqlCountJob {

    public static final RowType COUNT_ROW_SCHEMA = new RowType(Collections.singletonList(
        new RowField("f1", new BigIntType())
    ));

    // Program arg be like:
    // -tablePath delta/table; -next val
    public static void main(String[] args) throws Exception {

        // Extract target table path from job parameters.
        ParameterTool parameter = ParameterTool.fromArgs(args);
        String tablePath = parameter.get("tablePath") + JobClusterUtils.randomString();
        System.out.println("Table Path - " + tablePath);

        StreamExecutionEnvironment streamEnv = createTestStreamEnv(false);
        StreamTableEnvironment tableEnv = createTableStreamingEnv(streamEnv);
        Table table = addPipeline(tableEnv, tablePath);
        tableEnv.toDataStream(table)
            .map(new RowMapperFunction(COUNT_ROW_SCHEMA))
            .addSink(new ConsoleSink(COUNT_ROW_SCHEMA))
            .name("Processing pipeline Console Sink")
            .setParallelism(1);

        streamEnv.execute("Flink-Delta SQL Source Batch Demo");
    }

    private static Table addPipeline(StreamTableEnvironment tableEnv, String tablePath) {
        // setup Delta Catalog
        tableEnv.executeSql("CREATE CATALOG myDeltaCatalog WITH ('type' = 'delta-catalog')");
        tableEnv.executeSql("USE CATALOG myDeltaCatalog");

        // SQL definition for Delta Table where we will insert rows.
        tableEnv.executeSql(String.format(""
                + "CREATE TABLE sourceTable ("
                + "f1 STRING,"
                + "f2 STRING,"
                + "f3 INT"
                + ") WITH ("
                + " 'connector' = 'delta',"
                + " 'table-path' = '%s'"
                + ")",
            tablePath)
        );

        return tableEnv.sqlQuery("SELECT COUNT(*) FROM sourceTable");
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
