package org.example.cluster.hive;

import java.util.Arrays;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.utils.ConsoleSink;
import org.utils.job.sql.RowMapperFunction;

public class SourceBatchSqlHiveJob {

    public static final RowType FULL_SCHEMA_ROW_TYPE = new RowType(Arrays.asList(
        new RowType.RowField("f1", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("f2", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("f3", new IntType())
    ));

    // Program arg be like:
    // -sourceTableName aTable -hiveConfDir /path/to/hive/conf/dir/
    public static void main(String[] args) throws Exception {

        ParameterTool parameter = ParameterTool.fromArgs(args);
        String sourceTableName = parameter.get("sourceTableName");
        String hiveConfDirPath = parameter.get("hiveConfDir");

        System.out.println("Source table name - " + sourceTableName);
        System.out.println("Hive conf dir path - " + hiveConfDirPath);

        StreamExecutionEnvironment streamEnv = createTestStreamEnv(false);
        StreamTableEnvironment tableEnv = createTableStreamingEnv(streamEnv);
        Table table = addPipeline(tableEnv, sourceTableName, hiveConfDirPath);
        tableEnv.toDataStream(table)
            .map(new RowMapperFunction(FULL_SCHEMA_ROW_TYPE))
            .addSink(new ConsoleSink(FULL_SCHEMA_ROW_TYPE))
            .name("Processing pipeline Console Sink")
            .setParallelism(1);

        streamEnv.execute("Flink-Delta SQL Source Batch Demo");
    }

    private static Table addPipeline(
            StreamTableEnvironment tableEnv,
            String sourceTableName,
            String hiveConfDirPath) {

        String catalogConfig = String.format(", "
                + "'catalog-type' = 'hive',"
                + "'hadoop-conf-dir' = '%s'",
            hiveConfDirPath);

        // setup Delta Catalog
        tableEnv.executeSql(
            String.format(
                "CREATE CATALOG myDeltaCatalog WITH ('type' = 'delta-catalog'%s)",
                catalogConfig)
        );
        tableEnv.executeSql("USE CATALOG myDeltaCatalog");

        // It is assumed that Catalog will contain entry for "sourceTableName".
        // If not run SinkBatchSqlHiveJob first.
        return tableEnv.sqlQuery(String.format("SELECT * FROM %s", sourceTableName));
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
