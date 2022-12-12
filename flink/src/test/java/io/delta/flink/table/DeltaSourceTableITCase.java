/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.flink.table;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.delta.flink.utils.DeltaTestUtils;
import io.delta.flink.utils.ExecutionITCaseTestConstants;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.delta.flink.utils.DeltaTestUtils.buildCluster;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class DeltaSourceTableITCase {

    private static final int PARALLELISM = 2;

    private static final Logger LOG = LoggerFactory.getLogger(DeltaSourceTableITCase.class);

    private static final String TEST_SOURCE_TABLE_NAME = "sourceTable";

    private static final String TEST_SINK_TABLE_NAME = "sinkTable";

    private static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    /**
     * Schema for this table has only {@link ExecutionITCaseTestConstants#DATA_COLUMN_NAMES}
     * of type {@link ExecutionITCaseTestConstants#DATA_COLUMN_TYPES} columns.
     */
    private String nonPartitionedTablePath;

    /**
     * Schema for this table contains data columns
     * {@link ExecutionITCaseTestConstants#DATA_COLUMN_NAMES} and col1, col2
     * partition columns. Types of data columns are
     * {@link ExecutionITCaseTestConstants#DATA_COLUMN_TYPES}
     */
    private String partitionedTablePath;

    /**
     * Schema for this table has only
     * {@link ExecutionITCaseTestConstants#LARGE_TABLE_ALL_COLUMN_NAMES} of type
     * {@link ExecutionITCaseTestConstants#LARGE_TABLE_ALL_COLUMN_TYPES} columns.
     * Column types are long, long, String
     */
    private String nonPartitionedLargeTablePath;

    public static final RowType TEST_ROW_TYPE = new RowType(Arrays.asList(
        new RowType.RowField("col1", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("col2", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("col3", new IntType())
    ));

    private static ExecutorService testWorkers;

    private final MiniClusterWithClientResource miniClusterResource = buildCluster(PARALLELISM);

    protected RowType testRowType;

    @BeforeAll
    public static void beforeAll() throws IOException {
        testWorkers = Executors.newCachedThreadPool(r -> {
            final Thread thread = new Thread(r);
            thread.setUncaughtExceptionHandler((t, e) -> {
                t.interrupt();
                throw new RuntimeException(e);
            });
            return thread;
        });
        TEMPORARY_FOLDER.create();
    }

    @AfterAll
    public static void afterAll() {
        testWorkers.shutdownNow();
        TEMPORARY_FOLDER.delete();
    }

    @BeforeEach
    public void setUp() {
        try {
            miniClusterResource.before();

            nonPartitionedTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
            nonPartitionedLargeTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
            partitionedTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();

            DeltaTestUtils.initTestForPartitionedTable(partitionedTablePath);
            DeltaTestUtils.initTestForNonPartitionedTable(nonPartitionedTablePath);
            DeltaTestUtils.initTestForNonPartitionedLargeTable(
                nonPartitionedLargeTablePath);
        } catch (Exception e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }
    }

    @AfterEach
    public void afterEach() {
        miniClusterResource.after();
    }

    // TODO FLINK_SQL_PR2 Write test for streaming
    // TODO FLINK_SQL_PR2 Write test for static partition
    // TODO FLINK_SQL_PR2 with partitions from Delta Table
    @ParameterizedTest
    @ValueSource(strings = {"", "batch"})
    public void testBatchTableJob(String jobMode) throws Exception {

        String sinkTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
            getTestStreamEnv(("streaming".equalsIgnoreCase(jobMode)))
        );

        tableEnv.executeSql(
            buildSourceTableSql(
                nonPartitionedTablePath,
                false,
                false)
        );

        tableEnv.executeSql(buildSinkTableSql(sinkTablePath));

        String connectorModeHint = StringUtils.isNullOrWhitespaceOnly(jobMode) ?
            "" : String.format("/*+ OPTIONS('mode' = '%s') */", jobMode);

        String insertSql = String.format(
            "INSERT INTO sinkTable SELECT * FROM sourceTable %s",
            connectorModeHint);
        tableEnv.executeSql(insertSql).await(30, TimeUnit.SECONDS);

        // TODO FLINK_SQL_PR2 check column values.
        List<Integer> data = DeltaTestUtils.readParquetTable(sinkTablePath + "/");
        System.out.println(data);
        assertThat(data.size(), equalTo(2));

    }

    private String buildSinkTableSql(String tablePath) {
        return String.format(
            "CREATE TABLE sinkTable ("
                + " name VARCHAR,"
                + " surname VARCHAR,"
                + " age INT"
                + ") WITH ("
                + " 'connector' = 'filesystem',"
                + " 'path' = '%s',"
                + " 'auto-compaction' = 'false',"
                + " 'format' = 'parquet',"
                + " 'sink.parallelism' = '3'"
                + ")",
            tablePath);
    }

    private String buildSourceTableSql(
        String tablePath,
        boolean includeHadoopConfDir,
        boolean isPartitioned) {

        String resourcesDirectory = new File("src/test/resources/hadoop-conf").getAbsolutePath();
        String hadoopConfDirPath = (includeHadoopConfDir ?
            String.format(
                " 'hadoop-conf-dir' = '%s',",
                resourcesDirectory)
            : ""
        );

        String partitionedClause = isPartitioned ? "PARTITIONED BY (col1, col3) " : "";

        return String.format(
            "CREATE TABLE %s ("
                + " name VARCHAR,"
                + " surname VARCHAR,"
                + " age INT"
                + ") "
                + partitionedClause
                + "WITH ("
                + " 'connector' = 'delta',"
                + hadoopConfDirPath
                + " 'table-path' = '%s'"
                + ")",
            DeltaSourceTableITCase.TEST_SOURCE_TABLE_NAME,
            tablePath
        );
    }

    private StreamExecutionEnvironment getTestStreamEnv(boolean streamingMode) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());

        if (streamingMode) {
            env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
            env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        } else {
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        }

        return env;
    }

}
