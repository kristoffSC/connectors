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
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import io.delta.flink.utils.DeltaTestUtils;
import io.delta.flink.utils.TestParquetReader;
import io.github.artsok.ParameterizedRepeatedIfExceptionsTest;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.rules.TemporaryFolder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.AddFile;

public class DeltaSinkTableITCase {

    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    public static final RowType TEST_ROW_TYPE = new RowType(Arrays.asList(
        new RowType.RowField("col1", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("col2", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("col3", new IntType())
    ));

    protected RowType testRowType;

    @BeforeAll
    public static void beforeAll() throws IOException {
        TEMPORARY_FOLDER.create();
    }

    @AfterAll
    public static void afterAll() {
        TEMPORARY_FOLDER.delete();
    }

    /**
     * @return Stream of test {@link Arguments} elements. Arguments are in order:
     * <ul>
     *     <li>isPartitioned</li>
     *     <li>includeOptionalOptions</li>
     *     <li>useStaticPartition</li>
     *     <li>useBoundedMode</li>
     * </ul>
     */
    private static Stream<Arguments> tableArguments() {
        return Stream.of(
            Arguments.of(false, false, false, false),
            Arguments.of(false, true, false, false),
            Arguments.of(true, false, false, false),
            Arguments.of(true, false, true, false),
            Arguments.of(false, false, false, true)
        );
    }

    @SuppressWarnings("unchecked")
    @ParameterizedRepeatedIfExceptionsTest(
        suspend = 2000L, repeats = 3,
        name = "isPartitioned = {0}, " +
            "includeOptionalOptions = {1}, " +
            "useStaticPartition = {2}, " +
            "useBoundedMode = {3}")
    @MethodSource("tableArguments")
    public void testTableApi(
            boolean isPartitioned,
            boolean includeOptionalOptions,
            boolean useStaticPartition,
            boolean useBoundedMode) throws Exception {

        String deltaTablePath = setup(includeOptionalOptions);

        // GIVEN
        DeltaLog deltaLog = DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), deltaTablePath);
        List<AddFile> initialDeltaFiles = deltaLog.snapshot().getAllFiles();

        // WHEN
        if (useBoundedMode) {
            runFlinkJob(
                deltaTablePath,
                useBoundedMode,
                includeOptionalOptions,
                useStaticPartition,
                isPartitioned);
        } else {
            runFlinkJobInBackground(
                deltaTablePath,
                useBoundedMode,
                includeOptionalOptions,
                useStaticPartition,
                isPartitioned
            );
        }
        DeltaTestUtils.waitUntilDeltaLogExists(deltaLog, deltaLog.snapshot().getVersion() + 1);

        // THEN
        deltaLog.update();
        int tableRecordsCount =
            TestParquetReader.readAndValidateAllTableRecords(
                deltaLog,
                TEST_ROW_TYPE,
                DataFormatConverters.getConverterForDataType(
                    TypeConversions.fromLogicalToDataType(TEST_ROW_TYPE)));
        List<AddFile> files = deltaLog.update().getAllFiles();
        assertThat(files.size() > initialDeltaFiles.size(), equalTo(true));
        assertThat(tableRecordsCount > 0, equalTo(true));

        if (isPartitioned) {
            assertThat(
                deltaLog.snapshot().getMetadata().getPartitionColumns(),
                CoreMatchers.is(Arrays.asList("col1", "col3")));
        } else {
            assertThat(deltaLog.snapshot().getMetadata().getPartitionColumns().isEmpty(),
                equalTo(true));
        }

        List<String> expectedTableCols = includeOptionalOptions ?
            Arrays.asList("col1", "col2", "col3", "col4") : Arrays.asList("col1", "col2", "col3");
        assertThat(
            Arrays.asList(deltaLog.snapshot().getMetadata().getSchema().getFieldNames()),
            CoreMatchers.is(expectedTableCols));

        if (useStaticPartition) {
            for (AddFile file : deltaLog.snapshot().getAllFiles()) {
                assertThat(file.getPartitionValues().get("col1"), equalTo("val1"));
            }
        }
    }

    public String setup(boolean includeOptionalOptions) {
        try {
            String deltaTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
            // one of the optional options is whether the sink should try to update the table's
            // schema, so we are initializing an existing table to test this behaviour
            if (includeOptionalOptions) {
                DeltaTestUtils.initTestForTableApiTable(deltaTablePath);
                testRowType = DeltaSinkTestUtils.addNewColumnToSchema(TEST_ROW_TYPE);
            } else {
                testRowType = TEST_ROW_TYPE;
            }
            return deltaTablePath;
        } catch (IOException e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }
    }

    /**
     * Runs Flink job in a daemon thread.
     * <p>
     * This workaround is needed because if we try to first run the Flink job and then query the
     * table with Delta Standalone Reader (DSR) then we are hitting "closes classloader exception"
     * which in short means that finished Flink job closes the classloader for the classes that DSR
     * tries to reuse.
     */
    private void runFlinkJobInBackground(
            String deltaTablePath,
            boolean useBoundedMode,
            boolean includeOptionalOptions,
            boolean useStaticPartition,
            boolean isPartitioned) {
        new Thread(() -> {
            try {
                runFlinkJob(
                    deltaTablePath,
                    useBoundedMode,
                    includeOptionalOptions,
                    useStaticPartition,
                    isPartitioned
                );
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private void runFlinkJob(
            String deltaTablePath,
            boolean useBoundedMode,
            boolean includeOptionalOptions,
            boolean useStaticPartition,
            boolean isPartitioned) throws ExecutionException, InterruptedException {

        TableEnvironment tableEnv;
        if (useBoundedMode) {
            EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inBatchMode()
                .build();
            tableEnv = TableEnvironment.create(settings);
        } else {
            tableEnv = StreamTableEnvironment.create(getTestStreamEnv());
        }

        final String testSourceTableName = "test_source_table";
        String sourceSql = buildSourceTableSql(
            testSourceTableName,
            10,
            includeOptionalOptions,
            useBoundedMode);
        tableEnv.executeSql(sourceSql);

        final String testCompactSinkTableName = "test_compact_sink_table";
        String sinkSql = buildSinkTableSql(
            testCompactSinkTableName,
            deltaTablePath,
            includeOptionalOptions,
            isPartitioned);
        tableEnv.executeSql(sinkSql);

        final String sql1 = buildInsertIntoSql(
            testCompactSinkTableName,
            testSourceTableName,
            useStaticPartition);
        try {
            tableEnv.executeSql(sql1).await();
        } catch (Exception exception) {
            if (!exception.getMessage().contains("Failed to wait job finish")) {
                throw exception;
            }
        }
    }

    private StreamExecutionEnvironment getTestStreamEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        return env;
    }

    private String buildSourceTableSql(
            String testSourceTableName,
            int rows,
            boolean includeOptionalOptions,
            boolean useBoundedMode) {
        String additionalCol = includeOptionalOptions ? ", col4 INT " : "";
        String rowLimit = useBoundedMode ? "'number-of-rows' = '1'," : "";
        return String.format(
            "CREATE TABLE %s ("
                + " col1 VARCHAR,"
                + " col2 VARCHAR,"
                + " col3 INT"
                + additionalCol
                + ") WITH ("
                + " 'connector' = 'datagen',"
                + rowLimit
                + " 'rows-per-second' = '20'"
                + ")",
            testSourceTableName, rows);
    }

    private String buildSinkTableSql(
            String tableName,
            String tablePath,
            boolean includeOptionalOptions,
            boolean isPartitioned) {
        String resourcesDirectory = new File("src/test/resources/hadoop-conf").getAbsolutePath();
        String optionalTableOptions = (includeOptionalOptions ?
            String.format(
                " 'hadoop-conf-dir' = '%s', 'mergeSchema' = 'true', ",
                resourcesDirectory)
            : ""
        );

        String partitionedClause = isPartitioned ? "PARTITIONED BY (col1, col3) " : "";
        String additionalCol = includeOptionalOptions ? ", col4 INT " : "";

        return String.format(
            "CREATE TABLE %s ("
                + " col1 VARCHAR,"
                + " col2 VARCHAR,"
                + " col3 INT"
                + additionalCol
                + ") "
                + partitionedClause
                + "WITH ("
                + " 'connector' = 'delta',"
                + optionalTableOptions
                + " 'table-path' = '%s'"
                + ")",
            tableName, tablePath);
    }

    private String buildInsertIntoSql(
            String sinkTable,
            String sourceTable,
            boolean useStaticPartition) {
        if (useStaticPartition) {
            return String.format(
                "INSERT INTO %s PARTITION(col1='val1') " +
                    "SELECT col2, col3 FROM %s", sinkTable, sourceTable);
        }
        return String.format("INSERT INTO %s SELECT * FROM %s", sinkTable, sourceTable);
    }
}
