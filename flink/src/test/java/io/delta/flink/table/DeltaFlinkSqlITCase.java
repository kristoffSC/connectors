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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
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
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.delta.flink.utils.DeltaTestUtils.buildCluster;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;

public class DeltaFlinkSqlITCase {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaFlinkSqlITCase.class);

    private static final int PARALLELISM = 4;

    private static final String TEST_SOURCE_TABLE_NAME = "test_source_table";

    private static final String TEST_SINK_TABLE_NAME = "test_compact_sink_table";

    private static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

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
            Thread thread = new Thread(r);
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
        } catch (Exception e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }
    }

    @AfterEach
    public void afterEach() {
        miniClusterResource.after();
    }

    @Test
    public void testPipelineWithoutDeltaTables_1() throws Exception {

        String catalogSQL = "CREATE CATALOG myDeltaCatalog WITH ('type' = 'delta-catalog');";

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(getTestStreamEnv());
        tableEnv.executeSql(catalogSQL);

        String useDeltaCatalog = "USE CATALOG myDeltaCatalog;";
        tableEnv.executeSql(useDeltaCatalog);

        String sourceTableSql = "CREATE TABLE sourceTable ("
            + " col1 VARCHAR,"
            + " col2 VARCHAR,"
            + " col3 INT"
            + ") WITH ("
            + "'connector' = 'datagen',"
            + "'rows-per-second' = '1',"
            + "'fields.col3.kind' = 'sequence',"
            + "'fields.col3.start' = '1',"
            + "'fields.col3.end' = '5'"
            + ")";

        tableEnv.executeSql(sourceTableSql);

        String sinkTableSql = "CREATE TABLE sinkTable ("
            + " col1 VARCHAR,"
            + " col2 VARCHAR,"
            + " col3 INT"
            + ") WITH ("
            + "  'connector' = 'blackhole'"
            + ");";

        tableEnv.executeSql(sinkTableSql);

        String querySql = "INSERT INTO sinkTable SELECT * FROM sourceTable";
        TableResult result = tableEnv.executeSql(querySql);

        List<Row> results = new ArrayList<>();
        try (org.apache.flink.util.CloseableIterator<Row> collect = result.collect()) {
            collect.forEachRemaining(results::add);
        }

        assertThat(results.size(), equalTo(1));
        assertThat(results.get(0).getKind(), equalTo(RowKind.INSERT));
    }

    @Test
    public void testPipelineWithoutDeltaTables_2() throws Exception {

        String targetTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();

        String catalogSQL = "CREATE CATALOG myDeltaCatalog WITH ('type' = 'delta-catalog');";

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(getTestStreamEnv());
        tableEnv.executeSql(catalogSQL);

        String useDeltaCatalog = "USE CATALOG myDeltaCatalog;";
        tableEnv.executeSql(useDeltaCatalog);

        String sourceTableSql = "CREATE TABLE sourceTable ("
            + " col1 VARCHAR,"
            + " col2 VARCHAR,"
            + " col3 INT"
            + ") WITH ("
            + "'connector' = 'datagen',"
            + "'rows-per-second' = '1',"
            + "'fields.col3.kind' = 'sequence',"
            + "'fields.col3.start' = '1',"
            + "'fields.col3.end' = '5'"
            + ")";

        tableEnv.executeSql(sourceTableSql);

        String sinkTableSql = String.format(
            "CREATE TABLE sinkTable ("
                + " col1 VARCHAR,"
                + " col2 VARCHAR,"
                + " col3 INT"
                + ") WITH ("
                + " 'connector' = 'filesystem',"
                + " 'path' = '%s',"
                + " 'auto-compaction' = 'false',"
                + " 'format' = 'parquet',"
                + " 'sink.parallelism' = '3'"
                + ")",
            targetTablePath);

        tableEnv.executeSql(sinkTableSql);

        String insertSql = "INSERT INTO sinkTable SELECT * FROM sourceTable";
        tableEnv.executeSql(insertSql).await(5, TimeUnit.SECONDS);

        String selectSql = "SELECT * FROM sinkTable";
        TableResult selectResult = tableEnv.executeSql(selectSql);

        List<Row> sinkRows = new ArrayList<>();
        try (org.apache.flink.util.CloseableIterator<Row> collect = selectResult.collect()) {
            collect.forEachRemaining(sinkRows::add);
        }

        long uniqueValues =
            sinkRows.stream()
                .map((Function<Row, Integer>) row -> row.getFieldAs("col3"))
                .distinct().count();

        assertThat(sinkRows.size(), equalTo(5));
        assertThat(uniqueValues, equalTo(5L));
    }

    @Test
    public void testInsertIntoDeltaTableWithoutDeltaCatalog() throws Exception {

        // GIVEN
        String targetTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(getTestStreamEnv());

        String sourceTableSql = "CREATE TABLE sourceTable ("
            + " col1 VARCHAR,"
            + " col2 VARCHAR,"
            + " col3 INT"
            + ") WITH ("
            + "'connector' = 'datagen',"
            + "'rows-per-second' = '1',"
            + "'fields.col3.kind' = 'sequence',"
            + "'fields.col3.start' = '1',"
            + "'fields.col3.end' = '5'"
            + ")";

        tableEnv.executeSql(sourceTableSql);

        String sinkTableSql = String.format(
            "CREATE TABLE sinkTable ("
                + " col1 VARCHAR,"
                + " col2 VARCHAR,"
                + " col3 INT"
                + ") "
                + "WITH ("
                + " 'connector' = 'delta',"
                + " 'table-path' = '%s'"
                + ")",
            targetTablePath);

        tableEnv.executeSql(sinkTableSql);

        // WHEN
        String insertSql = "INSERT INTO sinkTable SELECT * FROM sourceTable";

        // THEN
        ValidationException validationException =
            assertThrows(ValidationException.class, () -> tableEnv.executeSql(insertSql));

        assertThat(
            "Query Delta table should not be possible without Delta catalog.",
            validationException.getCause().getMessage(),
            containsString("Cannot discover a connector using option: 'connector'='delta'")
        );
    }

    @Disabled
    @Test
    public void testUsingTwoCatalogs() throws Exception {

        // GIVEN
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(getTestStreamEnv());

        String deltaCatalogSql = "CREATE CATALOG myDeltaCatalog WITH ('type' = 'delta-catalog');";
        String defaultCatalogSql = "CREATE CATALOG flinkDefaultCatalog WITH"
            + " ('type' = 'generic_in_memory');";

        tableEnv.executeSql(deltaCatalogSql);
        tableEnv.executeSql(defaultCatalogSql);

        String targetTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();

        String sourceTableSql = "CREATE TABLE `flinkDefaultCatalog`.`default`.`sourceTable ("
            + " col1 VARCHAR,"
            + " col2 VARCHAR,"
            + " col3 INT"
            + ") WITH ("
            + "'connector' = 'datagen',"
            + "'rows-per-second' = '1',"
            + "'fields.col3.kind' = 'sequence',"
            + "'fields.col3.start' = '1',"
            + "'fields.col3.end' = '5'"
            + ")";

        tableEnv.executeSql(sourceTableSql);


        String sinkTableSql = String.format(
            "CREATE TABLE `myDeltaCatalog`.`default`.`sinkTable ("
                + " col1 VARCHAR,"
                + " col2 VARCHAR,"
                + " col3 INT"
                + ") "
                + "WITH ("
                + " 'connector' = 'delta',"
                + " 'table-path' = '%s'"
                + ")",
            targetTablePath);

        tableEnv.executeSql(sinkTableSql);
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

    @ParameterizedRepeatedIfExceptionsTest(
        suspend = 2000L, repeats = 3,
        name = "isPartitioned = {0}, " +
            "includeOptionalOptions = {1}, " +
            "useStaticPartition = {2}, " +
            "useBoundedMode = {3}")
    @MethodSource("tableArguments")
    public void testInsertIntoDeltaTableWithAllFields(
            boolean isPartitioned,
            boolean includeOptionalOptions,
            boolean useStaticPartition,
            boolean useBoundedMode) throws Exception {

        String deltaTablePath = setupTestFolders(includeOptionalOptions);

        // Column `col1` would be a partition column if isPartitioned or useStaticPartition
        // set to true.
        // Column `col3` would be a partition column if isPartitioned is set to true.
        String insertSql = buildInsertAllFieldsSql(useStaticPartition);

        DeltaLog deltaLog =
            testTableJob(
                deltaTablePath,
                isPartitioned,
                includeOptionalOptions,
                useStaticPartition,
                useBoundedMode,
                insertSql
            );

        Snapshot snapshot = deltaLog.update();

        // Validate that every inserted column has non value.
        try (CloseableIterator<RowRecord> open = snapshot.open()) {
            while (open.hasNext()) {
                RowRecord record = open.next();
                assertThat(record.getString("col1"), notNullValue());
                assertThat(record.getString("col2"), notNullValue());
                assertThat(record.getInt("col3"), notNullValue());
            }
        }
    }

    @ParameterizedRepeatedIfExceptionsTest(
        suspend = 2000L, repeats = 3,
        name = "isPartitioned = {0}, " +
            "includeOptionalOptions = {1}, " +
            "useStaticPartition = {2}, " +
            "useBoundedMode = {3}")
    @MethodSource("tableArguments")
    public void testInsertQueryWithOneFiled(
            boolean isPartitioned,
            boolean includeOptionalOptions,
            boolean useStaticPartition,
            boolean useBoundedMode) throws Exception {

        // Column `col1` would be a partition column if isPartitioned or useStaticPartition
        // set to true.
        // Column `col3` would be a partition column if isPartitioned is set to true.
        String insertSql = buildInsertOneFieldSql(useStaticPartition);

        DeltaLog deltaLog =
            testTableJob(
                TEMPORARY_FOLDER.newFolder().getAbsolutePath(),
                isPartitioned,
                includeOptionalOptions,
                useStaticPartition,
                useBoundedMode,
                insertSql
            );

        // Validate that every inserted column has null or not null value depends on the settings
        try (CloseableIterator<RowRecord> open = deltaLog.update().open()) {
            while (open.hasNext()) {
                RowRecord record = open.next();
                assertThat(record.getString("col1"), notNullValue());
                assertThat(
                    record.getString("col2"),
                    (useStaticPartition) ? notNullValue() : nullValue()
                );
                assertThat(
                    record.isNullAt("col3"),
                    (isPartitioned) ? equalTo(false) : equalTo(true)
                );
            }
        }
    }

    @ParameterizedRepeatedIfExceptionsTest(
        suspend = 2000L, repeats = 3,
        name = "isPartitioned = {0}, " +
            "includeOptionalOptions = {1}, " +
            "useStaticPartition = {2}, " +
            "useBoundedMode = {3}")
    @MethodSource("tableArguments")
    public void testInsertQueryWithOneFiledWithNullCasts(
            boolean isPartitioned,
            boolean includeOptionalOptions,
            boolean useStaticPartition,
            boolean useBoundedMode) throws Exception {

        // Column `col1` would be a partition column if isPartitioned or useStaticPartition
        // set to true.
        // Column `col3` would be a partition column if isPartitioned is set to true.
        String insertSql = buildInsertOneFieldSqlNullCasts(
            useStaticPartition,
            includeOptionalOptions
        );

        DeltaLog deltaLog =
            testTableJob(
                TEMPORARY_FOLDER.newFolder().getAbsolutePath(),
                isPartitioned,
                includeOptionalOptions,
                useStaticPartition,
                useBoundedMode,
                insertSql
            );

        // Validate that every inserted column has null or not null value depends on the settings
        try (CloseableIterator<RowRecord> open = deltaLog.update().open()) {
            while (open.hasNext()) {
                RowRecord record = open.next();
                assertThat(record.getString("col1"), notNullValue());
                assertThat(
                    record.getString("col2"),
                    (useStaticPartition) ? notNullValue() : nullValue()
                );
                assertThat(
                    record.isNullAt("col3"),
                    (isPartitioned) ? equalTo(false) : equalTo(true)
                );
            }
        }
    }

    private String buildInsertAllFieldsSql(boolean useStaticPartition) {

        if (useStaticPartition) {
            return String.format(
                "INSERT INTO %s PARTITION(col1='val1') SELECT col2, col3 FROM %s",
                DeltaFlinkSqlITCase.TEST_SINK_TABLE_NAME,
                DeltaFlinkSqlITCase.TEST_SOURCE_TABLE_NAME
            );
        }

        return String.format(
            "INSERT INTO %s SELECT * FROM %s",
            DeltaFlinkSqlITCase.TEST_SINK_TABLE_NAME,
            DeltaFlinkSqlITCase.TEST_SOURCE_TABLE_NAME
        );
    }

    private String buildInsertOneFieldSql(boolean useStaticPartition) {

        if (useStaticPartition) {
            return String.format(
                "INSERT INTO %s PARTITION(col1='val1') (col2) (SELECT col2 FROM %s)",
                DeltaFlinkSqlITCase.TEST_SINK_TABLE_NAME,
                DeltaFlinkSqlITCase.TEST_SOURCE_TABLE_NAME
            );
        }

        return String.format(
            "INSERT INTO %s (col1) (SELECT col1 FROM %s)",
            DeltaFlinkSqlITCase.TEST_SINK_TABLE_NAME,
            DeltaFlinkSqlITCase.TEST_SOURCE_TABLE_NAME
        );
    }

    private String buildInsertOneFieldSqlNullCasts(
        boolean useStaticPartition,
        boolean includeOptionalOptions) {

        if (useStaticPartition) {
            return String.format(
                "INSERT INTO %s PARTITION(col1='val1') (SELECT col2, cast(null as INT)"
                    + ((includeOptionalOptions) ? ", cast(null as INT) " : "")
                    + " FROM %s)",
                DeltaFlinkSqlITCase.TEST_SINK_TABLE_NAME,
                DeltaFlinkSqlITCase.TEST_SOURCE_TABLE_NAME
            );
        }

        return String.format(
            "INSERT INTO %s (SELECT col1, cast(null as VARCHAR), cast(null as INT) "
                + ((includeOptionalOptions) ? ", cast(null as INT) " : "")
                + "FROM %s)",
            DeltaFlinkSqlITCase.TEST_SINK_TABLE_NAME,
            DeltaFlinkSqlITCase.TEST_SOURCE_TABLE_NAME
        );
    }

    private DeltaLog testTableJob(
            String deltaTablePath,
            boolean isPartitioned,
            boolean includeOptionalOptions,
            boolean useStaticPartition,
            boolean useBoundedMode,
            String insertSql) throws Exception {

        // GIVEN
        DeltaLog deltaLog = DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), deltaTablePath);
        List<AddFile> initialDeltaFiles = deltaLog.snapshot().getAllFiles();

        // WHEN
        setupAndRunFLinkJob(
            isPartitioned,
            includeOptionalOptions,
            useBoundedMode,
            deltaTablePath,
            insertSql);

        DeltaTestUtils.waitUntilDeltaLogExists(deltaLog, deltaLog.snapshot().getVersion() + 1);

        // THEN
        validateTargetTable(
            isPartitioned,
            includeOptionalOptions,
            useStaticPartition,
            deltaLog,
            initialDeltaFiles
        );

        return deltaLog;
    }

    @SuppressWarnings("unchecked")
    private void validateTargetTable(
            boolean isPartitioned,
            boolean includeOptionalOptions,
            boolean useStaticPartition,
            DeltaLog deltaLog,
            List<AddFile> initialDeltaFiles) throws IOException {

        int tableRecordsCount =
            TestParquetReader.readAndValidateAllTableRecords(
                deltaLog,
                TEST_ROW_TYPE,
                DataFormatConverters.getConverterForDataType(
                    TypeConversions.fromLogicalToDataType(TEST_ROW_TYPE))
            );

        Snapshot snapshot = deltaLog.update();
        List<AddFile> files = snapshot.getAllFiles();
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

    private void setupAndRunFLinkJob(
                boolean isPartitioned,
                boolean includeOptionalOptions,
                boolean useBoundedMode,
                String deltaTablePath,
                String insertSql) throws Exception {

        if (useBoundedMode) {
            runFlinkJob(
                deltaTablePath,
                useBoundedMode,
                includeOptionalOptions,
                isPartitioned,
                insertSql);
        } else {
            runFlinkJobInBackground(
                deltaTablePath,
                useBoundedMode,
                includeOptionalOptions,
                isPartitioned,
                insertSql
            );
        }
    }

    public String setupTestFolders(boolean includeOptionalOptions) {
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
            boolean isPartitioned,
            String insertSql) {

        CompletableFuture.runAsync(
            () -> runFlinkJob(
                deltaTablePath,
                useBoundedMode,
                includeOptionalOptions,
                isPartitioned,
                insertSql),
            testWorkers
        ).exceptionally(throwable -> {
            LOG.error("Error while running Flink job in background.", throwable);
            return null;
        });
    }

    /**
     * Run Flink Job and block current thread until job finishes.
     */
    private void runFlinkJob(
            String deltaTablePath,
            boolean useBoundedMode,
            boolean includeOptionalOptions,
            boolean isPartitioned,
            String insertSql) {

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

        String catalogSQL = "CREATE CATALOG myDeltaCatalog WITH ('type' = 'delta-catalog');";
        tableEnv.executeSql(catalogSQL);

        String useDeltaCatalog = "USE CATALOG myDeltaCatalog;";
        tableEnv.executeSql(useDeltaCatalog);

        String sourceSql = buildSourceTableSql(
            10,
            includeOptionalOptions,
            useBoundedMode);
        tableEnv.executeSql(sourceSql);

        String sinkSql = buildSinkTableSql(
            deltaTablePath,
            includeOptionalOptions,
            isPartitioned);

        tableEnv.executeSql(sinkSql);

        try {
            tableEnv.executeSql(insertSql).await();
        } catch (Exception e) {
            if (!e.getMessage().contains("Failed to wait job finish")) {
                throw new RuntimeException(e);
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
            DeltaFlinkSqlITCase.TEST_SOURCE_TABLE_NAME, rows);
    }

    private String buildSinkTableSql(
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
            DeltaFlinkSqlITCase.TEST_SINK_TABLE_NAME, tablePath);
    }
}
