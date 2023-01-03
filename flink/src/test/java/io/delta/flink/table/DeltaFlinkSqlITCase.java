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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import io.delta.flink.utils.DeltaTestUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.delta.flink.utils.DeltaTestUtils.buildCluster;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Disabled
public class DeltaFlinkSqlITCase {

    private static final int PARALLELISM = 2;

    private static final Logger LOG = LoggerFactory.getLogger(DeltaFlinkSqlITCase.class);

    private static final String TEST_SOURCE_TABLE_NAME = "test_source_table";

    private static final String TEST_SINK_TABLE_NAME = "test_compact_sink_table";

    private static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private final MiniClusterWithClientResource miniClusterResource = buildCluster(PARALLELISM);

    @BeforeAll
    public static void beforeAll() throws IOException {
        TEMPORARY_FOLDER.create();
    }

    @AfterAll
    public static void afterAll() {
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
                + " 'sink.parallelism' = '2'"
                + ")",
            targetTablePath);

        tableEnv.executeSql(sinkTableSql);

        String insertSql = "INSERT INTO sinkTable SELECT * FROM sourceTable";
        tableEnv.executeSql(insertSql).await(10, TimeUnit.SECONDS);

        String selectSql = "SELECT * FROM sinkTable";
        //String selectSql = "SELECT * FROM sinkTable /*+ OPTIONS('path' = 'hello/me') */";
        //String selectSql = "SELECT * FROM sinkTable /*+ OPTIONS('connector' = 'dummy') */";
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
    public void testPipelineWithoutDeltaTables_3() throws Exception {

        String sourceTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
        String targetTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();

        DeltaTestUtils.initTestForTableApiTable(sourceTablePath);

        String catalogSQL = "CREATE CATALOG myDeltaCatalog WITH ('type' = 'delta-catalog');";

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(getTestStreamEnv());
        tableEnv.executeSql(catalogSQL);

        String useDeltaCatalog = "USE CATALOG myDeltaCatalog;";
        tableEnv.executeSql(useDeltaCatalog);

        String sourceTableSql = String.format("CREATE TABLE sourceTable ("
            + " col1 VARCHAR,"
            + " col2 VARCHAR,"
            + " col3 INT"
            + ") WITH ("
            + " 'connector' = 'filesystem',"
            + " 'path' = '%s',"
            + " 'format' = 'parquet'"
            + ")",
            sourceTablePath
            );

        tableEnv.executeSql(sourceTableSql);

        String sinkTableSql = String.format(
            "CREATE TABLE sinkTable ("
                + " col1 VARCHAR,"
                + " col2 VARCHAR,"
                + " col3 INT"
                + ") "
                + "PARTITIONED BY (col1)"
                + "WITH ("
                + " 'connector' = 'filesystem',"
                + " 'path' = '%s',"
                + " 'auto-compaction' = 'false',"
                + " 'format' = 'parquet',"
                + " 'sink.parallelism' = '2'"
                + ")",
            targetTablePath);

        tableEnv.executeSql(sinkTableSql);

        String insertSql = "INSERT INTO sinkTable SELECT * FROM sourceTable";
        tableEnv.executeSql(insertSql).await(120, TimeUnit.SECONDS);

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

        assertThat(sinkRows.size(), equalTo(1));
        assertThat(uniqueValues, equalTo(1L));
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

    @Test
    public void testSelectDeltaTableWithoutDeltaCatalog() throws Exception {

        // GIVEN
        String sourceTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(getTestStreamEnv());

        String sourceTableSql = String.format(
            "CREATE TABLE sourceTable ("
                + " col1 VARCHAR,"
                + " col2 VARCHAR,"
                + " col3 INT"
                + ") "
                + "WITH ("
                + " 'connector' = 'delta',"
                + " 'table-path' = '%s'"
                + ")",
            sourceTablePath);

        tableEnv.executeSql(sourceTableSql);

        String sinkTableSql = "CREATE TABLE sinkTable ("
            + " col1 VARCHAR,"
            + " col2 VARCHAR,"
            + " col3 INT"
            + ") WITH ("
            + "  'connector' = 'blackhole'"
            + ");";

        tableEnv.executeSql(sinkTableSql);

        // WHEN
        String selectSql = "SELECT * FROM sourceTable";

        // THEN
        ValidationException validationException =
            assertThrows(ValidationException.class, () -> tableEnv.executeSql(selectSql));

        assertThat(
            "Query Delta table should not be possible without Delta catalog.",
            validationException.getCause().getMessage(),
            containsString("Cannot discover a connector using option: 'connector'='delta'")
        );
    }

    @Test
    public void testSelectDeltaTableAsTempTable() throws Exception {

        // GIVEN
        String sourceTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
        DeltaTestUtils.initTestForTableApiTable(sourceTablePath);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(getTestStreamEnv());

        String catalogSQL = "CREATE CATALOG myDeltaCatalog WITH ('type' = 'delta-catalog');";
        tableEnv.executeSql(catalogSQL);

        String useDeltaCatalog = "USE CATALOG myDeltaCatalog;";
        tableEnv.executeSql(useDeltaCatalog);

        String sourceTableSql = String.format(
            "CREATE TABLE sourceTable ("
                + " col1 VARCHAR,"
                + " col2 VARCHAR,"
                + " col3 INT"
                + ") "
                + "WITH ("
                + " 'connector' = 'delta',"
                + " 'table-path' = '%s'"
                + ")",
            sourceTablePath);

        tableEnv.executeSql(sourceTableSql);

        String resourcesDirectory = new File("src/test/resources/hadoop-conf").getAbsolutePath();
        String tempDeltaTable = "CREATE TEMPORARY TABLE sourceTable_tmp"
            + "  WITH  ("
            + " 'mode' = 'streaming'"
            + ")"
            + "  LIKE sourceTable;";

        tableEnv.executeSql(tempDeltaTable);

        // WHEN
        String selectSql = "SELECT * FROM sourceTable_tmp";

        // THEN
        TableResult selectResult = tableEnv.executeSql(selectSql);
        List<Row> sourceRows = new ArrayList<>();
        try (org.apache.flink.util.CloseableIterator<Row> collect = selectResult.collect()) {
            collect.forEachRemaining(sourceRows::add);
        }

        assertThat(sourceRows.size(), equalTo(1));
    }

    @Test
    public void testSelectViewFromDeltaTable() throws Exception {

        // GIVEN
        String sourceTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
        DeltaTestUtils.initTestForTableApiTable(sourceTablePath);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(getTestStreamEnv());

        String catalogSQL = "CREATE CATALOG myDeltaCatalog WITH ('type' = 'delta-catalog');";
        tableEnv.executeSql(catalogSQL);

        String useDeltaCatalog = "USE CATALOG myDeltaCatalog;";
        tableEnv.executeSql(useDeltaCatalog);

        String sourceTableSql = String.format(
            "CREATE TABLE sourceTable ("
                + " col1 VARCHAR,"
                + " col2 VARCHAR,"
                + " col3 INT"
                + ") "
                + "WITH ("
                + " 'connector' = 'delta',"
                + " 'table-path' = '%s'"
                + ")",
            sourceTablePath);

        tableEnv.executeSql(sourceTableSql);

        String viewSql = "CREATE VIEW sourceTable_view AS "
            + "SELECT col1 from sourceTable";

        String temporaryViewSql = "CREATE TEMPORARY VIEW sourceTable_view_tmp AS "
            + "SELECT col1 from sourceTable";

        tableEnv.executeSql(viewSql);
        tableEnv.executeSql(temporaryViewSql);

        // WHEN
        String selectViewSql = "SELECT * FROM sourceTable_view";
        String selectViewTmpSql = "SELECT * FROM sourceTable_view_tmp";

        // THEN
        TableResult selectViewResult = tableEnv.executeSql(selectViewSql);
        TableResult selectTmpViewResult = tableEnv.executeSql(selectViewTmpSql);

        assertSelectResult(selectViewResult);
        assertSelectResult(selectTmpViewResult);
    }

    @Test
    public void testSelectWithClauseFromDeltaTable() throws Exception {

        // GIVEN
        String sourceTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
        DeltaTestUtils.initTestForTableApiTable(sourceTablePath);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(getTestStreamEnv());

        String catalogSQL = "CREATE CATALOG myDeltaCatalog WITH ('type' = 'delta-catalog');";
        tableEnv.executeSql(catalogSQL);

        String useDeltaCatalog = "USE CATALOG myDeltaCatalog;";
        tableEnv.executeSql(useDeltaCatalog);

        String sourceTableSql = String.format(
            "CREATE TABLE sourceTable ("
                + " col1 VARCHAR,"
                + " col2 VARCHAR,"
                + " col3 INT"
                + ") "
                + "WITH ("
                + " 'connector' = 'delta',"
                + " 'table-path' = '%s'"
                + ")",
            sourceTablePath);

        tableEnv.executeSql(sourceTableSql);

        // WHEN
        String withSelect = "WITH sourceTable_with AS ("
            + "SELECT col1 FROM sourceTable"
            + ") "
            + "SELECT * FROM sourceTable_with";

        // THEN
        TableResult selectViewResult= tableEnv.executeSql(withSelect);

        assertSelectResult(selectViewResult);
    }

    private void assertSelectResult(TableResult selectResult) throws Exception {
        List<Row> sourceRows = new ArrayList<>();
        try (CloseableIterator<Row> collect = selectResult.collect()) {
            collect.forEachRemaining(sourceRows::add);
        }

        assertThat(sourceRows.size(), equalTo(1));
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

    private StreamExecutionEnvironment getTestStreamEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(100, CheckpointingMode.EXACTLY_ONCE);
        return env;
    }
}
