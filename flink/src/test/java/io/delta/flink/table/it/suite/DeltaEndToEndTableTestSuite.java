package io.delta.flink.table.it.suite;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.delta.flink.utils.DeltaTestUtils;
import io.delta.flink.utils.ExecutionITCaseTestConstants;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.rules.TemporaryFolder;
import static io.delta.flink.utils.DeltaTestUtils.buildClusterResourceConfig;
import static io.delta.flink.utils.DeltaTestUtils.getTestStreamEnv;
import static io.delta.flink.utils.DeltaTestUtils.verifyDeltaTable;
import static io.delta.flink.utils.ExecutionITCaseTestConstants.LARGE_TABLE_ALL_COLUMN_NAMES;
import static io.delta.flink.utils.ExecutionITCaseTestConstants.LARGE_TABLE_ALL_COLUMN_TYPES;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class DeltaEndToEndTableTestSuite {

    private static final int PARALLELISM = 2;

    protected static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @RegisterExtension
    private static final MiniClusterExtension miniClusterResource =  new MiniClusterExtension(
        buildClusterResourceConfig(PARALLELISM)
    );

    /**
     * Schema for this table has only
     * {@link ExecutionITCaseTestConstants#LARGE_TABLE_ALL_COLUMN_NAMES} of type
     * {@link ExecutionITCaseTestConstants#LARGE_TABLE_ALL_COLUMN_TYPES} columns.
     * Column types are long, long, String
     */
    private String nonPartitionedLargeTablePath;

    private String sinkTablePath;

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
            nonPartitionedLargeTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
            sinkTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
            DeltaTestUtils.initTestForNonPartitionedLargeTable(nonPartitionedLargeTablePath);
        } catch (Exception e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }

        assertThat(sinkTablePath).isNotEqualToIgnoringCase(nonPartitionedLargeTablePath);
    }

    @Test
    public void testEndToEndTableJob() throws Exception {

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
            getTestStreamEnv(false) // streamingMode = false
        );

        setupDeltaCatalog(tableEnv);

        String sourceTable =
            String.format("CREATE TABLE sourceTable ("
                    + "col1 BIGINT,"
                    + "col2 BIGINT,"
                    + "col3 VARCHAR"
                    + ") "
                    + "WITH ("
                    + " 'connector' = 'delta',"
                    + " 'table-path' = '%s'"
                    + ")",
                nonPartitionedLargeTablePath);

        String sinkTable =
            String.format("CREATE TABLE sinkTable ("
                    + "col1 BIGINT,"
                    + "col2 BIGINT,"
                    + "col3 VARCHAR"
                    + ") "
                    + "WITH ("
                    + " 'connector' = 'delta',"
                    + " 'table-path' = '%s'"
                    + ")",
                sinkTablePath);

        String selectToInsertSql = "INSERT INTO sinkTable SELECT * FROM sourceTable;";

        tableEnv.executeSql(sourceTable);
        tableEnv.executeSql(sinkTable);

        tableEnv.executeSql(selectToInsertSql).await(10, TimeUnit.SECONDS);

        RowType rowType = RowType.of(LARGE_TABLE_ALL_COLUMN_TYPES, LARGE_TABLE_ALL_COLUMN_NAMES);
        verifyDeltaTable(sinkTablePath, rowType, 1100);
    }

    // TODO DC - work on this one
    @Test
    public void testWriteAndReadNestedStructures()
        throws Exception {

        String sourceTableSql = "CREATE TABLE sourceTable ("
            + " col1 INT,"
            + " col2 ROW <a INT, b INT>"
            + ") WITH ("
            + "'connector' = 'datagen',"
            + "'rows-per-second' = '1',"
            + "'fields.col1.kind' = 'sequence',"
            + "'fields.col1.start' = '1',"
            + "'fields.col1.end' = '5'"
            + ")";

        String deltaSinkTable =
            String.format("CREATE TABLE deltaSinkTable ("
                    + "innerA INT,"
                    + "innerB INT"
                    + ") "
                    + "WITH ("
                    + " 'connector' = 'delta',"
                    + " 'table-path' = '%s'"
                    + ")",
                sinkTablePath);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
            getTestStreamEnv(true) // streamingMode = false
        );

        setupDeltaCatalog(tableEnv);

        tableEnv.executeSql(sourceTableSql);
        tableEnv.executeSql(deltaSinkTable);

        tableEnv
            .executeSql("INSERT INTO deltaSinkTable SELECT col2.a, col2.b FROM sourceTable")
            .await(10, TimeUnit.SECONDS);

        TableResult tableResult = tableEnv.executeSql("SELECT * FROM deltaSinkTable");

        List<Row> result = new ArrayList<>();
        try (CloseableIterator<Row> collect = tableResult.collect()) {
            while (collect.hasNext()) {
                result.add(collect.next());
            }
        }

        assertThat(result).hasSize(5);
        for (Row row : result) {
            assertThat(row.getField("innerA")).isInstanceOf(Integer.class);
            assertThat(row.getField("innerB")).isInstanceOf(Integer.class);
        }

        // tableEnv.executeSql("INSERT INTO deltaSinkTable SELECT * FROM sourceTable")
        //    .await(10, TimeUnit.SECONDS);

        // For Flink 1.15
        // Caused by: java.lang.UnsupportedOperationException: Complex types not supported.
        // tableEnv.executeSql("SELECT col2.a AS innerA, col2.b AS innerB FROM deltaSinkTable")
        // .print();
    }

    public abstract void setupDeltaCatalog(TableEnvironment tableEnv);
}
