package io.delta.flink.table;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import io.delta.flink.utils.DeltaTestUtils;
import io.delta.flink.utils.ExecutionITCaseTestConstants;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.junit5.MiniClusterExtension;
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNot.not;

public class DeltaEndToEndTableITCase {

    private static final int PARALLELISM = 2;

    private static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

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

        assertThat(sinkTablePath, not(equalTo(nonPartitionedLargeTablePath)));
    }

    @Test
    public void testEndToEndTableJob() throws Exception {

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
            getTestStreamEnv(true) // streamingMode = false
        );

        setupDeltaCatalog(tableEnv);

        String sourceTable =
            String.format("CREATE TABLE sourceTable ("
                    + "col1 BIGINT,"
                    + "col2 BIGINT,"
                    + "col3 VARCHAR"
                    + ") "
                    + "PARTITIONED BY (col1)"
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

    private void setupDeltaCatalog(StreamTableEnvironment tableEnv) {

        String catalogSQL = "CREATE CATALOG myDeltaCatalog WITH ('type' = 'delta-catalog');";
        String useDeltaCatalog = "USE CATALOG myDeltaCatalog;";

        tableEnv.executeSql(catalogSQL);
        tableEnv.executeSql(useDeltaCatalog);
    }
}
