package io.delta.flink.table;

import java.io.IOException;
import java.util.Collections;

import io.delta.flink.internal.ConnectorUtils;
import io.delta.flink.utils.DeltaTestUtils;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.rules.TemporaryFolder;
import static io.delta.flink.utils.DeltaTestUtils.buildClusterResourceConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.Operation.Name;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;

public class DeltaCatalogITCase {

    private static final int PARALLELISM = 2;

    private static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @RegisterExtension
    private static final MiniClusterExtension miniClusterResource =  new MiniClusterExtension(
        buildClusterResourceConfig(PARALLELISM)
    );

    private TableEnvironment tableEnv;

    private String tablePath;

    @BeforeAll
    public static void beforeAll() throws IOException {
        TEMPORARY_FOLDER.create();
    }

    @AfterAll
    public static void afterAll() {
        TEMPORARY_FOLDER.delete();
    }

    @BeforeEach
    public void setUp() throws IOException {
        tablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
        tableEnv = TableEnvironment.create(
            EnvironmentSettings.newInstance()
                .build()
        );
        setupDeltaCatalog(tableEnv);
    }

    @Test
    public void shouldCreateTableIfDeltaLogDoesNotExists() throws Exception {

        // GIVEN
        DeltaLog deltaLog =
            DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), tablePath);

        assertThat(deltaLog.tableExists())
            .withFailMessage("There should be no Delta table files in test folder before test.")
            .isFalse();

        // TODO DC - add all types here
        String deltaTable =
            String.format("CREATE TABLE sourceTable ("
                    + "col1 BIGINT,"
                    + "col2 BIGINT,"
                    + "col3 VARCHAR,"
                    + "col4 AS col1 * col2," // computed column
                    + "col5 AS CONCAT(col3, '_hello')," // computed column
                    + "col6 AS CAST(col1 AS VARCHAR)" // computed column
                    + ") "
                    + "PARTITIONED BY (col1)"
                    + "WITH ("
                    + " 'connector' = 'delta',"
                    + " 'table-path' = '%s'"
                    + ")",
                tablePath);

        // WHEN
        tableEnv.executeSql(deltaTable).await();

        // THEN
        Metadata metadata = deltaLog.update().getMetadata();
        StructType actualSchema = metadata.getSchema();

        assertThat(actualSchema).isNotNull();
        assertThat(actualSchema.getFields())
            .withFailMessage(() -> schemaDoesNotMatchMessage(actualSchema))
            .containsExactly(
                new StructField("col1", new LongType()),
                new StructField("col2", new LongType()),
                new StructField("col3", new StringType()),
                new StructField("col4", new LongType()),
                new StructField("col5", new StringType()),
                new StructField("col6", new StringType())
            );

        assertThat(metadata.getPartitionColumns()).containsExactly("col1");
    }

    @Test
    public void shouldCreateTableIfDeltaLogExists() throws Exception {

        // GIVEN
        DeltaTestUtils.initTestForNonPartitionedTable(tablePath);

        DeltaLog deltaLog =
            DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), tablePath);

        assertThat(deltaLog.tableExists())
            .withFailMessage(
                "There should be Delta table files in test folder before calling DeltaCatalog.")
            .isTrue();

        String deltaTable =
            String.format("CREATE TABLE sourceTable ("
                    + "name VARCHAR,"
                    + "surname VARCHAR,"
                    + "age INT"
                    + ") "
                    + "WITH ("
                    + " 'connector' = 'delta',"
                    + " 'table-path' = '%s'"
                    + ")",
                tablePath);

        // WHEN
        tableEnv.executeSql(deltaTable).await();

        // THEN
        Metadata metadata = deltaLog.update().getMetadata();
        StructType schema = metadata.getSchema();

        assertThat(schema).isNotNull();
        assertThat(schema.getFields())
            .withFailMessage(() -> schemaDoesNotMatchMessage(schema))
            .containsExactly(
                new StructField("name", new StringType()),
                new StructField("surname", new StringType()),
                new StructField("age", new IntegerType())
            );

        assertThat(metadata.getPartitionColumns()).isEmpty();
    }

    /**
     * Verifies that CREATE TABLE will throw exception when _delta_log exists under table-path but
     * has different schema that specified in DDL.
     */
    @ParameterizedTest(name = "DDL schema = {0}")
    @ValueSource(strings = {
        "name VARCHAR, surname VARCHAR", // missing column
        "name VARCHAR, surname VARCHAR, age INT, extraCol INT", // extra column
        "name VARCHAR NOT NULL, surname VARCHAR, age INT, col AS age * 2",// extra computed column
        "name VARCHAR, surname VARCHAR, differentName INT", // different name for third column
        "name INT, surname VARCHAR, age INT", // different type for first column
        "name VARCHAR NOT NULL, surname VARCHAR, age INT" // all columns should be nullable
    })
    public void shouldThrowIfSchemaDoesNotMatch(String ddlSchema) throws Exception {

        // GIVEN
        DeltaTestUtils.initTestForNonPartitionedTable(tablePath);

        DeltaLog deltaLog =
            DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), tablePath);

        assertThat(deltaLog.tableExists())
            .withFailMessage(
                "There should be Delta table files in test folder before calling DeltaCatalog.")
            .isTrue();

        String deltaTable =
            String.format("CREATE TABLE sourceTable ("
                    + "%s"
                    + ") "
                    + "WITH ("
                    + " 'connector' = 'delta',"
                    + " 'table-path' = '%s'"
                    + ")",
                ddlSchema, tablePath);

        // WHEN
        RuntimeException exception =
            assertThrows(RuntimeException.class, () -> tableEnv.executeSql(deltaTable).await());

        // THEN
        assertThat(exception.getCause().getMessage()).contains(
            "has different schema or partition spec that one defined in CREATE TABLE DDL");

        // Check if there were no changes made to existing _delta_log
        Metadata metadata = deltaLog.update().getMetadata();
        verifyThatDeltaLogWasNotChanged(metadata);
        assertThat(metadata.getConfiguration()).isEmpty();
    }

    /**
     * Verifies that CREATE TABLE will throw exception when _delta_log exists under table-path but
     * has different partition spec that specified in DDL.
     */
    @Test
    public void shouldThrowIfPartitionSpecDoesNotMatch() throws Exception {

        // GIVEN
        DeltaTestUtils.initTestForNonPartitionedTable(tablePath);

        DeltaLog deltaLog =
            DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), tablePath);

        assertThat(deltaLog.tableExists())
            .withFailMessage(
                "There should be Delta table files in test folder before calling DeltaCatalog.")
            .isTrue();

        String deltaTable =
            String.format("CREATE TABLE sourceTable ("
                    + "name VARCHAR,"
                    + "surname VARCHAR,"
                    + "age INT"
                    + ") "
                    + "PARTITIONED BY (name)"
                    + "WITH ("
                    + " 'connector' = 'delta',"
                    + " 'table-path' = '%s'"
                    + ")",
                tablePath);

        // WHEN
        RuntimeException exception =
            assertThrows(RuntimeException.class, () -> tableEnv.executeSql(deltaTable).await());

        // THEN
        assertThat(exception.getCause().getMessage()).contains(
            "has different schema or partition spec that one defined in CREATE TABLE DDL");

        // Check if there were no changes made to existing _delta_log
        Metadata metadata = deltaLog.update().getMetadata();
        verifyThatDeltaLogWasNotChanged(metadata);
        assertThat(metadata.getConfiguration()).isEmpty();
    }

    /**
     * Verifies that CREATE TABLE will throw exception when _delta_log exists under table-path but
     * has different delta table properties that specified in DDL.
     */
    @Test
    public void shouldThrowIfDeltaTablePropertiesNotMatch() throws Exception {

        // GIVEN
        DeltaTestUtils.initTestForNonPartitionedTable(tablePath);

        DeltaLog deltaLog =
            DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), tablePath);

        // Set delta table property. DDL will try to override it with different value
        OptimisticTransaction transaction = deltaLog.startTransaction();
        Metadata updatedMetadata = transaction.metadata()
            .copyBuilder()
            .configuration(Collections.singletonMap("delta.appendOnly", "false"))
            .build();

        transaction.updateMetadata(updatedMetadata);
        transaction.commit(
            Collections.singletonList(updatedMetadata),
            new Operation(Name.SET_TABLE_PROPERTIES),
            ConnectorUtils.ENGINE_INFO
        );

        assertThat(deltaLog.tableExists())
            .withFailMessage(
                "There should be Delta table files in test folder before calling DeltaCatalog.")
            .isTrue();

        String deltaTable =
            String.format("CREATE TABLE sourceTable ("
                    + "name VARCHAR,"
                    + "surname VARCHAR,"
                    + "age INT"
                    + ") "
                    + "WITH ("
                    + " 'connector' = 'delta',"
                    + " 'table-path' = '%s',"
                    + " 'delta.appendOnly' = 'true'"
                    + ")",
                tablePath);

        // WHEN
        RuntimeException exception =
            assertThrows(RuntimeException.class, () -> tableEnv.executeSql(deltaTable).await());

        // THEN
        assertThat(exception.getCause().getMessage())
            .contains(
                "DDL option delta.appendOnly for table default.sourceTable has different value "
                    + "than _delta_log table property");

        // Check if there were no changes made to existing _delta_log
        Metadata metadata = deltaLog.update().getMetadata();
        verifyThatDeltaLogWasNotChanged(metadata);
        assertThat(metadata.getConfiguration())
            .containsExactlyEntriesOf(Collections.singletonMap("delta.appendOnly", "false"));
    }

    private void verifyThatDeltaLogWasNotChanged(Metadata metadata) {
        StructType schema = metadata.getSchema();
        assertThat(schema).isNotNull();
        assertThat(schema.getFields())
            .withFailMessage(() -> schemaDoesNotMatchMessage(schema))
            .containsExactly(
                new StructField("name", new StringType()),
                new StructField("surname", new StringType()),
                new StructField("age", new IntegerType())
            );
        assertThat(metadata.getPartitionColumns()).isEmpty();
    }

    private String schemaDoesNotMatchMessage(StructType schema) {
        return String.format(
            "Schema from _delta_log does not match schema from DDL.\n"
                + "The actual schema was:\n [%s]", schema.getTreeString()
        );
    }

    private void setupDeltaCatalog(TableEnvironment tableEnv) {

        String catalogSQL = "CREATE CATALOG myDeltaCatalog WITH ('type' = 'delta-catalog');";
        String useDeltaCatalog = "USE CATALOG myDeltaCatalog;";

        tableEnv.executeSql(catalogSQL);
        tableEnv.executeSql(useDeltaCatalog);
    }
}
