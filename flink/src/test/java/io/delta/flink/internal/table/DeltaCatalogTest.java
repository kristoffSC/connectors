package io.delta.flink.internal.table;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.delta.flink.utils.DeltaTestUtils;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.rules.TemporaryFolder;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.delta.standalone.DeltaLog;

// TODO DC - This test class is fully moved to table_feature_branch. Update feature branch if any
//  new test is added here.
@ExtendWith(MockitoExtension.class)
class DeltaCatalogTest {

    public static final String[] COLUMN_NAMES = new String[] {"col1", "col2", "col3"};

    public static final DataType[] COLUMN_TYPES = new DataType[] {
        new AtomicDataType(new BooleanType()),
        new AtomicDataType(new IntType()),
        new AtomicDataType(new VarCharType())
    };
    private static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private static final boolean ignoreIfExists = false;

    private static final String DATABASE = "default";

    public static final String CATALOG_NAME = "testCatalog";

    private DeltaCatalog deltaCatalog;

    private Map<String, String> ddlOptions;

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
        Catalog decoratedCatalog = new GenericInMemoryCatalog(CATALOG_NAME, DATABASE);
        decoratedCatalog.open();
        this.deltaCatalog = new DeltaCatalog(CATALOG_NAME, decoratedCatalog, new Configuration());
        this.ddlOptions = new HashMap<>();
        this.ddlOptions.put(
            DeltaTableConnectorOptions.TABLE_PATH.key(),
            TEMPORARY_FOLDER.newFolder().getAbsolutePath()
        );
    }

    @ParameterizedTest
    @NullSource  // pass a null value
    @ValueSource(strings = {"", " "})
    public void testThrowCreateTableInvalidTablePath(String deltaTablePath) {

        DeltaCatalogBaseTable deltaCatalogTable = setUpCatalogTable(
            COLUMN_NAMES,
            COLUMN_TYPES,
            (deltaTablePath == null) ? Collections.emptyMap() : Collections.singletonMap(
                DeltaTableConnectorOptions.TABLE_PATH.key(),
                deltaTablePath
            )
        );

        CatalogException exception = assertThrows(CatalogException.class, () ->
            deltaCatalog.createTable(deltaCatalogTable, ignoreIfExists)
        );

        assertThat(exception.getMessage())
            .isEqualTo("Path to Delta table cannot be null or empty.");
    }

    @Test
    public void testThrowCreateTableIfInvalidTableOptionUsed() {

        Map<String, String> invalidOptions = Stream.of(
                "spark.some.option",
                "delta.logStore",
                "io.delta.storage.S3DynamoDBLogStore.ddb.region",
                "parquet.writer.max-padding"
            )
            .collect(Collectors.toMap(optionName -> optionName, s -> "aValue"));

        String expectedValidationMessage = ""
            + "DDL contains invalid properties. DDL can have only delta table properties or "
            + "arbitrary user options only.\n"
            + "Invalid options used:\n"
            + " - spark.some.option\n"
            + " - delta.logStore\n"
            + " - io.delta.storage.S3DynamoDBLogStore.ddb.region\n"
            + " - parquet.writer.max-padding";

        testCreateTableOptionValidation(invalidOptions, expectedValidationMessage);
    }

    @Test
    public void testThrowCreateTableIfJobSpecificOptionUsed() {

        // This test will not check if options are mutual excluded.
        // This is covered by table Factory and Source builder tests.
        Map<String, String> invalidOptions = Stream.of(
                "startingVersion",
                "startingTimestamp",
                "updateCheckIntervalMillis",
                "updateCheckDelayMillis",
                "ignoreDeletes",
                "ignoreChanges",
                "versionAsOf",
                "timestampAsOf"
            )
            .collect(Collectors.toMap(optionName -> optionName, s -> "aValue"));

        String expectedValidationMessage = ""
            + "DDL contains invalid properties. DDL can have only delta table properties or "
            + "arbitrary user options only.\n"
            + "DDL contains job specific options. Job specific options can be used only via "
            + "Query hints.\n"
            + "Used Job specific options:\n"
            + " - ignoreDeletes\n"
            + " - startingTimestamp\n"
            + " - updateCheckIntervalMillis\n"
            + " - startingVersion\n"
            + " - ignoreChanges\n"
            + " - versionAsOf\n"
            + " - updateCheckDelayMillis\n"
            + " - timestampAsOf";

        testCreateTableOptionValidation(invalidOptions, expectedValidationMessage);
    }

    @Test
    public void testThrowCreateTableIfJobSpecificOptionAndInvalidTableOptionsAreUsed() {

        // This test will not check if options are mutual excluded.
        // This is covered by table Factory and Source builder tests.
        Map<String, String> invalidOptions = Stream.of(
                "spark.some.option",
                "delta.logStore",
                "io.delta.storage.S3DynamoDBLogStore.ddb.region",
                "parquet.writer.max-padding",
                "startingVersion",
                "startingTimestamp",
                "updateCheckIntervalMillis",
                "updateCheckDelayMillis",
                "ignoreDeletes",
                "ignoreChanges",
                "versionAsOf",
                "timestampAsOf"
            )
            .collect(Collectors.toMap(optionName -> optionName, s -> "aValue"));

        String expectedValidationMessage = ""
            + "DDL contains invalid properties. DDL can have only delta table properties or "
            + "arbitrary user options only.\n"
            + "Invalid options used:\n"
            + " - spark.some.option\n"
            + " - delta.logStore\n"
            + " - io.delta.storage.S3DynamoDBLogStore.ddb.region\n"
            + " - parquet.writer.max-padding\n"
            + "DDL contains job specific options. Job specific options can be used only via "
            + "Query hints.\n"
            + "Used Job specific options:\n"
            + " - startingTimestamp\n"
            + " - ignoreDeletes\n"
            + " - updateCheckIntervalMillis\n"
            + " - startingVersion\n"
            + " - ignoreChanges\n"
            + " - versionAsOf\n"
            + " - updateCheckDelayMillis\n"
            + " - timestampAsOf";

        testCreateTableOptionValidation(invalidOptions, expectedValidationMessage);
    }

    private void testCreateTableOptionValidation(
            Map<String, String> invalidOptions,
            String expectedValidationMessage) {
        ddlOptions.putAll(invalidOptions);
        DeltaCatalogBaseTable deltaCatalogTable = setUpCatalogTable(
            COLUMN_NAMES,
            COLUMN_TYPES,
            ddlOptions
        );

        CatalogException exception = assertThrows(CatalogException.class, () ->
            deltaCatalog.createTable(deltaCatalogTable, ignoreIfExists)
        );

        assertThat(exception.getMessage()).isEqualTo(expectedValidationMessage);
    }

    @Test
    public void testThrowIfMismatchedDdlOptionAndDeltaTableProperty() throws IOException {

        String tablePath = this.ddlOptions.get(
            DeltaTableConnectorOptions.TABLE_PATH.key()
        );

        DeltaTestUtils.initTestForNonPartitionedTable(tablePath);

        Map<String, String> configuration = Collections.singletonMap("delta.appendOnly", "false");

        DeltaLog deltaLog = DeltaTestUtils.setupDeltaTableWithProperty(tablePath, configuration);

        assertThat(deltaLog.tableExists())
            .withFailMessage(
                "There should be Delta table files in test folder before calling DeltaCatalog.")
            .isTrue();

        Map<String, String> mismatchedOptions =
            Collections.singletonMap("delta.appendOnly", "true");

        ddlOptions.putAll(mismatchedOptions);

        String[] columnNames = new String[] {"name", "surname", "age"};
        DataType[] columnTypes = new DataType[] {
            new AtomicDataType(new VarCharType()),
            new AtomicDataType(new VarCharType()),
            new AtomicDataType(new IntType())
        };

        DeltaCatalogBaseTable deltaCatalogTable = setUpCatalogTable(
            columnNames,
            columnTypes,
            ddlOptions
        );

        CatalogException exception = assertThrows(CatalogException.class, () ->
            deltaCatalog.createTable(deltaCatalogTable, ignoreIfExists)
        );

        assertThat(exception.getMessage())
            .isEqualTo(""
                + "Invalid DDL options for table [default.testTable]. DDL options for Delta table"
                + " connector cannot override table properties already defined in _delta_log.\n"
                + "DDL option name | DDL option value | Delta option value \n"
                + "delta.appendOnly | true | false");

    }

    private DeltaCatalogBaseTable setUpCatalogTable(
            String[] columnNames,
            DataType[] columnTypes,
            Map<String, String> options) {

        CatalogTable catalogTable = CatalogTable.of(
            Schema.newBuilder()
                .fromFields(columnNames, columnTypes)
                .build(),
            "comment",
            Collections.emptyList(), // partitionKeys
            options // options
        );

        return new DeltaCatalogBaseTable(
            new ObjectPath(DATABASE, "testTable"),
            new ResolvedCatalogTable(
                catalogTable,
                ResolvedSchema.physical(columnNames, columnTypes)
            )
        );
    }
}
