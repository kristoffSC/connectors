package io.delta.flink.internal.table;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
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

@ExtendWith(MockitoExtension.class)
class DeltaCatalogTest {

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

        String[] columnNames = new String[] {"col1", "col2", "col3"};
        DataType[] columnTypes = new DataType[] {
            new AtomicDataType(new BooleanType()),
            new AtomicDataType(new IntType()),
            new AtomicDataType(new VarCharType())
        };

        DeltaCatalogBaseTable deltaCatalogTable = setUpCatalogTable(
            columnNames,
            columnTypes,
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
    public void testThrowCreateTableInvalidTableOption() {

        Map<String, String> invalidOptions = Stream.of(
                "spark.some.option",
                "delta.logStore",
                "io.delta.storage.S3DynamoDBLogStore.ddb.region",
                "parquet.writer.max-padding"
            )
            .collect(Collectors.toMap(optionName -> optionName, s -> "aValue"));

        ddlOptions.putAll(invalidOptions);

        String[] columnNames = new String[] {"col1", "col2", "col3"};
        DataType[] columnTypes = new DataType[] {
            new AtomicDataType(new BooleanType()),
            new AtomicDataType(new IntType()),
            new AtomicDataType(new VarCharType())
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
            .containsIgnoringWhitespaces(""
                + "Invalid options used:\n"
                + "spark.some.option\n"
                + "delta.logStore\n"
                + "io.delta.storage.S3DynamoDBLogStore.ddb.region\n"
                + "parquet.writer.max-padding"
            );
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
            catalogTable
        );
    }
}
