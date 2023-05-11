package io.delta.flink.utils.resources;

import io.delta.flink.utils.DeltaTestUtils;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.rules.TemporaryFolder;

/**
 * {@link TableInfo} non-implementation for partitioned Delta table.
 */
public class TableApiTableInfo implements SqlTableInfo {

    private static final String tableInitStatePath =
        "/test-data/test-table-api";

    private static final String sqlTableSchema = "col1 VARCHAR, col2 VARCHAR, col3 INT";

    private static final String[] dataColumnNames = {"col1", "col2", "col3"};

    private static final LogicalType[] dataColumnTypes =
        {new CharType(), new CharType(), new IntType()};

    private final String runtimePath;

    private TableApiTableInfo(String runtimePath) {
        this.runtimePath = runtimePath;
    }

    /**
     * Initialized non-partitioned test Delta table that can be used for IT tests.
     * Table will be backed with files from {@code resources/test-data/test-table-api}.
     * The original files will be compiled to the temporary folder provided via
     * the argument. The path to created Delta table can be obtained from {@link #getTablePath()}.
     * <p>
     * Schema for created table will be:
     * <ul>
     *     <li> column names: "col1, col2, col3"</li>
     *     <li> column types: CharType, CharType, IntType</li>
     * </ul>
     *
     * @param tmpFolder Temporary folder where table files should be copied to.
     */
    public static TableApiTableInfo createWithInitData(TemporaryFolder tmpFolder) {
        try {
            String runtimePath = tmpFolder.newFolder().getAbsolutePath();
            DeltaTestUtils.initTestFor(tableInitStatePath, runtimePath);
            return new TableApiTableInfo(runtimePath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getSqlTableSchema() {
        return sqlTableSchema;
    }

    @Override
    public String getTablePath() {
        return runtimePath;
    }

    @Override
    public String getPartitions() {
        return null;
    }

    @Override
    public String[] getColumnNames() {
        return dataColumnNames;
    }

    @Override
    public LogicalType[] getColumnTypes() {
        return dataColumnTypes;
    }

    @Override
    public int getInitialRecordCount() {
        return 0;
    }

    @Override
    public RowType getRowType() {
        return RowType.of(dataColumnTypes, dataColumnNames);
    }

    @Override
    public boolean isPartitioned() {
        return false;
    }
}
