package io.delta.flink.utils.resources;

import io.delta.flink.utils.DeltaTestUtils;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.rules.TemporaryFolder;

/**
 * {@link TableInfo} non-implementation for partitioned Delta table with four Delta versions.
 */
public class VersionedNonPartitionedTable implements SqlTableInfo {

    private static final String tableInitStatePath =
        "/test-data/test-non-partitioned-delta-table-4-versions";

    private static final String[] dataColumnNames = {"col1", "col2", "col3"};

    private static final LogicalType[] dataColumnTypes =
        {new BigIntType(), new BigIntType(), new CharType()};

    private final String runtimePath;

    private VersionedNonPartitionedTable(String runtimePath) {
        this.runtimePath = runtimePath;
    }

    /**
     * Initialized non-partitioned test Delta table that can be used for IT tests.
     * Table will have four Delta table versions and will be backed with files from
     * {@code resources/test-data/test-non-partitioned-delta-table-4-versions}.
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
    public static VersionedNonPartitionedTable createWithInitData(TemporaryFolder tmpFolder) {
        try {
            String runtimePath = tmpFolder.newFolder().getAbsolutePath();
            DeltaTestUtils.initTestFor(tableInitStatePath, runtimePath);
            return new VersionedNonPartitionedTable(runtimePath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getSqlTableSchema() {
        return null;
    }

    @Override
    public String getTablePath() {
        return runtimePath;
    }

    @Override
    public String getPartitions() {
        return "";
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
        return 4;
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
