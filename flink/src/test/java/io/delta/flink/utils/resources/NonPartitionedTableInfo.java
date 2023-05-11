package io.delta.flink.utils.resources;

import io.delta.flink.utils.DeltaTestUtils;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.rules.TemporaryFolder;

/**
 * {@link TableInfo} implementation for unpartitioned Delta table.
 */
public class NonPartitionedTableInfo implements SqlTableInfo {

    private static final String tableInitStatePath =
        "/test-data/test-non-partitioned-delta-table-initial-state";

    private static final String sqlTableSchema = "name VARCHAR, surname VARCHAR, age INT";

    private static final String[] dataColumnNames = {"name", "surname", "age"};

    private static final LogicalType[] dataColumnTypes =
        {new CharType(), new CharType(), new IntType()};

    private final String runtimePath;

    private NonPartitionedTableInfo(String runtimePath) {
        this.runtimePath = runtimePath;
    }

    /**
     * Initialized non-partitioned test Delta table that can be used for IT tests. This table will
     * be backed by predefined delta table data from
     * {@code resources/test-data/test-non-partitioned-delta-table-initial-state}. Original files
     * will be compiled to the temporary folder provided via the argument. The path to created Delta
     * table can be obtained from {@link #getTablePath()}.
     * <p>
     * Schema for created table will be:
     * <ul>
     *     <li> column names: "name", "surname", "age"</li>
     *     <li> column types: VARCHAR, VARCHAR, INT</li>
     * </ul>
     *
     * @param tmpFolder Temporary folder where table files should be copied to.
     */
    public static NonPartitionedTableInfo createWithInitData(TemporaryFolder tmpFolder) {
        try {
            String runtimePath = tmpFolder.newFolder().getAbsolutePath();
            DeltaTestUtils.initTestFor(tableInitStatePath, runtimePath);
            return new NonPartitionedTableInfo(runtimePath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Initialized non-partitioned test Delta table without any records that can be used for IT
     * tests. This table will be backed by predefined delta table data from
     * {@code resources/test-data/test-non-partitioned-delta-table-initial-state}. Original files
     * will be compiled to the temporary folder provided via the argument. The path to created Delta
     * table can be obtained from {@link #getTablePath()}.
     * <p>
     * Schema for created table will be:
     * <ul>
     *     <li> column names: "name", "surname", "age"</li>
     *     <li> column types: VARCHAR, VARCHAR, INT</li>
     * </ul>
     *
     * @param tmpFolder Temporary folder where table files should be copied to.
     */
    public static TableInfo createWithoutInitData(TemporaryFolder tmpFolder) {
        try {
            String runtimePath = tmpFolder.newFolder().getAbsolutePath();
            return new NonPartitionedTableInfo(runtimePath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String getTablePath() {
        return runtimePath;
    }

    @Override
    public String getPartitions() {
        return "";
    }

    public String getTableInitStatePath() {
        return tableInitStatePath;
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
        return 2;
    }

    @Override
    public RowType getRowType() {
        return RowType.of(dataColumnTypes, dataColumnNames);
    }

    @Override
    public boolean isPartitioned() {
        return false;
    }

    public String getSqlTableSchema() {
        return sqlTableSchema;
    }
}
