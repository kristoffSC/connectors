package io.delta.flink.utils.resources;

import io.delta.flink.utils.DeltaTestUtils;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.rules.TemporaryFolder;

public class PartitionedTableInfo implements SqlTableInfo {

    private static final String tableInitStatePath =
        "/test-data/test-partitioned-delta-table-initial-state";

    private static final String sqlTableSchema =
        "name VARCHAR, surname VARCHAR, age INT, col1 VARCHAR, col2 VARCHAR";

    private static final String[] dataColumnNames = {"name", "surname", "age"};

    private static final LogicalType[] dataColumnTypes =
        {new CharType(), new CharType(), new IntType()};

    private static final int initialRecordCount = 2;

    private final String runtimePath;

    private PartitionedTableInfo(String runtimePath) {
        this.runtimePath = runtimePath;
    }

    public static PartitionedTableInfo createWithInitData(TemporaryFolder tmpFolder) {
        try {
            String runtimePath = tmpFolder.newFolder().getAbsolutePath();
            DeltaTestUtils.initTestFor(tableInitStatePath, runtimePath);
            return new PartitionedTableInfo(runtimePath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static TableInfo createWithoutInitData(TemporaryFolder tmpFolder) {
        try {
            String runtimePath = tmpFolder.newFolder().getAbsolutePath();
            return new PartitionedTableInfo(runtimePath);
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
        return "col1, col2";
    }

    @Override
    public String getTableInitStatePath() {
        return tableInitStatePath;
    }

    @Override
    public String[] getDataColumnNames() {
        return dataColumnNames;
    }

    @Override
    public LogicalType[] getDataColumnTypes() {
        return dataColumnTypes;
    }

    @Override
    public int getInitialRecordCount() {
        return initialRecordCount;
    }

    @Override
    public RowType getRowType() {
        return RowType.of(dataColumnTypes, dataColumnNames);
    }

    @Override
    public boolean isPartitioned() {
        return true;
    }
}
