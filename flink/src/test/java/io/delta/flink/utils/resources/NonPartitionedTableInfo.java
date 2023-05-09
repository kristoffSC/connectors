package io.delta.flink.utils.resources;

import io.delta.flink.utils.DeltaTestUtils;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.rules.TemporaryFolder;

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

    public static NonPartitionedTableInfo create(TemporaryFolder tmpFolder) {
        try {
            String runtimePath = tmpFolder.newFolder().getAbsolutePath();
            DeltaTestUtils.initTestFor(tableInitStatePath, runtimePath);
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
    public String[] getDataColumnNames() {
        return dataColumnNames;
    }

    @Override
    public LogicalType[] getDataColumnTypes() {
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

    public String getSqlTableSchema() {
        return sqlTableSchema;
    }
}
