package io.delta.flink.utils.resources;

import io.delta.flink.utils.DeltaTestUtils;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.rules.TemporaryFolder;

public class LargeNonPartitionedTableInfo implements SqlTableInfo {

    private static final String tableInitStatePath =
        "/test-data/test-non-partitioned-delta-table_1100_records";

    private static final String sqlTableSchema = "col1 BIGINT, col2 BIGINT, col3 VARCHAR";

    private static final String[] dataColumnNames = {"col1", "col2", "col3"};

    private static final LogicalType[] dataColumnTypes =
        {new BigIntType(), new BigIntType(), new VarCharType()};

    private final String runtimePath;

    private LargeNonPartitionedTableInfo(String runtimePath) {
        this.runtimePath = runtimePath;
    }

    public static LargeNonPartitionedTableInfo create(TemporaryFolder tmpFolder) {
        try {
            String runtimePath = tmpFolder.newFolder().getAbsolutePath();
            DeltaTestUtils.initTestFor(tableInitStatePath, runtimePath);
            return new LargeNonPartitionedTableInfo(runtimePath);
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
        return "";
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
        return 1100;
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
