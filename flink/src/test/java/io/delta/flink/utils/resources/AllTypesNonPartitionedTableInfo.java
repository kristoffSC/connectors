package io.delta.flink.utils.resources;

import io.delta.flink.utils.DeltaTestUtils;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.rules.TemporaryFolder;

public class AllTypesNonPartitionedTableInfo implements TableInfo {

    private static final String tableInitStatePath =
        "/test-data/test-non-partitioned-delta-table-alltypes";

    private static final String[] dataColumnNames = {
        "col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9", "col10"
    };

    private static final LogicalType[] dataColumnTypes = {
        new TinyIntType(), new SmallIntType(), new IntType(), new DoubleType(), new FloatType(),
        new DecimalType(), new DecimalType(), new TimestampType(), new VarCharType(),
        new BooleanType()
    };

    private final String runtimePath;

    private AllTypesNonPartitionedTableInfo(String runtimePath) {
        this.runtimePath = runtimePath;
    }

    public static AllTypesNonPartitionedTableInfo create(TemporaryFolder tmpFolder) {
        try {
            String runtimePath = tmpFolder.newFolder().getAbsolutePath();
            DeltaTestUtils.initTestFor(tableInitStatePath, runtimePath);
            return new AllTypesNonPartitionedTableInfo(runtimePath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
        return 5;
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
