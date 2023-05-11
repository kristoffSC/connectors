package io.delta.flink.utils.resources;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

public interface TableInfo {

    String getTablePath();

    String getPartitions();

    String[] getColumnNames();

    LogicalType[] getColumnTypes();

    int getInitialRecordCount();

    RowType getRowType();

    boolean isPartitioned();
}
