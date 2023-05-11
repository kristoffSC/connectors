package io.delta.flink.utils.resources;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/**
 * Provides information about Table path, schema and record count for predefined Delta tables used
 * in Integration tests.
 */
public interface TableInfo {

    /**
     * @return path to test Delta table. The path should be unique for every test run.
     */
    String getTablePath();

    /**
     * @return String containing comma separated partition column names for Delta table.
     * Empty string if table has no partitions.
     */
    String getPartitions();

    /**
     * @return array of column names for particular test Delta table. If table is partitioned,
     * this array must contain also partition column names.
     */
    String[] getColumnNames();

    /**
     * @return array of column Flink types for particular test Delta table. If table is partitioned,
     * this array must contain also partition column types.
     */
    LogicalType[] getColumnTypes();

    /**
     * @return number of records that this table have after initialization.
     */
    int getInitialRecordCount();

    /**
     * @return Flink {@link RowType} representing a row schema for given table.
     */
    RowType getRowType();

    /**
     * @return true if table is partitioned, false otherwise.
     */
    boolean isPartitioned();
}
