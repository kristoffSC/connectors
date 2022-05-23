package io.delta.flink.source.internal.utils;

import org.apache.flink.table.types.logical.LogicalType;

public class SourceSchema {

    private final String[] columnNames;

    private final LogicalType[] columnTypes;

    public SourceSchema(String[] columnNames, LogicalType[] columnTypes) {
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public LogicalType[] getColumnTypes() {
        return columnTypes;
    }
}
