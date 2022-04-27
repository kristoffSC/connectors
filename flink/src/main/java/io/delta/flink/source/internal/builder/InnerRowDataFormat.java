package io.delta.flink.source.internal.builder;

import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.formats.parquet.ParquetColumnarRowInputFormat;
import org.apache.flink.formats.parquet.vector.ColumnBatchFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;

public class InnerRowDataFormat extends ParquetColumnarRowInputFormat<DeltaSourceSplit>
    implements DeltaBulkFormat<RowData> {

    InnerRowDataFormat(Configuration hadoopConfig,
        RowType projectedType, int batchSize,
        boolean isUtcTimestamp, boolean isCaseSensitive) {
        super(hadoopConfig, projectedType, batchSize, isUtcTimestamp, isCaseSensitive);
    }

    InnerRowDataFormat(Configuration hadoopConfig,
        RowType projectedType, RowType producedType,
        ColumnBatchFactory<DeltaSourceSplit> batchFactory,
        int batchSize, boolean isUtcTimestamp, boolean isCaseSensitive) {
        super(hadoopConfig, projectedType, producedType, batchFactory, batchSize, isUtcTimestamp,
            isCaseSensitive);
    }

    public static RowDataFormatBuilder builder(
        String[] columnNames, LogicalType[] columnTypes, Configuration hadoopConfiguration) {
        return new RowDataFormatBuilder(columnNames, columnTypes, hadoopConfiguration);
    }

}
